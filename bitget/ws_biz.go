package bitget

import (
	"strconv"
	"strings"
	"time"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"github.com/banbox/bntp"
	"go.uber.org/zap"
)

const (
	wsSpot = "spot"
	wsMix  = "mix"
)

// makeHandleWsMsg creates the websocket message handler.
func makeHandleWsMsg(e *Bitget) banexg.FuncOnWsMsg {
	return func(client *banexg.WsClient, item *banexg.WsMsg) {
		if item == nil {
			return
		}
		var msg map[string]interface{}
		if err := utils.UnmarshalString(item.Text, &msg, utils.JsonNumAuto); err != nil {
			log.Error("ws msg unmarshal fail", zap.Error(err))
			return
		}
		// Handle login response
		if event, ok := msg["event"].(string); ok && event != "" {
			if event == "login" {
				code := getMapString(msg, "code")
				e.WsAuthLock.Lock()
				loginSuccess := code == "0" || code == ""
				if loginSuccess {
					e.WsAuthed[client.Key] = true
				}
				pendingRecon := e.WsPendingRecons[client.Key]
				if pendingRecon != nil {
					delete(e.WsPendingRecons, client.Key)
				}
				if ch, ok := e.WsAuthDone[client.Key]; ok {
					if loginSuccess {
						ch <- nil
					} else {
						errMsg := getMapString(msg, "msg")
						ch <- errs.NewMsg(errs.CodeUnauthorized, "ws login failed: %s - %s", code, errMsg)
					}
					delete(e.WsAuthDone, client.Key)
				}
				e.WsAuthLock.Unlock()
				if loginSuccess && pendingRecon != nil && len(pendingRecon.Keys) > 0 {
					go e.restorePendingSubscriptions(pendingRecon)
				}
				return
			}
			if event == "error" {
				log.Error("ws event error", zap.String("msg", item.Text))
				e.WsAuthLock.Lock()
				if ch, ok := e.WsAuthDone[client.Key]; ok {
					errMsg := getMapString(msg, "msg")
					ch <- errs.NewMsg(errs.CodeUnauthorized, "ws auth error: %s", errMsg)
					delete(e.WsAuthDone, client.Key)
				}
				e.WsAuthLock.Unlock()
				return
			}
			if event == "subscribe" || event == "unsubscribe" {
				// Subscription confirmation, ignore
				return
			}
			return
		}
		// Handle pong
		if op, ok := msg["op"].(string); ok && op == "pong" {
			return
		}
		// Data messages — dispatch by channel
		arg, _ := msg["arg"].(map[string]interface{})
		channel := getMapString(arg, "channel")
		switch {
		case channel == WsChanTrades:
			e.handleWsTrades(client, msg, arg)
		case channel == WsChanMarkPrice:
			e.handleWsMarkPrices(client, msg, arg)
		case strings.HasPrefix(channel, WsChanCandlePrefix):
			e.handleWsOHLCV(client, msg, arg)
		case channel == WsChanBooks || channel == WsChanBooks5:
			e.handleWsOrderBooks(client, msg, arg, channel)
		default:
			if channel != "" {
				log.Debug("unhandled ws channel", zap.String("channel", channel))
			}
		}
	}
}

func makeHandleWsReCon(e *Bitget) banexg.FuncOnWsReCon {
	return func(client *banexg.WsClient, connID int) *errs.Error {
		if client == nil {
			return nil
		}
		keys := client.GetSubKeys(connID)
		if client.MarketType == wsMix {
			// Auth needed for mix WS — clear auth state and re-login
			e.WsAuthLock.Lock()
			delete(e.WsAuthed, client.Key)
			e.WsPendingRecons[client.Key] = &WsPendingRecon{
				Client: client,
				ConnID: connID,
				Keys:   keys,
			}
			e.WsAuthLock.Unlock()

			acc, err := e.GetAccount(client.AccName)
			if err != nil {
				return err
			}
			return e.wsLoginAsync(client, acc, connID)
		}
		// For spot WS (no auth), restore subscriptions immediately
		if len(keys) == 0 {
			return nil
		}
		args := make([]map[string]interface{}, 0, len(keys))
		for _, key := range keys {
			ch, _, instId := parseWsKey(key)
			arg := map[string]interface{}{FldChannel: ch}
			if instId != "" {
				arg[FldInstId] = instId
			}
			args = append(args, arg)
		}
		return e.writeWsArgs(client, connID, true, keys, args)
	}
}

// -------- Watch Functions --------

func (e *Bitget) WatchTrades(symbols []string, params map[string]interface{}) (chan *banexg.Trade, *errs.Error) {
	if len(symbols) == 0 {
		return nil, errs.NewMsg(errs.CodeParamRequired, "symbols required for WatchTrades")
	}
	_, err := e.LoadMarkets(false, nil)
	if err != nil {
		return nil, err
	}
	client, err := e.getWsClientForSymbols(symbols, params)
	if err != nil {
		return nil, err
	}
	channel := WsChanTrades
	argsList := make([]map[string]interface{}, 0, len(symbols))
	keys := make([]string, 0, len(symbols))
	for _, sym := range symbols {
		id, err := e.GetMarketID(sym)
		if err != nil {
			return nil, err
		}
		argsList = append(argsList, map[string]interface{}{FldChannel: channel, FldInstId: id})
		keys = append(keys, buildWsKey(channel, id))
	}
	if err := e.writeWsArgs(client, 0, true, keys, argsList); err != nil {
		return nil, err
	}
	chanKey := client.Prefix(channel)
	create := func(cap int) chan *banexg.Trade { return make(chan *banexg.Trade, cap) }
	out := banexg.GetWsOutChan(e.Exchange, chanKey, create, params)
	e.AddWsChanRefs(chanKey, symbols...)
	e.DumpWS("WatchTrades", symbols)
	return out, nil
}

func (e *Bitget) UnWatchTrades(symbols []string, params map[string]interface{}) *errs.Error {
	if len(symbols) == 0 {
		return errs.NewMsg(errs.CodeParamRequired, "symbols required for UnWatchTrades")
	}
	client, err := e.getWsClientForSymbols(symbols, params)
	if err != nil {
		return err
	}
	channel := WsChanTrades
	argsList := make([]map[string]interface{}, 0, len(symbols))
	keys := make([]string, 0, len(symbols))
	for _, sym := range symbols {
		id, err := e.GetMarketID(sym)
		if err != nil {
			return err
		}
		argsList = append(argsList, map[string]interface{}{FldChannel: channel, FldInstId: id})
		keys = append(keys, buildWsKey(channel, id))
	}
	if err := e.writeWsArgs(client, 0, false, keys, argsList); err != nil {
		return err
	}
	chanKey := client.Prefix(channel)
	e.DelWsChanRefs(chanKey, symbols...)
	return nil
}

func (e *Bitget) WatchMarkPrices(symbols []string, params map[string]interface{}) (chan map[string]float64, *errs.Error) {
	if len(symbols) == 0 {
		return nil, errs.NewMsg(errs.CodeParamRequired, "symbols required for WatchMarkPrices")
	}
	_, err := e.LoadMarkets(false, nil)
	if err != nil {
		return nil, err
	}
	client, err := e.getWsClientForSymbols(symbols, params)
	if err != nil {
		return nil, err
	}
	channel := WsChanMarkPrice
	argsList := make([]map[string]interface{}, 0, len(symbols))
	keys := make([]string, 0, len(symbols))
	for _, sym := range symbols {
		id, err := e.GetMarketID(sym)
		if err != nil {
			return nil, err
		}
		argsList = append(argsList, map[string]interface{}{FldChannel: channel, FldInstId: id})
		keys = append(keys, buildWsKey(channel, id))
	}
	if err := e.writeWsArgs(client, 0, true, keys, argsList); err != nil {
		return nil, err
	}
	chanKey := client.Prefix("markPrice")
	create := func(cap int) chan map[string]float64 { return make(chan map[string]float64, cap) }
	out := banexg.GetWsOutChan(e.Exchange, chanKey, create, params)
	e.AddWsChanRefs(chanKey, "markPrice")
	e.DumpWS("WatchMarkPrices", symbols)
	return out, nil
}

func (e *Bitget) UnWatchMarkPrices(symbols []string, params map[string]interface{}) *errs.Error {
	if len(symbols) == 0 {
		return errs.NewMsg(errs.CodeParamRequired, "symbols required for UnWatchMarkPrices")
	}
	_, err := e.LoadMarkets(false, nil)
	if err != nil {
		return err
	}
	client, err := e.getWsClientForSymbols(symbols, params)
	if err != nil {
		return err
	}
	channel := WsChanMarkPrice
	argsList := make([]map[string]interface{}, 0, len(symbols))
	keys := make([]string, 0, len(symbols))
	for _, sym := range symbols {
		id, err := e.GetMarketID(sym)
		if err != nil {
			return err
		}
		argsList = append(argsList, map[string]interface{}{FldChannel: channel, FldInstId: id})
		keys = append(keys, buildWsKey(channel, id))
	}
	if err := e.writeWsArgs(client, 0, false, keys, argsList); err != nil {
		return err
	}
	chanKey := client.Prefix("markPrice")
	e.DelWsChanRefs(chanKey, "markPrice")
	return nil
}

func (e *Bitget) WatchOHLCVs(jobs [][2]string, params map[string]interface{}) (chan *banexg.PairTFKline, *errs.Error) {
	if len(jobs) == 0 {
		return nil, errs.NewMsg(errs.CodeParamRequired, "jobs required for WatchOHLCVs")
	}
	_, err := e.LoadMarkets(false, nil)
	if err != nil {
		return nil, err
	}
	// Collect all unique symbols to determine WS client
	symSet := make(map[string]bool)
	for _, job := range jobs {
		symSet[job[0]] = true
	}
	symbols := make([]string, 0, len(symSet))
	for s := range symSet {
		symbols = append(symbols, s)
	}
	client, err := e.getWsClientForSymbols(symbols, params)
	if err != nil {
		return nil, err
	}

	argsList := make([]map[string]interface{}, 0, len(jobs))
	keys := make([]string, 0, len(jobs))
	refKeys := make([]string, 0, len(jobs))
	for _, job := range jobs {
		symbol := job[0]
		timeframe := job[1]
		if symbol == "" || timeframe == "" {
			return nil, errs.NewMsg(errs.CodeParamInvalid, "invalid job for WatchOHLCVs")
		}
		id, err := e.GetMarketID(symbol)
		if err != nil {
			return nil, err
		}
		tf := e.GetTimeFrame(timeframe)
		if tf == "" {
			return nil, errs.NewMsg(errs.CodeInvalidTimeFrame, "invalid timeframe: %s", timeframe)
		}
		channel := "candle" + tf
		argsList = append(argsList, map[string]interface{}{FldChannel: channel, FldInstId: id})
		keys = append(keys, buildWsKey(channel, id))
		refKeys = append(refKeys, symbol+"@"+timeframe)
	}
	if err := e.writeWsArgs(client, 0, true, keys, argsList); err != nil {
		return nil, err
	}
	chanKey := client.Prefix("candle")
	create := func(cap int) chan *banexg.PairTFKline { return make(chan *banexg.PairTFKline, cap) }
	out := banexg.GetWsOutChan(e.Exchange, chanKey, create, params)
	e.AddWsChanRefs(chanKey, refKeys...)
	e.DumpWS("WatchOHLCVs", jobs)
	return out, nil
}

func (e *Bitget) UnWatchOHLCVs(jobs [][2]string, params map[string]interface{}) *errs.Error {
	if len(jobs) == 0 {
		return errs.NewMsg(errs.CodeParamRequired, "jobs required for UnWatchOHLCVs")
	}
	symSet := make(map[string]bool)
	for _, job := range jobs {
		symSet[job[0]] = true
	}
	symbols := make([]string, 0, len(symSet))
	for s := range symSet {
		symbols = append(symbols, s)
	}
	client, err := e.getWsClientForSymbols(symbols, params)
	if err != nil {
		return err
	}
	argsList := make([]map[string]interface{}, 0, len(jobs))
	keys := make([]string, 0, len(jobs))
	refKeys := make([]string, 0, len(jobs))
	for _, job := range jobs {
		symbol := job[0]
		timeframe := job[1]
		if symbol == "" || timeframe == "" {
			return errs.NewMsg(errs.CodeParamInvalid, "invalid job for UnWatchOHLCVs")
		}
		id, err := e.GetMarketID(symbol)
		if err != nil {
			return err
		}
		tf := e.GetTimeFrame(timeframe)
		if tf == "" {
			return errs.NewMsg(errs.CodeInvalidTimeFrame, "invalid timeframe: %s", timeframe)
		}
		channel := "candle" + tf
		argsList = append(argsList, map[string]interface{}{FldChannel: channel, FldInstId: id})
		keys = append(keys, buildWsKey(channel, id))
		refKeys = append(refKeys, symbol+"@"+timeframe)
	}
	if err := e.writeWsArgs(client, 0, false, keys, argsList); err != nil {
		return err
	}
	chanKey := client.Prefix("candle")
	e.DelWsChanRefs(chanKey, refKeys...)
	return nil
}

// -------- WS Client Management --------

// getWsClientForSymbols determines the WS client type based on the symbols' market type.
func (e *Bitget) getWsClientForSymbols(symbols []string, params map[string]interface{}) (*banexg.WsClient, *errs.Error) {
	// Determine market type from the first symbol
	if len(symbols) == 0 {
		return nil, errs.NewMsg(errs.CodeParamRequired, "symbols required")
	}
	market, err := e.GetMarket(symbols[0])
	if err != nil {
		// Default to spot if we can't determine
		return e.getWsClient(wsSpot, "")
	}

	kind := wsSpot
	if market.Contract {
		kind = wsMix
	}
	return e.getWsClient(kind, "")
}

func (e *Bitget) getWsClient(kind, accName string) (*banexg.WsClient, *errs.Error) {
	var hostKey string
	switch kind {
	case wsSpot:
		hostKey = HostWsSpot
	case wsMix:
		hostKey = HostWsMix
	default:
		return nil, errs.NewMsg(errs.CodeParamInvalid, "invalid ws type: %s", kind)
	}
	wsUrl := e.GetHost(hostKey)
	if wsUrl == "" {
		return nil, errs.NewMsg(errs.CodeParamInvalid, "ws host missing for %s", kind)
	}
	return e.GetClient(wsUrl, kind, accName)
}

// -------- WS Auth --------

func (e *Bitget) wsLogin(client *banexg.WsClient, acc *banexg.Account, connID int) *errs.Error {
	if client == nil || acc == nil {
		return errs.NewMsg(errs.CodeParamInvalid, "invalid ws login args")
	}

	e.WsAuthLock.Lock()
	if e.WsAuthed[client.Key] {
		e.WsAuthLock.Unlock()
		return nil
	}
	if doneCh, waiting := e.WsAuthDone[client.Key]; waiting {
		e.WsAuthLock.Unlock()
		select {
		case authErr := <-doneCh:
			select {
			case doneCh <- authErr:
			default:
			}
			return authErr
		case <-time.After(10 * time.Second):
			return errs.NewMsg(errs.CodeTimeout, "ws login timeout (waiting)")
		}
	}

	doneCh := make(chan *errs.Error, 10)
	e.WsAuthDone[client.Key] = doneCh
	e.WsAuthLock.Unlock()

	_, creds, err := e.GetAccountCreds(acc.Name)
	if err != nil {
		e.WsAuthLock.Lock()
		delete(e.WsAuthDone, client.Key)
		e.WsAuthLock.Unlock()
		return err
	}
	// Bitget WS auth: sign = timestamp + "GET" + "/user/self/verify"
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	payload := timestamp + "GET" + "/user/self/verify"
	sign, err2 := utils.Signature(payload, creds.Secret, "hmac", "sha256", "base64")
	if err2 != nil {
		e.WsAuthLock.Lock()
		delete(e.WsAuthDone, client.Key)
		e.WsAuthLock.Unlock()
		return errs.New(errs.CodeSignFail, err2)
	}
	args := []map[string]interface{}{
		{
			"apiKey":     creds.ApiKey,
			"passphrase": creds.Password,
			"timestamp":  timestamp,
			"sign":       sign,
		},
	}
	req := map[string]interface{}{
		"op":   "login",
		"args": args,
	}
	_, conn := client.UpdateSubs(connID, true, []string{})
	if conn == nil {
		e.WsAuthLock.Lock()
		delete(e.WsAuthDone, client.Key)
		e.WsAuthLock.Unlock()
		return errs.NewMsg(errs.CodeRunTime, "get ws conn fail")
	}
	if writeErr := client.Write(conn, req, nil); writeErr != nil {
		e.WsAuthLock.Lock()
		delete(e.WsAuthDone, client.Key)
		e.WsAuthLock.Unlock()
		return writeErr
	}

	select {
	case authErr := <-doneCh:
		e.WsAuthLock.Lock()
		if authErr == nil {
			e.WsAuthed[client.Key] = true
		}
		e.WsAuthLock.Unlock()
		go func() {
			time.Sleep(500 * time.Millisecond)
			e.WsAuthLock.Lock()
			delete(e.WsAuthDone, client.Key)
			e.WsAuthLock.Unlock()
		}()
		return authErr
	case <-time.After(10 * time.Second):
		e.WsAuthLock.Lock()
		delete(e.WsAuthDone, client.Key)
		e.WsAuthLock.Unlock()
		return errs.NewMsg(errs.CodeTimeout, "ws login timeout")
	}
}

func (e *Bitget) wsLoginAsync(client *banexg.WsClient, acc *banexg.Account, connID int) *errs.Error {
	if client == nil || acc == nil {
		return errs.NewMsg(errs.CodeParamInvalid, "invalid ws login args")
	}
	_, creds, err := e.GetAccountCreds(acc.Name)
	if err != nil {
		return err
	}
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	payload := timestamp + "GET" + "/user/self/verify"
	sign, err2 := utils.Signature(payload, creds.Secret, "hmac", "sha256", "base64")
	if err2 != nil {
		return errs.New(errs.CodeSignFail, err2)
	}
	args := []map[string]interface{}{
		{
			"apiKey":     creds.ApiKey,
			"passphrase": creds.Password,
			"timestamp":  timestamp,
			"sign":       sign,
		},
	}
	req := map[string]interface{}{
		"op":   "login",
		"args": args,
	}
	_, conn := client.UpdateSubs(connID, true, []string{})
	if conn == nil {
		return errs.NewMsg(errs.CodeRunTime, "get ws conn fail")
	}
	return client.Write(conn, req, nil)
}

// restorePendingSubscriptions restores subscriptions after successful reconnection login.
func (e *Bitget) restorePendingSubscriptions(recon *WsPendingRecon) {
	if recon == nil || recon.Client == nil || len(recon.Keys) == 0 {
		return
	}
	args := make([]map[string]interface{}, 0, len(recon.Keys))
	for _, key := range recon.Keys {
		ch, _, instId := parseWsKey(key)
		arg := map[string]interface{}{FldChannel: ch}
		if instId != "" {
			arg[FldInstId] = instId
		}
		args = append(args, arg)
	}
	if err := e.writeWsArgs(recon.Client, recon.ConnID, true, recon.Keys, args); err != nil {
		log.Error("restore subscriptions failed", zap.Error(err))
	}
}

// -------- WS Write --------

func (e *Bitget) writeWsArgs(client *banexg.WsClient, connID int, isSub bool, keys []string, args []map[string]interface{}) *errs.Error {
	if client == nil {
		return errs.NewMsg(errs.CodeParamInvalid, "ws client required")
	}
	_, conn := client.UpdateSubs(connID, isSub, keys)
	if conn == nil {
		return errs.NewMsg(errs.CodeRunTime, "get ws conn fail")
	}
	op := "subscribe"
	if !isSub {
		op = "unsubscribe"
	}
	req := map[string]interface{}{
		"op":   op,
		"args": args,
	}
	return client.Write(conn, req, nil)
}

// -------- WS Message Handlers --------

func (e *Bitget) handleWsTrades(client *banexg.WsClient, msg map[string]interface{}, arg map[string]interface{}) {
	data := getMapSlice(msg, "data")
	instId := getMapString(arg, "instId")
	if instId == "" && len(data) > 0 {
		instId = getMapString(data[0], "instId")
	}
	if instId != "" {
		client.SetSubsKeyStamp(buildWsKey(WsChanTrades, instId), bntp.UTCStamp())
	}
	chanKey := client.Prefix(WsChanTrades)
	for _, item := range data {
		trade := parseWsTradeItem(e, item)
		if trade == nil {
			continue
		}
		banexg.WriteOutChan(e.Exchange, chanKey, trade, true)
	}
}

func (e *Bitget) handleWsMarkPrices(client *banexg.WsClient, msg map[string]interface{}, _ map[string]interface{}) {
	data := getMapSlice(msg, "data")
	if len(data) == 0 {
		return
	}
	result := map[string]float64{}
	e.MarkPriceLock.Lock()
	for _, item := range data {
		symbol, price, marketType, instId := parseWsMarkPriceItem(e, item)
		if symbol == "" {
			continue
		}
		if marketType == "" {
			marketType = banexg.MarketSpot
		}
		dataMap, ok := e.MarkPrices[marketType]
		if !ok {
			dataMap = map[string]float64{}
			e.MarkPrices[marketType] = dataMap
		}
		dataMap[symbol] = price
		result[symbol] = price
		if instId != "" {
			client.SetSubsKeyStamp(buildWsKey(WsChanMarkPrice, instId), bntp.UTCStamp())
		}
	}
	e.MarkPriceLock.Unlock()
	if len(result) > 0 {
		banexg.WriteOutChan(e.Exchange, client.Prefix("markPrice"), result, true)
	}
}

func (e *Bitget) handleWsOHLCV(client *banexg.WsClient, msg map[string]interface{}, arg map[string]interface{}) {
	data := getMapSlice(msg, "data")
	if len(data) == 0 {
		return
	}
	channel := getMapString(arg, "channel")
	instId := getMapString(arg, "instId")
	if instId == "" && len(data) > 0 {
		instId = getMapString(data[0], "instId")
	}
	if channel == "" || instId == "" {
		return
	}
	symbol := instId
	if market := getMarketByIDAny(e, instId, ""); market != nil {
		symbol = market.Symbol
	}
	tf := strings.TrimPrefix(channel, "candle")
	if tf == channel {
		tf = ""
	}
	client.SetSubsKeyStamp(buildWsKey(channel, instId), bntp.UTCStamp())
	chanKey := client.Prefix("candle")
	for _, item := range data {
		kline := parseWsCandleItem(item)
		if kline == nil {
			continue
		}
		out := &banexg.PairTFKline{
			Symbol:    symbol,
			TimeFrame: tf,
			Kline:     *kline,
		}
		banexg.WriteOutChan(e.Exchange, chanKey, out, true)
	}
}

func (e *Bitget) handleWsOrderBooks(client *banexg.WsClient, msg map[string]interface{}, arg map[string]interface{}, channel string) {
	data := getMapSlice(msg, "data")
	instId := getMapString(arg, "instId")
	if instId == "" && len(data) > 0 {
		instId = getMapString(data[0], "instId")
	}
	if instId != "" {
		client.SetSubsKeyStamp(buildWsKey(channel, instId), bntp.UTCStamp())
	}
	action := getMapString(msg, "action")
	chanKey := client.Prefix(channel)
	for _, item := range data {
		book := e.applyWsOrderBookUpdate(client, item, channel, action)
		if book == nil {
			continue
		}
		banexg.WriteOutChan(e.Exchange, chanKey, book, true)
	}
}

func (e *Bitget) applyWsOrderBookUpdate(client *banexg.WsClient, item map[string]interface{}, channel, action string) *banexg.OrderBook {
	instId := getMapString(item, "instId")
	if instId == "" {
		return nil
	}
	market := getMarketByIDAny(e, instId, "")
	symbol := instId
	if market != nil {
		symbol = market.Symbol
	}
	asksRaw := getMapSlice(item, "asks")
	bidsRaw := getMapSlice(item, "bids")
	asks := parseWsBookSide(asksRaw)
	bids := parseWsBookSide(bidsRaw)
	ts := parseInt(getMapString(item, "ts"))
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	e.OdBookLock.Lock()
	book, ok := e.OrderBooks[symbol]
	if !ok || action == "snapshot" {
		limit := len(asks)
		if len(bids) > limit {
			limit = len(bids)
		}
		if limit == 0 {
			limit = 400
		}
		book = &banexg.OrderBook{
			Symbol:    symbol,
			TimeStamp: ts,
			Asks:      banexg.NewOdBookSide(false, limit, asks),
			Bids:      banexg.NewOdBookSide(true, limit, bids),
			Limit:     limit,
			Cache:     make([]map[string]string, 0),
		}
		e.OrderBooks[symbol] = book
		e.OdBookLock.Unlock()
		return book
	}
	e.OdBookLock.Unlock()
	if len(asks) > 0 {
		book.Asks.Update(asks)
	}
	if len(bids) > 0 {
		book.Bids.Update(bids)
	}
	book.TimeStamp = ts
	return book
}

// -------- WS Parsers --------

func parseWsTradeItem(e *Bitget, item map[string]interface{}) *banexg.Trade {
	instId := getMapString(item, "instId")
	if instId == "" {
		return nil
	}
	symbol := instId
	if market := getMarketByIDAny(e, instId, ""); market != nil {
		symbol = market.Symbol
	}
	price := parseFloat(getMapString(item, "px"))
	amount := parseFloat(getMapString(item, "sz"))
	side := getMapString(item, "side")
	ts := parseInt(getMapString(item, "ts"))
	if ts == 0 {
		// Try timestamp field
		ts = parseInt(getMapString(item, "timestamp"))
	}
	return &banexg.Trade{
		ID:        getMapString(item, "tradeId"),
		Symbol:    symbol,
		Price:     price,
		Amount:    amount,
		Cost:      price * amount,
		Timestamp: ts,
		Side:      strings.ToLower(side),
		Info:      item,
	}
}

func parseWsMarkPriceItem(e *Bitget, item map[string]interface{}) (string, float64, string, string) {
	if item == nil {
		return "", 0, "", ""
	}
	instId := getMapString(item, "instId")
	if instId == "" {
		return "", 0, "", ""
	}
	symbol := instId
	marketType := ""
	if e != nil {
		if market := getMarketByIDAny(e, instId, ""); market != nil {
			symbol = market.Symbol
			marketType = market.Type
		}
	}
	if marketType == "" {
		marketType = banexg.MarketSpot
	}
	price := parseFloat(getMapString(item, "markPx"))
	if price == 0 {
		// Some implementations use "markPrice" instead of "markPx"
		price = parseFloat(getMapString(item, "markPrice"))
	}
	return symbol, price, marketType, instId
}

func parseWsCandleItem(item map[string]interface{}) *banexg.Kline {
	if item == nil {
		return nil
	}
	stamp := parseInt(getMapString(item, "0"))
	open := parseFloat(getMapString(item, "1"))
	high := parseFloat(getMapString(item, "2"))
	low := parseFloat(getMapString(item, "3"))
	closeP := parseFloat(getMapString(item, "4"))
	vol := parseFloat(getMapString(item, "5"))
	quoteVol := 0.0
	if val := getMapString(item, "6"); val != "" {
		quoteVol = parseFloat(val)
	}
	return &banexg.Kline{
		Time:   stamp,
		Open:   open,
		High:   high,
		Low:    low,
		Close:  closeP,
		Volume: vol,
		Quote:  quoteVol,
	}
}

// -------- WS Ping --------

func makeCheckWsTimeout(e *Bitget) func() {
	return func() {
		e.WsChecking = true
		defer func() {
			e.WsChecking = false
		}()
		pingInterval := time.Second * 20
		for {
			time.Sleep(pingInterval)
			for _, client := range e.WSClients {
				conns, lock := client.LockConns()
				for _, conn := range conns {
					// Bitget WS requires {"op":"ping"} JSON
					pingData := []byte(`{"op":"ping"}`)
					if err := client.WriteRaw(conn, pingData); err != nil {
						log.Warn("send ping fail", zap.String("url", client.URL),
							zap.Int("conn", conn.GetID()), zap.Error(err))
					}
				}
				lock.Unlock()
			}
		}
	}
}
