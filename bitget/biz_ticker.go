package bitget

import (
	"strconv"
	"time"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/utils"
)

// -------- FetchTicker --------

func (e *Bitget) FetchTicker(symbol string, params map[string]interface{}) (*banexg.Ticker, *errs.Error) {
	args, market, err := e.LoadArgsMarket(symbol, params)
	if err != nil {
		return nil, err
	}
	api, err := e.tickerAPI(market)
	if err != nil {
		return nil, err
	}
	args[FldSymbol] = market.ID
	tryNum := e.GetRetryNum("FetchTicker", 1)
	res := requestRetry[[]map[string]interface{}](e, api, args, tryNum)
	if res.Error != nil {
		return nil, res.Error
	}
	items := res.Result
	if len(items) == 0 {
		return nil, errs.NewMsg(errs.CodeDataNotFound, "empty ticker result")
	}
	arr, err := decodeResult[Ticker](items)
	if err != nil {
		return nil, err
	}
	ticker := parseTicker(e, &arr[0], items[0], market.Type)
	if ticker == nil {
		return nil, errs.NewMsg(errs.CodeDataNotFound, "empty ticker")
	}
	return ticker, nil
}

// -------- FetchTickers --------

func (e *Bitget) FetchTickers(symbols []string, params map[string]interface{}) ([]*banexg.Ticker, *errs.Error) {
	args := utils.SafeParams(params)
	marketType, contractType, err := e.LoadArgsMarketType(args, symbols...)
	if err != nil {
		return nil, err
	}
	return e.fetchTickersByType(marketType, contractType, symbols, args)
}

func (e *Bitget) fetchTickersByType(marketType, contractType string, symbols []string, args map[string]interface{}) ([]*banexg.Ticker, *errs.Error) {
	api, err := e.tickersAPI(marketType, contractType)
	if err != nil {
		return nil, err
	}
	tryNum := e.GetRetryNum("FetchTickers", 1)
	res := requestRetry[[]map[string]interface{}](e, api, args, tryNum)
	if res.Error != nil {
		return nil, res.Error
	}
	items := res.Result
	arr, err := decodeResult[Ticker](items)
	if err != nil {
		return nil, err
	}
	result := make([]*banexg.Ticker, 0, len(arr))
	for i, item := range arr {
		ticker := parseTicker(e, &item, items[i], marketType)
		result = append(result, ticker)
	}
	symbolSet := banexg.BuildSymbolSet(symbols)
	return banexg.FilterTickers(result, symbolSet), nil
}

// -------- FetchTickerPrice --------

func (e *Bitget) FetchTickerPrice(symbol string, params map[string]interface{}) (map[string]float64, *errs.Error) {
	var symbols []string
	if symbol != "" {
		symbols = []string{symbol}
	}
	tickers, err := e.FetchTickers(symbols, params)
	if err != nil {
		return nil, err
	}
	return banexg.TickersToPriceMap(tickers, nil), nil
}

// -------- FetchLastPrices --------

func (e *Bitget) FetchLastPrices(symbols []string, params map[string]interface{}) ([]*banexg.LastPrice, *errs.Error) {
	tickers, err := e.FetchTickers(symbols, params)
	if err != nil {
		return nil, err
	}
	return banexg.TickersToLastPrices(tickers, nil), nil
}

// -------- FetchOHLCV --------

func (e *Bitget) FetchOHLCV(symbol, timeframe string, since int64, limit int, params map[string]interface{}) ([]*banexg.Kline, *errs.Error) {
	args, market, err := e.LoadArgsMarket(symbol, params)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 100
	}
	api, err := e.ohlcvAPI(market)
	if err != nil {
		return nil, err
	}
	args[FldSymbol] = market.ID
	args[FldPeriod] = e.GetTimeFrame(timeframe)
	until := utils.PopMapVal(args, banexg.ParamUntil, int64(0))

	// Bitget uses after/before for pagination
	if since > 0 {
		args[FldAfter] = strconv.FormatInt(since, 10)
	}
	if until > 0 {
		args[FldBefore] = strconv.FormatInt(until, 10)
	}

	// Determine if we need history endpoint for older data
	method := api
	if since > 0 {
		nowMs := time.Now().UnixMilli()
		if nowMs-since > 86400000 {
			// Use history endpoint if data is older than 1 day
			historyAPI, err := e.ohlcvHistoryAPI(market)
			if err == nil {
				method = historyAPI
			}
		}
	}
	tryNum := e.GetRetryNum("FetchOHLCV", 1)
	res := requestRetry[[][]string](e, method, args, tryNum)
	if res.Error != nil {
		return nil, res.Error
	}
	return parseOHLCV(res.Result), nil
}

// -------- FetchOrderBook --------

func (e *Bitget) FetchOrderBook(symbol string, limit int, params map[string]interface{}) (*banexg.OrderBook, *errs.Error) {
	args, market, err := e.LoadArgsMarket(symbol, params)
	if err != nil {
		return nil, err
	}
	api, err := e.depthAPI(market)
	if err != nil {
		return nil, err
	}
	args[FldSymbol] = market.ID
	if limit > 0 {
		args[FldLimit] = strconv.Itoa(limit)
	}
	tryNum := e.GetRetryNum("FetchOrderBook", 1)
	res := requestRetry[[]map[string]interface{}](e, api, args, tryNum)
	if res.Error != nil {
		return nil, res.Error
	}
	if len(res.Result) == 0 {
		return nil, errs.NewMsg(errs.CodeDataNotFound, "empty orderbook result")
	}
	return parseOrderBook(market, res.Result[0], limit), nil
}

// -------- API Routing Helpers --------

func (e *Bitget) tickerAPI(market *banexg.Market) (string, *errs.Error) {
	if market.Spot {
		return MethodSpotGetPublicTicker, nil
	}
	if market.Contract {
		return MethodMixGetMarketTicker, nil
	}
	return "", errs.NewMsg(errs.CodeParamInvalid, "unsupported market type: %s", market.Type)
}

func (e *Bitget) tickersAPI(marketType, contractType string) (string, *errs.Error) {
	if marketType == banexg.MarketSpot {
		return MethodSpotGetPublicTickers, nil
	}
	return MethodMixGetMarketTickers, nil
}

func (e *Bitget) ohlcvAPI(market *banexg.Market) (string, *errs.Error) {
	if market.Spot {
		return MethodSpotGetPublicKline, nil
	}
	if market.Contract {
		return MethodMixGetMarketCandles, nil
	}
	return "", errs.NewMsg(errs.CodeParamInvalid, "unsupported market type: %s", market.Type)
}

func (e *Bitget) ohlcvHistoryAPI(market *banexg.Market) (string, *errs.Error) {
	if market.Spot {
		return MethodSpotGetPublicHistoryKline, nil
	}
	if market.Contract {
		return MethodMixGetMarketHistoryCandles, nil
	}
	return "", errs.NewMsg(errs.CodeParamInvalid, "unsupported market type")
}

func (e *Bitget) depthAPI(market *banexg.Market) (string, *errs.Error) {
	if market.Spot {
		return MethodSpotGetPublicDepth, nil
	}
	if market.Contract {
		return MethodMixGetMarketDepth, nil
	}
	return "", errs.NewMsg(errs.CodeParamInvalid, "unsupported market type")
}

// -------- Parsers --------

func parseTicker(e *Bitget, item *Ticker, info map[string]interface{}, marketType string) *banexg.Ticker {
	if item == nil {
		return nil
	}
	symbol := e.SafeSymbol(item.Symbol, "", marketType)
	if symbol == "" {
		symbol = item.Symbol
	}
	last := parseFloat(item.Last)
	bid := parseFloat(item.BestBid)
	ask := parseFloat(item.BestAsk)
	high := parseFloat(item.High24h)
	low := parseFloat(item.Low24h)
	baseVol := parseFloat(item.BaseVol)
	quoteVol := parseFloat(item.QuoteVol)
	if quoteVol == 0 {
		quoteVol = parseFloat(item.USDTVol)
	}
	open := parseFloat(item.Open24h)
	ts := parseInt(item.Ts)
	if ts == 0 {
		ts = parseInt(item.CTime)
	}
	change := last - open
	pct := 0.0
	if open > 0 {
		pct = change / open * 100
	}
	return &banexg.Ticker{
		Symbol:      symbol,
		TimeStamp:   ts,
		Bid:         bid,
		Ask:         ask,
		High:        high,
		Low:         low,
		Open:        open,
		Close:       last,
		Last:        last,
		Change:      change,
		Percentage:  pct,
		BaseVolume:  baseVol,
		QuoteVolume: quoteVol,
		Info:        info,
	}
}

func parseOrderBook(market *banexg.Market, item map[string]interface{}, limit int) *banexg.OrderBook {
	if item == nil || market == nil {
		return nil
	}
	asksRaw := getMapSlice(item, "asks")
	bidsRaw := getMapSlice(item, "bids")
	asks := parseWsBookSide(asksRaw)
	bids := parseWsBookSide(bidsRaw)
	ts := parseInt(getMapString(item, "ts"))
	if ts == 0 {
		ts = parseInt(getMapString(item, "timestamp"))
	}
	return &banexg.OrderBook{
		Symbol:    market.Symbol,
		TimeStamp: ts,
		Asks:      banexg.NewOdBookSide(false, len(asks), asks),
		Bids:      banexg.NewOdBookSide(true, len(bids), bids),
		Limit:     limit,
		Cache:     make([]map[string]string, 0),
	}
}

func parseOHLCV(rows [][]string) []*banexg.Kline {
	if len(rows) == 0 {
		return nil
	}
	res := make([]*banexg.Kline, 0, len(rows))
	for _, row := range rows {
		if len(row) < 6 {
			continue
		}
		stamp := parseInt(row[0])
		open := parseFloat(row[1])
		high := parseFloat(row[2])
		low := parseFloat(row[3])
		closeP := parseFloat(row[4])
		vol := parseFloat(row[5])
		quoteVol := 0.0
		if len(row) > 6 {
			quoteVol = parseFloat(row[6])
		}
		res = append(res, &banexg.Kline{
			Time:   stamp,
			Open:   open,
			High:   high,
			Low:    low,
			Close:  closeP,
			Volume: vol,
			Quote:  quoteVol,
		})
	}
	// Bitget returns ascending order, no reversal needed
	return res
}
