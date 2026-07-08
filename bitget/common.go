package bitget

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/utils"
)

// instTypeFromMarket derives Bitget instType from a resolved market.
func instTypeFromMarket(market *banexg.Market) string {
	if market == nil {
		return ""
	}
	if market.Spot {
		return InstTypeSpot
	}
	if market.Swap || market.Future {
		if market.Linear {
			return InstTypeUsdtFutures
		}
		return InstTypeCoinFutures
	}
	if market.Margin {
		return InstTypeSpot
	}
	return marketToInstType[market.Type]
}

// marketFromInstType converts Bitget instType to banexg market type.
func marketFromInstType(instType string) string {
	t, ok := instTypeToMarket[instType]
	if ok {
		return t
	}
	return banexg.MarketSpot
}

// instTypeForSymbol determines the WS instType and market info for a symbol.
func instTypeForSymbol(e *Bitget, symbol string) (string, *banexg.Market, error) {
	market, err := e.GetMarket(symbol)
	if err != nil {
		return "", nil, err
	}
	instType := instTypeFromMarket(market)
	if instType == "" {
		return "", nil, errs.NewMsg(errs.CodeParamInvalid, "unsupported market type for symbol: %s", symbol)
	}
	return instType, market, nil
}

// getMarketByIDAny looks up a market by its raw exchange ID.
func getMarketByIDAny(e *Bitget, marketId, marketType string) *banexg.Market {
	if e == nil {
		return nil
	}
	market := e.GetMarketById(marketId, marketType)
	if market != nil {
		return market
	}
	e.MarketsByIdLock.Lock()
	list := e.MarketsById[marketId]
	e.MarketsByIdLock.Unlock()
	if len(list) > 0 {
		return list[0]
	}
	return nil
}

func parseFloat(val string) float64 {
	out, _ := strconv.ParseFloat(val, 64)
	return out
}

func parseInt(val string) int64 {
	out, _ := strconv.ParseInt(val, 10, 64)
	return out
}

func parseBoolStr(val string) bool {
	val = strings.ToLower(strings.TrimSpace(val))
	return val == "true" || val == "1"
}

// decodeResult decodes raw map slice to struct slice.
func decodeResult[T any](items []map[string]interface{}) ([]T, *errs.Error) {
	var arr []T
	if err := utils.DecodeStructMap(items, &arr, "json"); err != nil {
		return nil, errs.New(errs.CodeUnmarshalFail, err)
	}
	return arr, nil
}

// intervalSeconds returns the duration of one kline interval in seconds.
func intervalSeconds(interval string) int64 {
	switch interval {
	case "1m":
		return 60
	case "5m":
		return 300
	case "15m":
		return 900
	case "30m":
		return 1800
	case "1h":
		return 3600
	case "4h":
		return 14400
	case "6h":
		return 21600
	case "12h":
		return 43200
	case "1d":
		return 86400
	case "1w":
		return 604800
	default:
		return 60
	}
}

// convertToContractSymbol converts a Bitget mix symbol (e.g., BTCUSDT_USDT)
// to banexg unified symbol (e.g., BTC/USDT:USDT).
func convertToContractSymbol(baseCoin, quoteCoin, settleCoin string) string {
	symbol := baseCoin
	if quoteCoin != "" {
		symbol = baseCoin + "/" + quoteCoin
	}
	if settleCoin != "" && settleCoin != quoteCoin {
		symbol = symbol + ":" + settleCoin
	}
	return symbol
}

// isSpotInstType returns true if instType is spot.
func isWsInstType(val string) bool {
	switch val {
	case InstTypeSpot, InstTypeUsdtFutures, InstTypeCoinFutures, InstTypeUsdcFutures:
		return true
	default:
		return false
	}
}

// buildWsKey creates a WS subscription key.
func buildWsKey(channel, instId string) string {
	if instId == "" {
		return channel
	}
	return channel + ":" + instId
}

// parseWsKey splits a WS key into channel, instType, instId.
func parseWsKey(key string) (string, string, string) {
	parts := strings.Split(key, ":")
	if len(parts) == 1 {
		return parts[0], "", ""
	}
	if len(parts) == 2 {
		if isWsInstType(parts[1]) {
			return parts[0], parts[1], ""
		}
		return parts[0], "", parts[1]
	}
	return parts[0], parts[1], strings.Join(parts[2:], ":")
}

// parseWsBookSide parses order book side from numeric-indexed maps.
func parseWsBookSide(levels []map[string]interface{}) [][2]float64 {
	if len(levels) == 0 {
		return nil
	}
	res := make([][2]float64, 0, len(levels))
	for _, lvl := range levels {
		price := parseFloat(getMapString(lvl, "0"))
		size := parseFloat(getMapString(lvl, "1"))
		if price == 0 && size == 0 {
			continue
		}
		res = append(res, [2]float64{price, size})
	}
	return res
}

// updateAccLeverages updates account leverages from positions and returns config changes.
func updateAccLeverages(acc *banexg.Account, positions []*banexg.Position) []*banexg.AccountConfig {
	if acc == nil || len(positions) == 0 {
		return nil
	}
	acc.LockLeverage.Lock()
	defer acc.LockLeverage.Unlock()
	updates := make([]*banexg.AccountConfig, 0)
	for _, pos := range positions {
		if pos == nil || pos.Symbol == "" || pos.Leverage <= 0 {
			continue
		}
		cur, ok := acc.Leverages[pos.Symbol]
		if !ok || cur != pos.Leverage {
			acc.Leverages[pos.Symbol] = pos.Leverage
			updates = append(updates, &banexg.AccountConfig{Symbol: pos.Symbol, Leverage: pos.Leverage})
		}
	}
	return updates
}

// getMapSlice safely extracts a slice of maps from a map.
func getMapSlice(m map[string]interface{}, key string) []map[string]interface{} {
	if m == nil {
		return nil
	}
	val, ok := m[key]
	if !ok {
		return nil
	}
	switch v := val.(type) {
	case []interface{}:
		result := make([]map[string]interface{}, 0, len(v))
		for _, item := range v {
			if itemMap, ok := item.(map[string]interface{}); ok {
				result = append(result, itemMap)
			}
		}
		return result
	case []map[string]interface{}:
		return v
	default:
		return nil
	}
}

// getMapString safely extracts a string from a map.
func getMapString(m map[string]interface{}, key string) string {
	if m == nil {
		return ""
	}
	val, ok := m[key]
	if !ok {
		return ""
	}
	switch v := val.(type) {
	case string:
		return v
	case json.Number:
		return v.String()
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case int64:
		return strconv.FormatInt(v, 10)
	case int:
		return strconv.Itoa(v)
	default:
		return fmt.Sprint(v)
	}
}
