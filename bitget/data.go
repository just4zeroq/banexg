package bitget

import "github.com/banbox/banexg"

const (
	HostPublic   = "public"
	HostPrivate  = "private"
	HostWsSpot   = "ws_spot"
	HostWsMix    = "ws_mix"
)

// Bitget API field keys
const (
	FldSymbol   = "symbol"
	FldChannel  = "channel"
	FldInstType = "instType"
	FldInstId   = "instId"
	FldPeriod   = "period"
	FldAfter    = "after"
	FldBefore   = "before"
	FldLimit    = "limit"
	FldEnd      = "end"
)

// Bitget WebSocket channel names
const (
	WsChanTrades       = "trade"
	WsChanMarkPrice    = "mark-price"
	WsChanCandlePrefix = "candle"
	WsChanBooks        = "books"
	WsChanBooks5       = "books5"
)

// Bitget instType values (used in WS subscription args)
const (
	InstTypeSpot        = "spot"
	InstTypeUsdtFutures = "usdt-futures"
	InstTypeCoinFutures = "coin-futures"
	InstTypeUsdcFutures = "usdc-futures"
)

// WS operation constants
const (
	WsOpSubscribe   = "subscribe"
	WsOpUnsubscribe = "unsubscribe"
	WsOpLogin       = "login"
	WsOpPing        = "ping"
)

var (
	timeFrameMap = map[string]string{
		"1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m",
		"1h": "1H", "4h": "4H", "6h": "6H", "12h": "12H",
		"1d": "1DUTC", "3d": "3DUTC", "1w": "1WUTC", "1M": "1MUTC",
	}

	// marketToInstType maps banexg MarketType → Bitget instType
	marketToInstType = map[string]string{
		banexg.MarketSpot:   InstTypeSpot,
		banexg.MarketLinear: InstTypeUsdtFutures,
		banexg.MarketInverse: InstTypeCoinFutures,
		banexg.MarketSwap:   InstTypeUsdtFutures,
	}

	// instTypeToMarket maps Bitget instType → banexg MarketType
	instTypeToMarket = map[string]string{
		InstTypeSpot:        banexg.MarketSpot,
		InstTypeUsdtFutures: banexg.MarketLinear,
		InstTypeCoinFutures: banexg.MarketInverse,
		InstTypeUsdcFutures: banexg.MarketLinear,
	}
)

// API method name constants — spot
const (
	MethodSpotGetPublicProducts      = "spotGetPublicProducts"
	MethodSpotGetPublicTicker        = "spotGetPublicTicker"
	MethodSpotGetPublicTickers       = "spotGetPublicTickers"
	MethodSpotGetPublicKline         = "spotGetPublicKline"
	MethodSpotGetPublicHistoryKline  = "spotGetPublicHistoryKline"
	MethodSpotGetPublicDepth         = "spotGetPublicDepth"
	MethodSpotGetPublicFundingRate     = "spotGetPublicFundingRate" // not actually used
)

// API method name constants — mix (futures/perpetual)
const (
	MethodMixGetMarketContracts           = "mixGetMarketContracts"
	MethodMixGetMarketTicker              = "mixGetMarketTicker"
	MethodMixGetMarketTickers             = "mixGetMarketTickers"
	MethodMixGetMarketCandles             = "mixGetMarketCandles"
	MethodMixGetMarketHistoryCandles      = "mixGetMarketHistoryCandles"
	MethodMixGetMarketDepth               = "mixGetMarketDepth"
	MethodMixGetMarketCurrentFundingRate  = "mixGetMarketCurrentFundingRate"
	MethodMixGetMarketFundingTime         = "mixGetMarketFundingTime"
)

// API method name constants — private
const (
	MethodPrivateGetOrderHistory          = "privateGetOrderHistory"
)
