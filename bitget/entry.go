package bitget

import (
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

func New(options map[string]interface{}) (*Bitget, *errs.Error) {
	exg := &Bitget{
		WsPendingRecons: make(map[string]*WsPendingRecon),
		Exchange: &banexg.Exchange{
			ExgInfo: &banexg.ExgInfo{
				ID:        "bitget",
				Name:      "Bitget",
				Countries: []string{"SC"},
			},
			RateLimit:  20,
			Options:    options,
			TimeFrames: timeFrameMap,
			Hosts: &banexg.ExgHosts{
				Test: map[string]string{
					HostPublic:  "https://api.bitget.com",
					HostPrivate: "https://api.bitget.com",
					HostWsSpot:  "wss://ws.bitget.com/spot/v1/stream",
					HostWsMix:   "wss://ws.bitget.com/mix/v1/stream",
				},
				Prod: map[string]string{
					HostPublic:  "https://api.bitget.com",
					HostPrivate: "https://api.bitget.com",
					HostWsSpot:  "wss://ws.bitget.com/spot/v1/stream",
					HostWsMix:   "wss://ws.bitget.com/mix/v1/stream",
				},
				Www: "https://www.bitget.com",
				Doc: []string{
					"https://bitgetlimited.github.io/apidoc/en/mix/",
				},
			},
			Fees: &banexg.ExgFee{
				Main:   &banexg.TradeFee{FeeSide: "get", Taker: 0.001, Maker: 0.001, Percentage: true},
				Linear: &banexg.TradeFee{FeeSide: "quote", Taker: 0.0006, Maker: 0.0002, Percentage: true},
				Inverse: &banexg.TradeFee{FeeSide: "base", Taker: 0.0006, Maker: 0.0002, Percentage: true},
			},
			Apis: map[string]*banexg.Entry{
				// Spot public
				MethodSpotGetPublicProducts:     {Path: "api/spot/v1/public/products", Host: HostPublic, Method: "GET", Cost: 1},
				MethodSpotGetPublicTicker:       {Path: "api/spot/v1/public/ticker", Host: HostPublic, Method: "GET", Cost: 1},
				MethodSpotGetPublicTickers:      {Path: "api/spot/v1/public/tickers", Host: HostPublic, Method: "GET", Cost: 1},
				MethodSpotGetPublicKline:        {Path: "api/spot/v1/public/kline", Host: HostPublic, Method: "GET", Cost: 1},
				MethodSpotGetPublicHistoryKline: {Path: "api/spot/v1/public/history-kline", Host: HostPublic, Method: "GET", Cost: 1},
				MethodSpotGetPublicDepth:        {Path: "api/spot/v1/public/depth", Host: HostPublic, Method: "GET", Cost: 1},
				// Mix public
				MethodMixGetMarketContracts:          {Path: "api/mix/v1/market/contracts", Host: HostPublic, Method: "GET", Cost: 1},
				MethodMixGetMarketTicker:             {Path: "api/mix/v1/market/ticker", Host: HostPublic, Method: "GET", Cost: 1},
				MethodMixGetMarketTickers:            {Path: "api/mix/v1/market/tickers", Host: HostPublic, Method: "GET", Cost: 1},
				MethodMixGetMarketCandles:            {Path: "api/mix/v1/market/candles", Host: HostPublic, Method: "GET", Cost: 1},
				MethodMixGetMarketHistoryCandles:     {Path: "api/mix/v1/market/history-candles", Host: HostPublic, Method: "GET", Cost: 1},
				MethodMixGetMarketDepth:              {Path: "api/mix/v1/market/depth", Host: HostPublic, Method: "GET", Cost: 1},
				MethodMixGetMarketCurrentFundingRate: {Path: "api/mix/v1/market/current-funding-rate", Host: HostPublic, Method: "GET", Cost: 1},
				MethodMixGetMarketFundingTime:        {Path: "api/mix/v1/market/funding-time", Host: HostPublic, Method: "GET", Cost: 1},
			},
			Has: map[string]map[string]int{
				"": {
					banexg.ApiFetchTicker:         banexg.HasOk,
					banexg.ApiFetchTickers:        banexg.HasOk,
					banexg.ApiFetchTickerPrice:    banexg.HasOk,
					banexg.ApiFetchOHLCV:          banexg.HasOk,
					banexg.ApiFetchOrderBook:      banexg.HasOk,
					banexg.ApiWatchTrades:         banexg.HasOk,
					banexg.ApiUnWatchTrades:       banexg.HasOk,
					banexg.ApiWatchMarkPrices:     banexg.HasOk,
					banexg.ApiUnWatchMarkPrices:   banexg.HasOk,
					banexg.ApiWatchOHLCVs:         banexg.HasOk,
					banexg.ApiUnWatchOHLCVs:       banexg.HasOk,
					banexg.ApiWatchOrderBooks:     banexg.HasFail,
					banexg.ApiUnWatchOrderBooks:   banexg.HasFail,
					banexg.ApiFetchCurrencies:     banexg.HasFail,
					banexg.ApiLoadLeverageBrackets: banexg.HasFail,
					banexg.ApiGetLeverage:         banexg.HasFail,
					banexg.ApiFetchOrder:          banexg.HasFail,
					banexg.ApiFetchOrders:         banexg.HasFail,
					banexg.ApiFetchBalance:        banexg.HasFail,
					banexg.ApiFetchAccountPositions: banexg.HasFail,
					banexg.ApiFetchPositions:      banexg.HasFail,
					banexg.ApiFetchOpenOrders:     banexg.HasFail,
					banexg.ApiCreateOrder:         banexg.HasFail,
					banexg.ApiEditOrder:           banexg.HasFail,
					banexg.ApiCancelOrder:         banexg.HasFail,
					banexg.ApiSetLeverage:         banexg.HasFail,
					banexg.ApiCalcMaintMargin:     banexg.HasFail,
					banexg.ApiWatchMyTrades:       banexg.HasFail,
					banexg.ApiWatchBalance:        banexg.HasFail,
					banexg.ApiWatchPositions:      banexg.HasFail,
					banexg.ApiWatchAccountConfig:  banexg.HasFail,
				},
			},
			CredKeys: map[string]bool{"ApiKey": true, "Secret": true, "Password": true},
		},
		WsAuthDone: make(map[string]chan *errs.Error),
		WsAuthed:   make(map[string]bool),
	}
	exg.Sign = makeSign(exg)
	exg.FetchMarkets = makeFetchMarkets(exg)
	exg.OnWsMsg = makeHandleWsMsg(exg)
	exg.OnWsReCon = makeHandleWsReCon(exg)
	exg.CheckWsTimeout = makeCheckWsTimeout(exg)
	err := exg.Init()
	return exg, err
}

func NewExchange(options map[string]interface{}) (banexg.BanExchange, *errs.Error) {
	return New(options)
}
