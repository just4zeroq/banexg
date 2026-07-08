package bitget

import (
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/sasha-s/go-deadlock"
)

// WsPendingRecon stores info needed to restore subscriptions after reconnection login.
type WsPendingRecon struct {
	Client *banexg.WsClient
	ConnID int
	Keys   []string
}

type Bitget struct {
	*banexg.Exchange
	RecvWindow int
	// WsAuthDone tracks login completion channels per client key.
	WsAuthDone map[string]chan *errs.Error
	// WsAuthed tracks whether each client has successfully logged in.
	WsAuthed map[string]bool
	// WsPendingRecons stores pending reconnection info to restore subs after login.
	WsPendingRecons map[string]*WsPendingRecon
	WsAuthLock      deadlock.Mutex
}

// Instrument describes spot product or mix contract info.
type Instrument struct {
	Symbol       string `json:"symbol"`
	BaseCoin     string `json:"baseCoin"`
	QuoteCoin    string `json:"quoteCoin"`
	MinTradeNum  string `json:"minTradeNum"`
	MaxTradeNum  string `json:"maxTradeNum"`
	PriceScale   string `json:"priceScale"`
	QuantityScale string `json:"quantityScale"`
	MinTradeUSDT string `json:"minTradeUSDT"`
	Status       string `json:"status"`
	// Mix-specific fields
	BaseCoinName  string `json:"baseCoinName"`
	QuoteCoinName string `json:"quoteCoinName"`
	Size          string `json:"size"`        // contract size
	Leverage      string `json:"leverage"`    // max leverage
	MinTradeVol   string `json:"minTradeVol"` // min trade volume (contracts)
	MaxTradeVol   string `json:"maxTradeVol"`
	TradeVol      string `json:"tradeVol"`
	PriceEndStep  string `json:"priceEndStep"` // tick size
	Precision     string `json:"precision"`    // volume precision
}

// Ticker describes /spot/v1/public/ticker(s) or /mix/v1/market/ticker(s) response item.
type Ticker struct {
	Symbol   string `json:"symbol"`
	Last     string `json:"last"`
	BestAsk  string `json:"bestAsk"`
	BestBid  string `json:"bestBid"`
	High24h  string `json:"high24h"`
	Low24h   string `json:"low24h"`
	BaseVol  string `json:"baseVol"`  // 24h volume (base currency)
	QuoteVol string `json:"quoteVol"` // 24h volume (quote currency)
	USDTVol  string `json:"usdtVol"`  // mix: volume in USDT
	Open24h  string `json:"open24h"`  // mix only
	Ts       string `json:"timestamp"` // spot
	CTime    string `json:"cTime"`    // mix
}

// FundingRate describes mix funding rate response.
type FundingRate struct {
	Symbol       string `json:"symbol"`
	FundingRate  string `json:"fundingRate"`
	NextRate     string `json:"nextRate"`
	FundingTime  string `json:"fundingTime"`
	NextFundTime string `json:"nextFundingTime"`
	Timestamp    string `json:"timestamp"`
}
