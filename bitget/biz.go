package bitget

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/utils"
)

func (e *Bitget) Init() *errs.Error {
	err := e.Exchange.Init()
	if err != nil {
		return err
	}
	if len(e.CareMarkets) == 0 {
		e.CareMarkets = banexg.DefaultCareMarkets()
	}
	e.ExgInfo.NoHoliday = true
	e.ExgInfo.FullDay = true
	return nil
}

func makeSign(e *Bitget) banexg.FuncSign {
	return func(api *banexg.Entry, args map[string]interface{}) *banexg.HttpReq {
		params := utils.SafeParams(args)
		accID := e.PopAccName(params)
		if err := e.CheckRiskyAllowed(api, accID); err != nil {
			return &banexg.HttpReq{Error: err, Private: true}
		}
		url := api.Url
		headers := http.Header{}
		body := ""
		isPrivate := api.Host == HostPrivate
		method := api.Method

		if method == "GET" && len(params) > 0 {
			url += "?" + utils.UrlEncodeMap(params, true)
		} else if method == "POST" && len(params) > 0 {
			body, _ = utils.MarshalString(params)
		}

		if isPrivate {
			var creds *banexg.Credential
			var err *errs.Error
			accID, creds, err = e.GetAccountCreds(accID)
			if err != nil {
				return &banexg.HttpReq{Error: err, Private: true}
			}

			timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
			requestPath := api.Path
			queryStr := ""
			if method == "GET" && len(params) > 0 {
				queryStr = utils.UrlEncodeMap(params, true)
				requestPath += "?" + queryStr
			}
			// Bitget sign payload: timestamp + method + requestPath + body
			payload := timestamp + method + "/" + requestPath + body
			sign, _ := utils.Signature(payload, creds.Secret, "hmac", "sha256", "base64")

			headers.Set("ACCESS-KEY", creds.ApiKey)
			headers.Set("ACCESS-SIGN", sign)
			headers.Set("ACCESS-TIMESTAMP", timestamp)
			headers.Set("ACCESS-PASSPHRASE", creds.Password)
			if method == "POST" {
				headers.Set("Content-Type", "application/json")
			}
		}

		return &banexg.HttpReq{AccName: accID, Url: url, Method: method, Headers: headers, Body: body, Private: isPrivate}
	}
}

// -------- Generic REST caller --------

func requestRetry[T any](e *Bitget, api string, params map[string]interface{}, tryNum int) *banexg.ApiRes[T] {
	noCache := utils.PopMapVal(params, banexg.ParamNoCache, false)
	res_ := e.RequestApiRetryAdv(context.Background(), api, params, tryNum, !noCache, false)
	res := &banexg.ApiRes[T]{HttpRes: res_}
	if res.Error != nil {
		return res
	}
	var rsp = struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data T      `json:"data"`
	}{}
	err := utils.UnmarshalString(res.Content, &rsp, utils.JsonNumDefault)
	if err != nil {
		res.Error = errs.New(errs.CodeUnmarshalFail, err)
		return res
	}
	if rsp.Code != "00000" {
		errMsg := rsp.Msg
		res.Error = errs.NewMsg(errs.CodeRunTime, "[%s] %s", rsp.Code, errMsg)
	} else {
		res.Result = rsp.Data
		e.CacheApiRes(api, res_)
	}
	return res
}

// -------- FetchMarkets --------

func makeFetchMarkets(e *Bitget) banexg.FuncFetchMarkets {
	return func(marketTypes []string, params map[string]interface{}) (banexg.MarketMap, *errs.Error) {
		result := make(banexg.MarketMap)
		if len(marketTypes) == 0 {
			return result, nil
		}
		tryNum := e.GetRetryNum("FetchMarkets", 1)

		for _, mktType := range marketTypes {
			switch mktType {
			case banexg.MarketSpot:
				spotMarkets, err := fetchSpotMarkets(e, tryNum)
				if err != nil {
					return nil, err
				}
				for sym, m := range spotMarkets {
					result[sym] = m
				}
			case banexg.MarketLinear, banexg.MarketSwap, banexg.MarketInverse:
				mixMarkets, err := fetchMixMarkets(e, mktType, tryNum)
				if err != nil {
					return nil, err
				}
				for sym, m := range mixMarkets {
					result[sym] = m
				}
			}
		}
		return result, nil
	}
}

func fetchSpotMarkets(e *Bitget, tryNum int) (banexg.MarketMap, *errs.Error) {
	res := requestRetry[[]Instrument](e, MethodSpotGetPublicProducts, map[string]interface{}{}, tryNum)
	if res.Error != nil {
		return nil, res.Error
	}
	result := make(banexg.MarketMap)
	for _, inst := range res.Result {
		market := parseSpotInstrument(e, &inst)
		if market != nil {
			applyMarketFees(e, market)
			result[market.Symbol] = market
		}
	}
	return result, nil
}

func fetchMixMarkets(e *Bitget, marketType string, tryNum int) (banexg.MarketMap, *errs.Error) {
	res := requestRetry[[]Instrument](e, MethodMixGetMarketContracts, map[string]interface{}{}, tryNum)
	if res.Error != nil {
		return nil, res.Error
	}
	result := make(banexg.MarketMap)
	for _, inst := range res.Result {
		market := parseMixInstrument(e, &inst, marketType)
		if market != nil {
			applyMarketFees(e, market)
			result[market.Symbol] = market
		}
	}
	return result, nil
}

func parseSpotInstrument(e *Bitget, inst *Instrument) *banexg.Market {
	if inst.Status != "online" {
		return nil
	}
	base := e.SafeCurrencyCode(inst.BaseCoin)
	quote := e.SafeCurrencyCode(inst.QuoteCoin)
	if base == "" || quote == "" {
		return nil
	}
	symbol := base + "/" + quote
	priceScale := parseFloat(inst.PriceScale)
	qtyScale := parseFloat(inst.QuantityScale)
	minTrade := parseFloat(inst.MinTradeNum)

	return &banexg.Market{
		ID:           inst.Symbol,
		Symbol:       symbol,
		Base:         base,
		Quote:        quote,
		Type:         banexg.MarketSpot,
		Spot:         true,
		Contract:     false,
		Precision: &banexg.Precision{
			Price:      priceScale,
			ModePrice:  banexg.PrecModeTickSize,
			Amount:     qtyScale,
			ModeAmount: banexg.PrecModeTickSize,
		},
		Limits: &banexg.MarketLimits{
			Amount: &banexg.LimitRange{Min: minTrade},
		},
		Active: inst.Status == "online",
	}
}

func parseMixInstrument(e *Bitget, inst *Instrument, marketType string) *banexg.Market {
	if inst.Status != "online" {
		return nil
	}
	base := e.SafeCurrencyCode(inst.BaseCoin)
	quote := e.SafeCurrencyCode(inst.QuoteCoin)
	settle := e.SafeCurrencyCode(inst.QuoteCoin) // mix contracts settle in quote currency by default
	if base == "" || quote == "" {
		return nil
	}
	// Determine if linear or inverse
	isLinear := true
	if strings.ToUpper(inst.QuoteCoin) == "USD" {
		isLinear = false
	}
	mktType := marketType
	if marketType == banexg.MarketSwap || marketType == "" {
		if isLinear {
			mktType = banexg.MarketLinear
		} else {
			mktType = banexg.MarketInverse
		}
	}
	symbol := base + "/" + quote + ":" + settle
	// For futures, would need expiry info — Bitget mix v1 is perpetual only
	contractSz := parseFloat(inst.Size)
	if contractSz == 0 {
		contractSz = 1
	}
	priceStep := parseFloat(inst.PriceEndStep)
	volPrecision := parseFloat(inst.Precision)
	minVol := parseFloat(inst.MinTradeVol)

	return &banexg.Market{
		ID:           inst.Symbol,
		Symbol:       symbol,
		Base:         base,
		Quote:        quote,
		Settle:       settle,
		Type:         mktType,
		Swap:         true,
		Contract:     true,
		Linear:       isLinear,
		Inverse:      !isLinear,
		ContractSize: contractSz,
		Precision: &banexg.Precision{
			Price:      priceStep,
			ModePrice:  banexg.PrecModeTickSize,
			Amount:     volPrecision,
			ModeAmount: banexg.PrecModeTickSize,
		},
		Limits: &banexg.MarketLimits{
			Amount:   &banexg.LimitRange{Min: minVol},
			Leverage: &banexg.LimitRange{},
			Price:    &banexg.LimitRange{},
			Cost:     &banexg.LimitRange{},
			Market:   &banexg.LimitRange{},
		},
		Active: inst.Status == "online",
	}
}

func applyMarketFees(e *Bitget, market *banexg.Market) {
	if market == nil || e.Fees == nil {
		return
	}
	if market.Spot {
		if e.Fees.Main != nil {
			market.Taker = e.Fees.Main.Taker
			market.Maker = e.Fees.Main.Maker
			market.FeeSide = e.Fees.Main.FeeSide
		}
		return
	}
	if market.Contract {
		fee := e.Fees.Linear
		if market.Inverse && e.Fees.Inverse != nil {
			fee = e.Fees.Inverse
		}
		if fee != nil {
			market.Taker = fee.Taker
			market.Maker = fee.Maker
			market.FeeSide = fee.FeeSide
		}
	}
}

// -------- Bitget-specific rest caller for public data --------

// restFetch fetches data from a public API endpoint using requestRetry and returns raw result.
func restFetch[T any](e *Bitget, api string, params map[string]interface{}) (*T, *errs.Error) {
	tryNum := e.GetRetryNum("restFetch", 1)
	res := requestRetry[T](e, api, params, tryNum)
	if res.Error != nil {
		return nil, res.Error
	}
	zero := new(T)
	*zero = res.Result
	return zero, nil
}

// restFetchSlice fetches a slice response from a public API endpoint.
func restFetchSlice[T any](e *Bitget, api string, params map[string]interface{}) ([]T, *errs.Error) {
	tryNum := e.GetRetryNum("restFetchSlice", 1)
	res := requestRetry[[]T](e, api, params, tryNum)
	if res.Error != nil {
		return nil, res.Error
	}
	return res.Result, nil
}
