package bitget

import (
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/utils"
)

// -------- FetchFundingRate --------

func (e *Bitget) FetchFundingRate(symbol string, params map[string]interface{}) (*banexg.FundingRateCur, *errs.Error) {
	args, market, err := e.LoadArgsMarket(symbol, params)
	if err != nil {
		return nil, err
	}
	if !market.Contract {
		return nil, errs.NewMsg(errs.CodeParamInvalid, "funding rate only available for contract markets")
	}
	args[FldSymbol] = market.ID
	tryNum := e.GetRetryNum("FetchFundingRate", 1)
	res := requestRetry[[]FundingRate](e, MethodMixGetMarketCurrentFundingRate, args, tryNum)
	if res.Error != nil {
		return nil, res.Error
	}
	if len(res.Result) == 0 {
		return nil, errs.NewMsg(errs.CodeDataNotFound, "empty funding rate result")
	}
	return parseFundingRate(e, &res.Result[0], market.Type), nil
}

// -------- FetchFundingRates --------

func (e *Bitget) FetchFundingRates(symbols []string, params map[string]interface{}) ([]*banexg.FundingRateCur, *errs.Error) {
	args := utils.SafeParams(params)
	// If symbols specified, fetch one by one
	if len(symbols) > 0 {
		result := make([]*banexg.FundingRateCur, 0, len(symbols))
		for _, sym := range symbols {
			fr, err := e.FetchFundingRate(sym, args)
			if err != nil {
				return nil, err
			}
			result = append(result, fr)
		}
		return result, nil
	}
	// Otherwise fetch all contracts' funding rates
	// Bitget doesn't have a batch funding rate endpoint for all symbols,
	// so we load market info first then fetch per symbol
	markets, err := e.LoadMarkets(false, nil)
	if err != nil {
		return nil, err
	}
	result := make([]*banexg.FundingRateCur, 0)
	for sym, m := range markets {
		if !m.Contract {
			continue
		}
		fr, err := e.FetchFundingRate(sym, args)
		if err != nil {
			continue // skip errors for individual symbols
		}
		result = append(result, fr)
	}
	return result, nil
}

// -------- Parsers --------

func parseFundingRate(e *Bitget, fr *FundingRate, marketType string) *banexg.FundingRateCur {
	symbol := e.SafeSymbol(fr.Symbol, "", marketType)
	if symbol == "" {
		symbol = fr.Symbol
	}
	return &banexg.FundingRateCur{
		Symbol:              symbol,
		FundingRate:         parseFloat(fr.FundingRate),
		NextFundingRate:     parseFloat(fr.NextRate),
		FundingTimestamp:    parseInt(fr.FundingTime),
		NextFundingTimestamp: parseInt(fr.NextFundTime),
		Timestamp:           parseInt(fr.Timestamp),
	}
}
