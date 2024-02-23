import datetime
from dateutil import tz
import jquantsapi
import pandas
import numpy
import mplfinance as mpf

class Market:
    def __init__(self):
        self._cli = jquantsapi.Client()

    def cli(self):
        return self._cli

    def get_brands(self):
        if not hasattr(self,'_brands'):
            self._brands = self.cli().get_listed_info()
        return self._brands
    
    ##水産・農林業 0050
    ##鉱業 1050
    ##建設業 2050
    ##食料品 3050
    ##繊維製品 3100
    ##パルプ・紙 3150
    ##化学 3200
    ##医薬品 3250
    ##石油・石炭製品 3300
    ##ゴム製品 3350
    ##ガラス・土石製品 3400
    ##鉄鋼 3450
    ##非鉄金属 3500
    ##金属製品 3550
    ##機械 3600
    ##電気機器 3650
    ##輸送用機器 3700
    ##精密機器 3750
    ##その他製品 3800
    ##電気・ガス業 4050
    ##陸運業 5050
    ##海運業 5100
    ##空運業 5150
    ##倉庫・運輸関連業 5200
    ##情報・通信業 5250
    ##卸売業 6050
    ##小売業 6100
    ##銀行業 7050
    ##証券、商品先物取引業 7100
    ##保険業 7150
    ##その他金融業 7200
    ##不動産業 8050
    ##サービス業 9050
    ##その他 9999
class Sector33():
    def __init__(self,code):
        self._code = code

        # BrandオブジェクトのList
        self._brands = self._get_brands()
    
    def _get_brands(self):
        market = Market()
        all_brands = market.get_brands()
        category_brands = all_brands[all_brands['Sector33Code'] == f'{self._code}']
        brands = []
        for brand_code in category_brands.loc[:,"Code"].to_list():
            brands.append = Brand(brand_code)
        return brands

class Brand(Market):
    def __init__(self,code):
        super().__init__()
        self._code = code
        # free planは12週間前〜2年12週間前までしか取れない。
        # https://jpx.gitbook.io/j-quants-ja/outline/data-spec
        self._end = datetime.datetime.now(tz=tz.gettz('Asia/Tokyo')).date()-datetime.timedelta(weeks=12)
        self._start = datetime.date(year=self._end.year-2,month=self._end.month,day=self._end.day)

    # 企業情報
    # code
    def get_code(self):
        return self._code

    # 名前
    def get_company_name(self):
        brand = self.get_info()
        return brand['CompanyName']

    def get_info(self):
        brands = super().get_brands()
        index = brands[brands['Code'] == self._code].index
        return brands.iloc[index].iloc[-1].to_dict()

    # チャート
    def get_prices(self):
        if not hasattr(self,'_price'):
            df = super().cli().get_prices_daily_quotes(
                code=self._code,
                from_yyyymmdd = self._get_yyyymmdd(self._start),
                to_yyyymmdd = self._get_yyyymmdd(self._end)
            )
            df['Timestamp'] = pandas.to_datetime(df['Date'])
            self._price = df
        return self._price
    
    # graph
    def make_graph(self,path:str='./graph.png'):
        df = self.get_prices()[["Timestamp","Open","High","Low","Close","Volume"]]
        df.set_index('Timestamp', inplace = True)
        mpf.plot(df[-100:], type='candle',
            figratio=(5,4),volume=True,
            mav=(5, 25), style='yahoo',
            savefig=path
        )

    # 決算情報
    #一株当たりの純利益
    def get_EarningsPerShare(self):
        return self.get_fins_statements().loc[:,['CurrentPeriodEndDate','EarningsPerShare']]

    def get_fins_statements(self):
        if not hasattr(self,'_fins_statements'):
            df = super().cli().get_fins_statements(code=self._code)
            df = df.replace(to_replace='', value=numpy.nan)
            for column in ['NetSales','OperatingProfit','OrdinaryProfit','Profit','EarningsPerShare','DilutedEarningsPerShare'
                        ,'TotalAssets','Equity','EquityToAssetRatio','BookValuePerShare'
                        ,'CashFlowsFromOperatingActivities','CashFlowsFromInvestingActivities','CashFlowsFromFinancingActivities'
                        ,'CashAndEquivalents','ResultDividendPerShare1stQuarter','ResultDividendPerShare2ndQuarter'
                        ,'ResultDividendPerShare3rdQuarter','ResultDividendPerShareFiscalYearEnd','ResultDividendPerShareAnnual'
                        ,'ResultTotalDividendPaidAnnual','ResultPayoutRatioAnnual'
                        ,'ForecastDividendPerShare1stQuarter','ForecastDividendPerShare2ndQuarter','ForecastDividendPerShare3rdQuarter'
                        ,'ForecastDividendPerShareFiscalYearEnd','ForecastDividendPerShareAnnual','ForecastTotalDividendPaidAnnual'
                        ,'ForecastPayoutRatioAnnual','NextYearForecastDividendPerShare1stQuarter','NextYearForecastDividendPerShare2ndQuarter'
                        ,'NextYearForecastDividendPerShare3rdQuarter','NextYearForecastDividendPerShareFiscalYearEnd','NextYearForecastDividendPerShareAnnual'
                        ,'NextYearForecastPayoutRatioAnnual','ForecastNetSales2ndQuarter','ForecastOperatingProfit2ndQuarter'
                        ,'ForecastOrdinaryProfit2ndQuarter','ForecastProfit2ndQuarter','ForecastEarningsPerShare2ndQuarter'
                        ,'NextYearForecastNetSales2ndQuarter','NextYearForecastOperatingProfit2ndQuarter','NextYearForecastOrdinaryProfit2ndQuarter'
                        ,'NextYearForecastProfit2ndQuarter','NextYearForecastEarningsPerShare2ndQuarter','ForecastNetSales'
                        ,'ForecastOperatingProfit','ForecastOrdinaryProfit','ForecastProfit','ForecastEarningsPerShare','NextYearForecastNetSales'
                        ,'NextYearForecastOperatingProfit','NextYearForecastOrdinaryProfit','NextYearForecastProfit'
                        ,'NextYearForecastEarningsPerShare','NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock'
                        ,'NumberOfTreasuryStockAtTheEndOfFiscalYear','AverageNumberOfShares','NonConsolidatedNetSales','NonConsolidatedOperatingProfit'
                        ,'NonConsolidatedOrdinaryProfit','NonConsolidatedProfit','NonConsolidatedEarningsPerShare','NonConsolidatedTotalAssets'
                        ,'NonConsolidatedEquity','NonConsolidatedEquityToAssetRatio','NonConsolidatedBookValuePerShare'
                        ,'ForecastNonConsolidatedNetSales2ndQuarter','ForecastNonConsolidatedOperatingProfit2ndQuarter'
                        ,'ForecastNonConsolidatedOrdinaryProfit2ndQuarter','ForecastNonConsolidatedProfit2ndQuarter'
                        ,'ForecastNonConsolidatedEarningsPerShare2ndQuarter','NextYearForecastNonConsolidatedNetSales2ndQuarter'
                        ,'NextYearForecastNonConsolidatedOperatingProfit2ndQuarter','NextYearForecastNonConsolidatedOrdinaryProfit2ndQuarter'
                        ,'NextYearForecastNonConsolidatedProfit2ndQuarter','NextYearForecastNonConsolidatedEarningsPerShare2ndQuarter'
                        ,'ForecastNonConsolidatedNetSales','ForecastNonConsolidatedOperatingProfit','ForecastNonConsolidatedOrdinaryProfit'
                        ,'ForecastNonConsolidatedProfit','ForecastNonConsolidatedEarningsPerShare','NextYearForecastNonConsolidatedNetSales'
                        ,'NextYearForecastNonConsolidatedOperatingProfit','NextYearForecastNonConsolidatedOrdinaryProfit','NextYearForecastNonConsolidatedProfit'
                        ,'NextYearForecastNonConsolidatedEarningsPerShare']:
                df[column] = pandas.to_numeric(df[column], errors="coerce")
            self._fins_statements = df
        return self._fins_statements

    def _get_yyyymmdd(self,date:datetime.date):
        return f'{date.year:04}{date.month:02}{date.day:02}'