import datetime
from dateutil import tz
import jquantsapi
import pandas
import numpy
import mplfinance as mpf
import matplotlib

class Market:
    def __init__(self):
        self._cli = jquantsapi.Client()

    def cli(self):
        return self._cli

    def get_brands(self):
        if not hasattr(self,'_brands'):
            self._brands = self.cli().get_listed_info()
        return self._brands


    def _get_all_brands(self):
        all_brands = self.get_brands()
        #category_brands = all_brands[all_brands['MarketCode'].isin(['0112']) & all_brands['Code'].isin(['22260','48160'])]
        category_brands = all_brands[all_brands['MarketCode'].isin(['0111'])]
        #category_brands = all_brands[all_brands['MarketCode'].isin(['0111','0112','0113'])]
        brands = []
        for brand_code in category_brands.loc[:,"Code"].to_list():
            brands.append(Brand(brand_code))
        return brands

    def _get_latest_datas(self,brand):
            return brand.get_latests_data()

    def get_all_latest_datas(self):
        dfs = []
        from concurrent import futures
        with futures.ProcessPoolExecutor() as executor:
            dfs = list(
                executor.map(self._get_latest_datas, self._get_all_brands())
            )
        df = pandas.DataFrame()
        df = pandas.concat(dfs,axis=0)
        df.reset_index(inplace=True, drop=True)
        return df

    def make_scatter_roe_pbr(self):
        df = self.get_all_latest_datas()
        #予想純利益
        df['ForecastTotalProfit'] = df['Profit']/((df['CurrentPeriodEndDate']-df['CurrentPeriodStartDate'])/(df['CurrentFiscalYearEndDate']-df['CurrentFiscalYearStartDate']))
        #自己資本
        df["EquityToAsset"] = df['Equity']*df["EquityToAssetRatio"]

        df["ROE"] = df['ForecastTotalProfit']/df["EquityToAsset"]
        df["PBR"] = df["Close"]/df["BookValuePerShare"]
        matplotlib.pyplot.scatter(df["ROE"],df["PBR"])
        #x軸
        matplotlib.pyplot.xlabel("ROE")
        matplotlib.pyplot.xlim(0, 0.4)
        #y軸
        matplotlib.pyplot.ylabel("PBR")
        matplotlib.pyplot.ylim(0, 4)
        matplotlib.pyplot.grid(True)
        matplotlib.pyplot.savefig("ROEvPBR.png")
        return df

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
            brands.append(Brand(brand_code))
        return brands

class Brand(Market):
    def __init__(self,code,plan='free'):
        super().__init__()
        self._code = code
        if plan == 'light':
            self._end = datetime.datetime.now(tz=tz.gettz('Asia/Tokyo')).date()
            self._start = datetime.date(year=self._end.year-5,month=self._end.month,day=self._end.day)
        elif plan == 'standard':
            self._end = datetime.datetime.now(tz=tz.gettz('Asia/Tokyo')).date()
            self._start = datetime.date(year=self._end.year-10,month=self._end.month,day=self._end.day)
        elif plan == 'premium':
            self._end = datetime.datetime.now(tz=tz.gettz('Asia/Tokyo')).date()
            self._start = datetime.date(year=1900,month=1,day=1)
        else:
            # free planは12週間前〜2年12週間前までしか取れない。
            # https://jpx.gitbook.io/j-quants-ja/outline/data-spec
            self._end = datetime.datetime.now(tz=tz.gettz('Asia/Tokyo')).date()-datetime.timedelta(weeks=12)
            self._start = datetime.date(year=self._end.year-2,month=self._end.month,day=self._end.day)

    # 企業情報
    # code
    def get_code(self):
        return self._code

    # 名前
    def get_company_name(self,lang='en'):
        brand = self.get_info()
        if lang == 'jp':
            return brand['CompanyName']
        else:
            return brand['CompanyNameEnglish']

    # 33セクター分類
    def get_sector33(self):
        brand = self.get_info()
        return brand['Sector33Code']

    def get_info(self):
        brands = super().get_brands()
        index = brands[brands['Code'] == str(self._code)].index
        return brands.iloc[index].iloc[-1].to_dict()

    # チャート
    def get_prices(self,start=None,end=None):
        if not hasattr(self,'_price'):
            df = super().cli().get_prices_daily_quotes(
                code=self._code,
                from_yyyymmdd = self._get_yyyymmdd(start if start is not None else self._start),
                to_yyyymmdd = self._get_yyyymmdd(end if end is not None else self._end)
            )
            df['Timestamp'] = pandas.to_datetime(df['Date'])
            self._price = df
        return self._price.copy()
    
    def get_latest_prices(self):
        start = self._end-datetime.timedelta(weeks=1)
        return self.get_prices(start=start).tail(1).reset_index(drop=True)

    # graph
    def make_graph(self,path:str='./graph.png',plot:str='ROE'):
        df = self.get_prices()[["Timestamp","Open","High","Low","Close","Volume"]]
        df.reset_index(inplace=True, drop=True)
        df.set_index('Timestamp', inplace = True)
        df,add_df = self._add_plot(df,plot=plot)
        mpf.plot(df, type='candle',
            figratio=(5,4),volume=True,
            mav=(3, 12), style='yahoo',
            title=f'{self.get_company_name()} over {plot}',
            datetime_format='%Y/%m/%d',
            addplot=add_df,
            savefig=path
        )

    #addplot
    def _add_plot(self,chart_df:pandas.DataFrame, plot:str):
        df = self.get_fins_statements()
        #PER14,15,16倍線
        if plot == 'PER':
            df['Timestamp'] = pandas.to_datetime(df['DisclosedDate'])
            # 一株当たりの純利益の当期予想を残期間によって算出
            df['ForecastTotalEarningsPerShare'] = df['EarningsPerShare']/((df['CurrentPeriodEndDate']-df['CurrentPeriodStartDate'])/(df['CurrentFiscalYearEndDate']-df['CurrentFiscalYearStartDate']))
            df['EarningsPerShareX16'] = df['ForecastTotalEarningsPerShare'].apply(lambda x: x * 16)
            df['EarningsPerShareX15'] = df['ForecastTotalEarningsPerShare'].apply(lambda x: x * 15)
            df['EarningsPerShareX14'] = df['ForecastTotalEarningsPerShare'].apply(lambda x: x * 14)
            df = df.loc[:,['Timestamp','EarningsPerShare','ForecastTotalEarningsPerShare','EarningsPerShareX16','EarningsPerShareX15','EarningsPerShareX14']]
            df.reset_index(inplace=True, drop=True)
            df.set_index('Timestamp', inplace = True)
            print(chart_df)
            print(df)

            #chartとmergeする
            df = pandas.concat([chart_df,df],axis=1)
            df['EarningsPerShareX16'] = df['EarningsPerShareX16'].bfill()
            df['EarningsPerShareX15'] = df['EarningsPerShareX15'].bfill()
            df['EarningsPerShareX14'] = df['EarningsPerShareX14'].bfill()
            adp = [
                mpf.make_addplot(df[["EarningsPerShareX16", "EarningsPerShareX15", "EarningsPerShareX14"]], type='line',panel=0)
            ]
        #平均PERから割安/割高分岐線を描画
        elif plot =='averagePER':
            df['Timestamp'] = pandas.to_datetime(df['DisclosedDate'])
            df.set_index('Timestamp', inplace = True)
            # 一株当たりの純利益/四半期
            df['QuotaPeriod'] = ((df['CurrentPeriodEndDate']-df['CurrentPeriodStartDate'])/(df['CurrentFiscalYearEndDate']-df['CurrentFiscalYearStartDate']))*4
            df['EarningsPerSharePerYear'] = df['EarningsPerShare'].where(df['QuotaPeriod']==4,numpy.nan)
            df.at[df.index[-1], 'EarningsPerSharePerYear'] = df.at[df.index[-1],'EarningsPerShare']*4/df.at[df.index[-1],'QuotaPeriod']
            print(df.at[df.index[-1],'EarningsPerShare'])
            print(df.at[df.index[-1],'EarningsPerShare']*4/df.at[df.index[-1],'QuotaPeriod'])
            print(df.loc[:,['QuotaPeriod','EarningsPerShare','EarningsPerSharePerYear']] )

            #chartとmergeする
            df = pandas.concat([chart_df,df],axis=1)
            df['EarningsPerSharePerYear'] = df['EarningsPerSharePerYear'].bfill()

            #平均PERを求める
            df['PER'] = df['Close']/df['EarningsPerSharePerYear']
            AveragePER = df['PER'].mean()

            #平均PERとEPSを掛けて、割安/割高分岐線を描く
            df['Break-evenPoint'] = AveragePER*df['EarningsPerSharePerYear']
            print(df.columns)
            print(df)
            adp = [
                mpf.make_addplot(df[["Break-evenPoint"]], type='line',panel=0)
            ]
        elif plot == "ROE":
            df['Timestamp'] = pandas.to_datetime(df['DisclosedDate'])
            df.set_index('Timestamp', inplace = True)
            #予想純利益
            df['ForecastTotalProfit'] = df['Profit']/((df['CurrentPeriodEndDate']-df['CurrentPeriodStartDate'])/(df['CurrentFiscalYearEndDate']-df['CurrentFiscalYearStartDate']))
            #自己資本
            df["EquityToAsset"] = df['Equity']*df["EquityToAssetRatio"]
            df["ROE"] = df['ForecastTotalProfit']/df["EquityToAsset"]
            print(df.loc[:,["ROE","Profit","ForecastTotalProfit","Equity","EquityToAssetRatio"]])

            #chartとmergeする
            df = pandas.concat([chart_df,df],axis=1)
            df['ROE'] = df['ROE'].bfill()

            #価格*ROE比率
            df["ROEratio"] = df["Close"]*df["ROE"]
            #過去平均ROE倍率
            averageROEratio = df["ROEratio"].mean()
            #割安割高ライン
            df['Break-evenPoint'] = averageROEratio/df["ROE"]
            adp = [
                mpf.make_addplot(df[["Break-evenPoint"]], type='line',panel=0)
            ]
        return df,adp

    # 決算情報
    #一株当たりの純利益
    def get_EarningsPerShare(self):
        return self.get_fins_statements().loc[:,['CurrentPeriodEndDate','EarningsPerShare']].copy()

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
            for column in ['CurrentPeriodStartDate','CurrentPeriodEndDate','CurrentFiscalYearStartDate','CurrentFiscalYearEndDate']:
                df[column] = pandas.to_datetime(df[column])
            self._fins_statements = df
        return self._fins_statements.copy()

    def get_latest_fins_statements(self):
        df = self.get_fins_statements()
        return df[df['TypeOfCurrentPeriod'] == 'FY'].tail(1).reset_index(drop=True)

    def get_latests_data(self):
        df_fs = self.get_latest_fins_statements()
        if len(df_fs) == 0:
            df_price = self.get_latest_prices()
        else:
            #発表日の翌営業日の取引記録
            start = df_fs.at[df_fs.index[0],"DisclosedDate"]
            end = start + datetime.timedelta(weeks=1)
            try:
                df_price = self.get_prices(start=start,end=end).head(2).tail(1).reset_index(drop=True)
            except:
                df_price = self.get_latest_prices()
        df = pandas.concat([df_fs,df_price],axis=1)
        df['Code'] = self.get_code()
        return df

    def _get_yyyymmdd(self,date:datetime.date):
        return f'{date.year:04}{date.month:02}{date.day:02}'