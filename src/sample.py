from datetime import datetime
from dateutil import tz
import jquantsapi

cli = jquantsapi.Client()

#銘柄一覧
brand = cli.get_listed_info()
print(brand[brand['Code'] == '67580'])

#チャート(取得可能範囲は3年～最新の3か月遅れ?)
#全銘柄
price = cli.get_price_range(
    start_dt=datetime(2022, 12, 1, tzinfo=tz.gettz("Asia/Tokyo")),
    end_dt=datetime(2022, 12, 2, tzinfo=tz.gettz("Asia/Tokyo")),
)
print(price[price['Code'] == '67580'])
#個別銘柄
daily=cli.get_prices_daily_quotes(code='67580',from_yyyymmdd='20211201',to_yyyymmdd='20231201')
print(daily)

#財務情報(1Q遅れ？)
financial = cli.get_fins_statements(code='67580')
print(financial.loc[:,['CurrentPeriodStartDate','CurrentPeriodEndDate','EarningsPerShare','ResultDividendPerShareAnnual','ResultPayoutRatioAnnual']])

#決算発表スケジュール
schedule = cli.get_fins_announcement()
print(schedule[schedule['Code'] == '67580'])