from src.data.stock.tushare import *

def get_data():
    df = ts_stk_daily('000001.SZ', '20190101', '20200101')
    df = df[['trade_date', 'open', 'high', 'low', 'close', 'vol']]
    df = df.rename(columns={
        'trade_date': 'Date',
        'open': 'Open', 
        'high': 'High',
        'low': 'Low',
        'close': 'Close',
        'vol': 'Volume'})
    df['Adj Close'] = df.Close
    df.Date = pd.to_datetime(df.Date)

    csv_path = '/data/stock/stk_bar_1day/pat_000001.csv'
    df.to_csv(csv_path, index=False)