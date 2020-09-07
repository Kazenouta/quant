from src.utils.funcs import *

def ts_stk_daily(symbol, s_date, e_date):
    '''从tushare获取股票日线数据'''
    pro = DataSource.ts_api()

    if type(symbol) == list:
        symbol = ','.join(symbol)

    df = pro.daily(
        ts_code=symbol, 
        start_date=s_date, 
        end_date=e_date)
    
    return df