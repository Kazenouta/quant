from src.utils.funcs import *


def get_codes(exchange='CZCE', fut_type='2'):
    '''获取期货代码
    fut_type    0     全部
                1     普通合约
                2     主力合约&连续合约
    '''
    pro = ts_api()
    if fut_type == '0':
        fut_type=None, 
        fields='ts_code,symbol,name,list_date,delist_date'
    elif fut_type == '1':
        fields='ts_code,symbol,name,list_date,delist_date'
    elif fut_type == '2':
        fields='ts_code,symbol,name'

    df = pro.fut_basic(
        exchange=exchange, 
        fut_type=fut_type, 
        fields=fields)

    return df

def get_data(fut_code='UR.ZCE', s_date='20190809', e_date='20191231'):
    pro = ts_api()
    df = pro.fut_daily(
        ts_code=fut_code, 
        start_date=s_date, 
        end_date=e_date)

    HandleDB.to_sql(df, 'czce_url')