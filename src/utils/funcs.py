import tushare as ts
from datetime import datetime, timedelta
import pandas as pd
from src.conf.config import *

def ts_api():
    pro = ts.pro_api('703fd07f16a5c9e171961ad1a980d8b90793243b78b1ba6b0d92791d')
    
    return pro


class HandleDate:
    '''时间相关'''

    @staticmethod
    def now():
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def time_ago(_time, value, unit, _format=None):
        '''
        unit in ['Y', 'M', 'D', 'H', 'm', 'S']
        '''
        _time = pd.to_datetime(_time)

        if unit in ['Y', 'year']:
            _time =  _time - timedelta2(years=int(value))
        elif unit in ['M', 'month']:
            _time =  _time - timedelta2(months=int(value))
        elif unit in ['D', 'day']:
            _time =  _time - timedelta2(days=int(value))
        elif unit in ['H', 'hour']:
            _time =  _time - timedelta2(hours=int(value))
        elif unit in ['m', 'min', 'minute']:
            _time =  _time - timedelta2(minutes=int(value))
        elif unit in ['S', 'sec', 'second']:
            _time =  _time - timedelta2(seconds=int(value))
        else:
            raise ValueError("Invalid input of unit: ", unit)

        if _format == None:
            return _time
        else:
            return _time.strftime(_format)

    @staticmethod
    def monthsago(from_date, months):
        from_date = pd.to_datetime(from_date)
        return from_date - timedelta2(months=int(months))

    @staticmethod
    def date_range(s_date, e_date, freq='M', date_format='%Y%m'):
        '''获取一段时间内的日期列表'''
        if freq == 'M':
            # 频率为月度时, 向后推迟一个月, 否则有可能取不到最近的月份数据
            e_date = pd.to_datetime(e_date).strftime('%Y-%m')
            e_date = HandleDate.monthsago(e_date, -1)

        date_ls = pd.date_range(s_date, e_date, freq=freq)
        date_ls = [date.strftime(date_format) for date in date_ls]

        return date_ls
    
    @staticmethod
    def dt_floor(time_series, freq):
        '''
        AS/YS	  year start frequency
        A/Y	      year end frequency
        QS	      quarter start frequency
        Q	      quarter end frequency
        MS	      month start frequency
        M	      month end frequency
        SMS	      semi-month start frequency (1st and 15th)
        SM	      semi-month end frequency (15th and end of month)
        W	      weekly frequency
        D         daily frequency
        H	      hourly frequency
        T/min	  minutely frequency
        S	      secondly frequency
        L/ms	  milliseconds
        U/us	  microseconds
        N	      nanoseconds
        '''
        if freq == '1sec':
            return time_series.dt.floor('S')
        elif freq == '1min':
            return time_series.dt.floor('T')
        elif freq == '1day':
            return time_series.dt.floor('D')

    @staticmethod
    def format_date(dt):
        '''日期格式化'''
        if pd.isna(dt):
            return None
        elif dt == '0000-00-00' or dt > '2200-12-31':
            return pd.to_datetime('2200-12-31')
            
        return pd.to_datetime(dt)
        
    @staticmethod
    def cols_to_datetime(df, columns):
        '''批量将列转为时间格式'''
        for col in columns:
            df[col] = pd.to_datetime(df[col].astype('strs'))

        return df

    @staticmethod
    def date_format(date, date_type='%Y-%m-%d'):
        '''将日期格式化为date_type格式'''
        return pd.to_datetime(str(date)).strftime(date_type)

    @staticmethod
    def daysago(from_date, days, _format='%Y%m%d'):
        from_date = pd.to_datetime(from_date)
        to_date = from_date - timedelta2(days=int(days))
        to_date = to_date.strftime(_format)
        
        return to_date

    @staticmethod
    def cal_unix(dt, dt_format='%Y-%m-%d %H:%M:%S.%f'):   
        '''计算指定日期的UNIX时间戳, 可以使用精确到微秒的日期计算 UNIX 时间戳, 将微秒添加到时间戳的最后
        :params:    dt          要进行转换日期数据
        :params:    dt_formt    解析日期的格式, 用于解析时间字符串
        :return:    unix_str    UNIX 时间戳字符串
        '''

        unix_str =  str(int(time.mktime(datetime.strptime(
            dt, dt_format).timetuple())))
            
        return unix_str

    @staticmethod
    def to_timestamp(time_str, format='%Y-%m-%d %H:%M:%S'):
        timestamp = time.mktime(time.strptime(time_str, format))

        return timestamp

    @staticmethod
    def get_trading_dates_p1(s_date=19000101, e_date=22001231, period="w"):
        """
        获取每周/每月/每季度/每年的第一个交易日序列
        :param s_date: 开始日期
        :param e_date: 结束日期
        :param period: 周期: w/m/q/y
        :return:
        """
        period_no = None
        new_dates = []
        for date_ in HandleDB.get_trading_dates(s_date, e_date):
            date_ = pd.to_datetime(date_)
            if period == "w":
                tmp = date_.week
            elif period == "m":
                tmp = date_.month
            elif period == "q":
                tmp = date_.quarter
            elif period == "y":
                tmp = date_.year
            else:
                raise ValueError("Not a valid period: %e" % period)
            if tmp != period_no:
                new_dates.append(date_)
                period_no = tmp
            date_ += timedelta(days=1)
        return pd.Series(new_dates)


class HandleString:
    '''字符串相关'''
    @staticmethod
    def re_find(pattern, string):
        '''正则查找
        pattern in ['int', 'str']
        '''
        if pattern == 'int':
            return int(''.join(re.findall(r'\d', string)))
        else:
            return ''.join(re.findall('[\u4e00-\u9fa5_a-zA-Z]', '123cv'))

    @staticmethod
    def change_suffix(symbol):
        '''修改symbol后缀'''
        if symbol[-4:] == 'XSHE':
            return symbol[:-4] + 'SZ'
        elif symbol[-4:] == 'XSHG':
            return symbol[:-4] + 'SH'
        else:
            return symbol
        
    @staticmethod
    def add_exchange(symbol):
        codeDict = {
            '600': 'SH',
            '601': 'SH',
            '603': 'SH',
            '688': 'SH',
            '000': 'SZ',
            '001': 'SZ',
            '002': 'SZ',
            '300': 'SZ'}
        exchange = codeDict[str(symbol)[:3]]
        symbol = f'{symbol}.{exchange}'
        
        return symbol

    @staticmethod
    def get_exchange(code):
        # stock code rules
        codeDict = {
            '600': 'SH',
            '601': 'SH',
            '603': 'SH',
            '688': 'SH',
            '000': 'SZ',
            '001': 'SZ',
            '002': 'SZ',
            '300': 'SZ'
            # "sh": ['600', '601', '603'],
            # "sz": ['000', '002', '300']
        }
        if len(code) != 6:
            raise ValueError("Code length is not 6. ")
        try:
            return codeDict[code[:3]]
        except KeyError as e:
            if code in ["T00018"]:
                return "SH"
            raise ValueError("Key error: %s, when code is %s" % (str(e), code))

    @staticmethod
    def get_new_symbol(symbol):
        code = symbol[:6]
        suffix = HandleString.get_exchange(code)
        return '{}.{}'.format(code, suffix)

    @staticmethod
    def invert_dict(d):
        return dict([(v,k) for (k,v) in d.items()])


class HandleDB:
    '''操作数据库'''
    @staticmethod
    def truncate_table(tb_name, engine):
        sql = 'truncate table {}'.format(tb_name)
        engine.execute(sql)
        engine.dispose()

    @staticmethod
    def del_table(tb_name, engine):
        sql = 'delete from {}'.format(tb_name)
        engine.execute(sql)
        engine.dispose()

    @staticmethod
    def del_date(tb_name, engine, t_date, flag='curr_after'):
        t_date = HandleDate.date_format(t_date, '%Y%m%d')
        if flag == 'curr_after':   # 删除指定日期及其后的数据
            sql = "delete from {} where t_date>=to_date('{}','yyyymmdd')".format(tb_name, t_date)
        elif flag == 'curr':       # 删除指定日期数据
            sql = "delete from {} where t_date=to_date('{}','yyyymmdd')".format(tb_name, t_date)
            print(sql)
        else:
            raise ValueError('flag:{} error !!!'.format(flag))
        print(sql)
        engine.execute(sql)
        engine.dispose()

    @staticmethod
    def to_sql(df, tb_name, engine=ENGINE, if_exists='append'):
        df.to_sql(
            tb_name,
            con = engine,
            if_exists = if_exists,
            index = False,
            chunksize = 2000000)

    @staticmethod
    def read_sql(sql, engine):
        df = pd.read_sql(sql,engine)
        engine.dispose()
        return df

    @staticmethod
    def is_trading_date(date, engine=ENGINE):
        date = pd.to_datetime(date).strftime('%Y%m%d')
        sql = "select count(*) c from b_cal_trading_dates where t_date=to_date('{}','yyyymmdd')".format(date)
        df = pd.read_sql(sql, con=engine)

        return df.c[0] == 1

    @staticmethod
    def get_tables(engine):
        '''获取指定数据库中的表列表'''
        sql = 'select table_name from user_tables order by table_name'

        return pd.read_sql(sql, engine).table_name.to_list()