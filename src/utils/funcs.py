import tushare as ts
from datetime import datetime, timedelta
import pandas as pd
from src.conf.config import *
import requests, re
from bs4 import BeautifulSoup

class DataSource:
    @staticmethod
    def ts_api():
        pro = ts.pro_api('703fd07f16a5c9e171961ad1a980d8b90793243b78b1ba6b0d92791d')
        
        return pro
 
class HandleSpider:
    
    
    @staticmethod
    def get_html(url,headers=None,data=None):
        if headers == True:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.81 Safari/537.36'
            } 
        html = requests.post(url,headers=headers,data=data).text
        
        return html

    @staticmethod
    def get_soup(url,headers=None,data=None):
        if headers == True:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.81 Safari/537.36'
            } 
        html = requests.post(url,headers=headers,data=data).text
        soup = BeautifulSoup(html,'lxml')
        
        return soup

class HandleSYS:
    '''系统/进程相关'''
    @staticmethod
    def get_attrs(obj):
        '''获取指定对象的属性, 对象可以是变量/数组/函数/类'''
        return dir(obj)

    @staticmethod
    def get_func_name(func):
        '''获取指定函数的函数名'''
        return  func.__name__

    @staticmethod
    def get_stdout(func, *args, **kw):
        '''获取指定函数的标准输出, 结果以列表形式返回'''
        f = io.StringIO()
        with redirect_stdout(f):
            func(*args, **kw)
        out = f.getvalue().split('\n')[:-1]

        return out

    @staticmethod
    def multi_processing(tasks, args, process_num=5):
        '''多进程执行任务
        :params:    tasks          要执行的任务
        :params:    args           任务执行的参数
        :params:    process_num    进程数量
        '''
        pool = multiprocessing.Pool(processes=process_num)
        # 多个任务使用同一参数
        if type(tasks)==list and type(args)!=list:
            for task in tasks:
                pool.apply_async(task, args=(args,))
            pool.close()
            pool.join()
        # 单个任务使用多个参数
        elif type(tasks)!=list and type(args)==list:
            for arg in args:
                pool.apply_async(tasks, args=(arg,))
            pool.close()
            pool.join()
        # 多个任务使用多个参数
        elif type(tasks)==list and type(args)==list:
            for task in tasks:
                for arg in args:
                    pool.apply_async(task, args=(arg,))
            pool.close()
            pool.join()
        # 单个任务使用单个参数
        elif type(tasks)!=list and type(args)!=list:
            task(args)
        else:
            raise ValueError('tasks or args is invalid!')

    @staticmethod
    def pwd():
        '''获取当前工作目录'''
        return subprocess.getoutput('pwd')

    @staticmethod
    def get_ip():
        """
        查询本机ip地址
        :return:
        """
        try:
            s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
            s.connect(('8.8.8.8',80))
            ip = s.getsockname()[0]
        finally:
            s.close()

        return ip

    @staticmethod
    def get_pid_from_task(task, user='ark'):
        '''获取指定任务的pid'''
        task = f'[{task[0]}]{task[1:]}'
        cmd = f"ps -ef | grep {user} | grep '{task}'"
        cmd += " | awk '{print $2}'"
        print(cmd)
        pid = subprocess.getoutput(cmd).split('\n')[-1]

        return pid

    @staticmethod
    def get_pid_from_port(port, user='bxz'):
        cmd = f'lsof -i:{port} | grep {user}'
        cmd += " | awk '{print $2}'"
        print(cmd)
        pid = subprocess.getoutput(cmd).split('\n')[0]

        return pid

    @staticmethod
    def kill_task(task, user='ark'):
        pid = HandleSYS.get_pid_from_task(task, user)
        if pid != '':
            os.system(f'kill {pid}')
            print(f'{pid} killed!')

    @staticmethod
    def kill_port(port, user='bxz'):
        pid = HandleSYS.get_pid_from_port(port, user)
        if pid != '':
            os.system(f'kill {pid}')
            print(f'{pid} killed!')


class HandleFile:
    '''文件相关'''
    @staticmethod
    def join_files(files_or_dir, dist_file, with_columns=True, dtype=None):
        '''批量合并CSV文件'''
        # 要合并的文件列表
        if os.path.isdir(files_or_dir):  # 目录
            file_ls = sorted(HandleFile.ls_dir(files_or_dir))
            file_dir = files_or_dir
        elif type(files_or_dir) == list:
            file_ls = sorted(files_or_dir)
            file_dir = os.path.split(file_ls[0])[0]
        else:
            raise Exception('args is invalid!')
        # 合并后的文件路径
        if '/' in dist_file:  # 输入的是文件路径
            pass
        else:   # 输入文件名时默认保存到要合并文件所在的目录下
            dist_file = os.path.join(file_dir, dist_file)
            print(dist_file)
        # 文件合并
        if with_columns == True:   # 文件包含列名, 合并时需要去掉除第一个文件之外的所有文件列名
            for file in file_ls:
                if HandleFile.is_empty(file):
                    continue
                df = pd.read_csv(file, dtype=dtype)
                HandleFile.csv_append(dist_file, df)
        else:   # 文件不含列名时, 可以直接使用Linux自带命令处理
            file_ls = ' '.join(file_ls)
            os.system('cat {} >> {}'.format(file_ls, dist_file))



    @staticmethod
    def uncomp_file(comp_file_path, to_dir, comp_type):
        '''解压文件
        rar
        -------
            unrar e test.rar 解压文件到当前目录
            unrar x test.rar /path/to/extract
            unrar l test.rar 查看rar中的文件
            unrar v test.rar 更详细
            unrar t test.rar 测试是否可以成功解压
        '''
        if comp_type == 'rar':
            cmd = f'unrar x {comp_file_path} {to_dir}'
        elif comp_type == 'zip':
            cmd = f'unzip -rqj {comp_file_path} {to_dir}'

        os.system(cmd)

    @staticmethod
    def cat(file_ls, cat_file):
        '''文件合并'''
        file_ls = ' '.join(file_ls)
        # 合并
        os.system('cat {} >> {}'.format(file_ls, cat_file))

    @staticmethod
    def zip(file_dir, zip_file):
        '''压缩文件夹'''
        os.system('zip -rqj {} {}'.format(zip_file, file_dir))

    @staticmethod
    def csv_append(path, data):
        '''向CSV文件添加内容'''
        if len(data) == 0:
            return 
        # data['create_date'] = datetime.now()
        # 如果文件已经存在且不为空, 直接写入数据, 不要header和index
        if os.path.exists(path) and os.path.getsize(path)!=0: 
            data.to_csv(path, mode='a', index=False, header=False, encoding='utf8')
        else:
            data.to_csv(path, mode='w', index=False, header=True, encoding='utf8')

    @staticmethod
    def csv_write(path, data):
        '''向CSV文件添加内容'''
        data['create_date'] = datetime.now()
        data.to_csv(path, mode='w', index=False, encoding='utf8')
       
    @staticmethod
    def read_dir(dir_path, file_kw):
        '''获取指定目录下包含某个字符串的所有文件的数据'''
        file_ls = os.listdir(dir_path)
        file_ls = [
            os.path.join(dir_path, file) 
            for file in file_ls 
            if file_kw in file
        ]
        df_ls = []
        for file in file_ls:
            if HandleFile.file_size(file) == 0:
                continue
            data = pd.read_csv(file)
            df_ls.append(data)
        return pd.concat(df_ls, sort=False)

    @staticmethod
    def touch_file(file_path):
        '''创建一个空文件'''
        if not os.path.exists(file_path): 
            f = open(file_path, 'w')
            f.close()

    @staticmethod
    def make_dirs(dir_path):
        '''创建目录'''
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

    @staticmethod
    def is_exist(file_path):
        return os.path.exists(file_path)

    @staticmethod
    def del_file(file_path):
        if os.path.exists(file_path):
            cmd = f'rm -rf {file_path}'
            os.system(cmd)

    @staticmethod
    def file_size(file_path):
        '''获取文件大小'''
        return os.path.getsize(file_path)

    @staticmethod
    def is_empty(file_path):
        return os.path.getsize(file_path) == 0

    @staticmethod
    def read_csv(file_path, dtype=None):
        if os.path.getsize(file_path) != 0:
            return pd.read_csv(file_path, dtype=dtype)
        else:
            return ''

    @staticmethod
    def read_csv_ls(csv_ls, dtype=None):
        '''批量读取CSV文件'''
        data_ls = []
        for file in csv_ls:
            if os.path.getsize(file) == 0:
                continue
            data = pd.read_csv(file, dtype=dtype)
            data_ls.append(data)
        if data_ls == []:
            return data_ls
        else:
            return pd.concat(data_ls, sort=False)

    # @staticmethod
    # def walk_hdf(hdf_file):
    #     store = pd.HDFStore(hdf_file)
    #     for (path, subgroups, subkeys) in store.walk():
    #         for subgroup in subgroups:
    #             print('GROUP: {}/{}'.format(path, subgroup))
    #         for subkey in subkeys:
    #             key = '/'.join([path, subkey])
    #             print('KEY: {}'.format(key))
    #             print(store.get(key))

    @staticmethod
    def rename(old_path, new_path):
        cmd = f'mv {old_path} {new_path}'
        os.system(cmd)


    @staticmethod
    def ls_tar(tar_file_path):
        '''
        'r' or 'r:*'   Open for reading with transparent compression (recommended).
        'r:'           Open for reading exclusively without compression.
        'r:gz'         Open for reading with gzip compression.
        'r:bz2'        Open for reading with bzip2 compression.
        'a' or 'a:'    Open for appending with no compression. The file is created if it does not exist.
        'w' or 'w:'    Open for uncompressed writing.
        'w:gz'         Open for gzip compressed writing.
        'w:bz2'        Open for bzip2 compressed writing
        '''
        tar = tarfile.open(tar_file_path,'r:*')
        return tar.get_names()

    import os, tarfile

    @staticmethod
    def to_targz(source_dir, output_filename=None):
        """
        将指定文件夹打包压缩为tar.gz文件
        :param source_dir: 需要打包的目录
        :param output_filename: 压缩文件名
        :return: 
        """
        if output_filename == None:
            output_filename = '{}.tar.gz'.format(source_dir)
        try:
            with tarfile.open(output_filename, "w:gz") as tar:
                tar.add(source_dir, arcname=os.path.basename(source_dir))
        except Exception as e:
            print(e) 

    @staticmethod
    def read_targz(targz_file_path, member):
        tar = tarfile.open(targz_file_path)
        obj = tar.getmember(member)
        data = tar.extractfile(obj)
        # 转 DataFrame
        # df = pd.DataFrame(data)
        # df[0] = df[0].map(lambda x: x.decode('utf8'))
        # df = df[0].str.split(',', expand=True)
        # df.columns = df.iloc[0, :]
        # df = df.shift(-1)
        return data.read()

    @staticmethod
    def extract_tar_single(tar_file_path, file_name, save_path):
        with tarfile.open(tar_file_path,'r:*') as tar:
            tar.extract(file_name, save_path)

    @staticmethod
    def extract_tar_all(tar_file_path, save_path):
        with tarfile.open(tar_file_path,'r:*') as tar:
            tar.extractall(save_path)

    @staticmethod
    def read_bcolz(bcolz_file_path):
        f = bcolz.open(bcolz_file_path)
        return f.todataframe()

    @staticmethod
    def to_bcolz(df, bcolz_file_path):
        bcolz.ctable.fromdataframe(df, rootdir=bcolz_file_path)

    @staticmethod
    def ls_bcolz(bcolz_file_path, arg='line_map'):
        f = bcolz.open(bcolz_file_path)
        index = f.attrs[arg]
        return index

    @staticmethod
    def to_hdf(data, hdf_file, key, complevel=1):
        data.to_hdf(hdf_file, key, mode='a', complevel=complevel)

    @staticmethod
    def read_hdf(hdf_file, key):
        return pd.read_hdf(hdf_file, key)

    @staticmethod
    def ls_hdf(hdf_file, group='lv1'):
        f = h5py.File(hdf_file, 'r')
        if group == 'lv1':
            keys = list(f.keys())
        elif group == 'lv2':
            keys = []
            lv1_keys = list(f.keys())
            for lv1_key in lv1_keys:
                lv2_keys = list(f[lv1_key].keys())
                lv2_keys = [f'{lv1_key}/{lv2_key}' for lv2_key in lv2_keys]
                keys += lv2_keys
        else:
            try:
                keys = list(f[group].keys())
            except Exception as e:
                raise ValueError('{} not exist'.format(group))
        f.close()
        
        return keys

    @staticmethod
    def del_hdf_key(hdf_file, key):
        with h5py.File(hdf_file,  "a") as f:
            del f[key]

    @staticmethod
    def read_rar(rar_file_path, csv_file_name, header=None, dtype=None):
        '''读取rar压缩文件夹中的某个csv文件'''
        rar = rarfile.RarFile(rar_file_path)
        data = rar.open(csv_file_name)
        df = pd.read_csv(data, header=header, dtype=dtype)
        if type(df) is pd.DataFrame:
            return df
        else:
            raise ValueError('Can not get DataFrame from {} in {}'.format(csv_file_name, rar_file_path))

    @staticmethod
    def ls_rar(rar_file_path):
        '''获取rar压缩文件夹dir'''
        rar = rarfile.RarFile(rar_file_path)
        rar_dir = rar.namelist()

        return rar_dir

    @staticmethod
    def to_rar(to_rar_files, rar_file_path):
        if type(to_rar_files) == list:
            to_rar_files = ' '.join(to_rar_files)
        cmd = 'rar a {rar_file_path} {to_rar_files}'
        os.system(cmd)

    @staticmethod
    def read_zip(zip_file_path, csv_file_name, header=None, dtype=None, low_memory=False):
        '''读取zip压缩文件夹中的某个csv文件'''
        try:
            _zip = zipfile.ZipFile(zip_file_path)
            data = _zip.open(csv_file_name)
            df = pd.read_csv(data, header=header, dtype=dtype, low_memory=low_memory)
            return df
        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def ls_zip(zip_file_path):
        '''获取zip压缩文件夹dir'''
        _zip = zipfile.ZipFile(zip_file_path)
        zip_dir = _zip.namelist()

        return zip_dir

    @staticmethod
    def read_comp(comp_file_path, csv_file_name, comp_type, header=None, dtype=None):
        '''读取压缩文件'''
        if comp_type == 'rar':
            df = read_rar(comp_file_path, csv_file_name, header, dtype)
        elif comp_type == 'zip':
            df = read_zip(comp_file_path, csv_file_name, header, dtype)
        else:
            raise ValueError(f'comp_type: {comp_type} is invalid!')
        
        return df

    @staticmethod
    def ls_comp(comp_file_path, comp_type):
        '''列出压缩文件中的文件名'''
        if comp_type == 'rar':
            ls = ls_rar(comp_file_path)
        elif comp_type == 'zip':
            ls = ls_zip(comp_file_path)
        else:
            raise ValueError(f'comp_type: {comp_type} is invalid!')
        
        return ls

    @staticmethod
    def to_zip(to_zip_file, zip_file, is_python=False):
        '''将指定文件压缩为zip格式'''
        if is_python:
            with zipfile.ZipFile(zip_file,"w",zipfile.ZIP_DEFLATED) as f:
                f.write(to_zip_file)
        elif not is_python:
            zip_file_dir = os.path.split(to_zip_file)[0]
            os.system(f'cd {zip_file_dir}')
            os.system(f'zip -rq {zip_file} {to_zip_file}')

    @staticmethod
    def read_comp(file_path, csv_file_name, comp_type, header=None, dtype=None):
        '''读取压缩文件夹中的某个csv文件'''
        if comp_type == 'rar':
            return HandleFile.read_rar(file_path, csv_file_name, header, dtype)
        elif comp_type == 'zip':
            return HandleFile.read_zip(file_path, csv_file_name, header, dtype)

    @staticmethod
    def ls_comp(file_path, comp_type):
        '''获取zip压缩文件夹dir'''
        if comp_type == 'rar':
            return HandleFile.ls_rar(file_path)
        elif comp_type == 'zip':
            return HandleFile.ls_zip(file_path)

    @staticmethod
    def ls_dir(path):
        '''得到指定目录下所有文件的路径'''
        for params in os.walk(path):
            return [os.path.join(params[0], j) for j in params[2]]


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
    def from_timestamp(timestamp, unit='s', _format=None):
        _time = pd.to_datetime(
            int(timestamp), 
            unit = unit, 
            origin = pd.Timestamp('1970-01-01 08:00:00'))
        if _format != None:
            _time = _time.strftime(_format)

        return _time

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


class HandleDF:
    '''操作 DataFrame'''
    @staticmethod
    def parell_groupby(group_gen, func):
        '''并行分组
        eg: ret = parell_groupby(df.groupby('symbol'), func)
        '''
        from joblib import Parallel, delayed
        import multiprocessing
        retLst = Parallel(n_jobs=multiprocessing.cpu_count())(delayed(func)(group) for name, group in group_gen)
        return pd.concat(retLst)

    @staticmethod
    def time_offset(pd_series, offset_value=1, offset_unit='min'):
        '''
        offset_unit in ['hour', 'min', 'sec']
        '''
        if pd_series.dtype != 'datetime64[ns]':
            pd_series = pd.to_datetime(pd_series)
            
        if offset_unit == 'hour':
            return pd_series + pd_hour(offset_value)
        elif offset_unit == 'min':
            return pd_series + pd_minute(offset_value)
        elif offset_unit == 'sec':
            return pd_series + pd_second(offset_value)
        else:
            raise ValueError(f'offset_unit: {offset_unit} is invalid')

    @staticmethod
    def cols_to_datetime(df, columns):
        '''批量将列转为时间格式'''
        if type(columns) != list:
            raise ValueError('columns: {columns} is not a list')
        elif columns == []:
            return df
        else:
            for col in columns:
                if col in df.columns:
                    df[col] = df[col].astype('str')
                    df[col] = pd.to_datetime(df[col])
                else:
                    raise ValueError(f'column {col} not in df.columns')

        return df

    @staticmethod
    def cnt_col(df, cols):
        '''计算某一列中各元素出现的次数'''
        df_cnt = pd.DataFrame(df.groupby(cols).symbol.count())
        df_cnt.columns = ['cnt']
        df_cnt = df_cnt.reset_index()

        return df_cnt

    @staticmethod
    def minus_series(s1, s2):
        return [i for i in s1 if i not in s2]

    @staticmethod
    def dir2df(dir_path):
        '''将指定目录下的所有CSV文件读出并转为DF'''
        file_name_ls = os.listdir(dir_path)
        file_path_ls = [os.path.join(dir_path, file_name) for file_name in file_name_ls]
        df = pd.concat([
            pd.read_csv(file_path) 
            for file_path in file_path_ls 
            if os.path.getsize(file_path)!=0
        ], sort=False)
        return df

    @staticmethod
    def check_col(df1, df2, start_seq=0):
        for col_seq in range(start_seq, len(df1.columns)):
            df1 = df1.dropna(subset=[df1.columns[col_seq]])
            df2 = df2.dropna(subset=[df2.columns[col_seq]])
            if (len(df1)==0) and (len(df2)==0):
                continue
            check_ls = df1.iloc[:,col_seq]!=df2.iloc[:,col_seq]
            ret1 = df1.loc[check_ls].iloc[:,col_seq]
            ret2 = df2.loc[check_ls].iloc[:,col_seq]
            if (len(ret1)!=0) or (len(ret2)!=0):
                print(col_seq, ' - ', len(ret1))
                break
    
    @staticmethod
    def drop_duplicates(df, cols):
        '''去重'''
        if type(cols) != list:
            raise ValueError('cols: {cols} is not a list')
        elif cols == []:
            return df
        else:
            return df.drop_duplicates(subset=cols)

    @staticmethod
    def drop_na(df, cols):
        '''对传入的列名做去空操作, 去空采用how='all'''
        if type(cols) != list:
            raise ValueError('cols: {cols} is not a list')
        df = df.dropna(how='any', subset=cols)

        return df
        
    @staticmethod
    def fill_na(df, fill_dict):
        '''输入列名与填充值的字典, 获得填充后的DF'''
        if type(fill_dict) != dict:
            raise ValueError('fill_dict: {fill_dict} is not a dict')
        elif fill_dict == {}:
            return df
        else:
            for col, fill_value in fill_dict.items():
                df[col] = df[col].fillna(fill_value)
        
        return df

    @staticmethod
    def drop_col(df, cols):
        '''删除列'''
        if type(cols) == list:
            for col in cols:
                if col in df.columns:
                    df = df.drop(col, axis=1)
        elif type(cols) == str:
            if cols in df.columns:
                df = df.drop(col, axis=1)
        else:
            raise ValueError('cols is invalid')

        return df

    @staticmethod
    def merge(df_ls, how, on):
        '''将多个DF联结'''
        df0 = df_ls[0]
        for df in df_ls[1:]:
            df0 = pd.merge(df0, df, how=how, on=on)

        return df0

    @staticmethod
    def to_timestamp(time_series, unit='s'):
        '''将日期格式数据转为时间戳数据'''
        if time_series.dtype != 'datetime64[ns]':
            time_series = pd.to_datetime(time_series)

        timestamp_series = (
            (time_series.values 
            - np.datetime64('1970-01-01 08:00:00')) 
            / np.timedelta64(1, unit))

        return timestamp_series

    @staticmethod
    def from_timestamp(timestamp_series, unit='s'):
        '''将时间戳数据转为日期格式'''
        time_series = pd.to_datetime(
            timestamp_series, 
            unit = unit, 
            origin = pd.Timestamp('1970-01-01 08:00:00'))

        return time_series


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
    def to_sql(df, tb_name, engine=ENGINE_RUNTIME, if_exists='append'):
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
    def is_trading_date(date, engine=ENGINE_RUNTIME):
        date = pd.to_datetime(date).strftime('%Y%m%d')
        sql = "select count(*) c from b_cal_trading_dates where t_date=to_date('{}','yyyymmdd')".format(date)
        df = pd.read_sql(sql, con=engine)

        return df.c[0] == 1

    @staticmethod
    def get_tables(engine):
        '''获取指定数据库中的表列表'''
        sql = 'select table_name from user_tables order by table_name'

        return pd.read_sql(sql, engine).table_name.to_list()