import multiprocessing
import numpy as np
import pandas as pd
# import numba as nb
from sqlalchemy import *
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta as timedelta2
from loguru import logger
from pandas.util.testing import assert_frame_equal
from pandas.tseries.offsets import (Hour as pd_hour, Minute as pd_minute, Second as pd_second)
import subprocess, os
import functools, io
from contextlib import redirect_stdout

class Logger:
    '''日志记录'''
    @staticmethod
    def log_path(log_name):
        '''获取日志路径'''
        file_path = os.path.join('/home/bxz/projects/quant/test/log', log_name + '_{time:YYYYMMDD}.log')

        return file_path

    @staticmethod
    def add(log_name, retention='10 days', rotation='1 day'):
        '''创建日志文件, 每天创建一个日志文件, 最多停留10天
        --------------
        1. logger.add('runtime_{time}.log', rotation="500 MB")   每 500MB 存储一个文件
        2. logger.add('runtime_{time}.log', rotation='00:00')    每天 0 点新创建一个 log 文件
        3. logger.add('runtime_{time}.log', rotation='1 week')   每隔一周创建一个 log 文件
        4. logger.add('runtime.log', retention='10 days')        日志文件最长保留 10 天
        '''
        log_path = Logger.log_path(log_name)
        log = logger.add(sink=log_path, format="{time} {level} {message}", retention=retention, rotation=rotation)
        return log

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
    def multi_processing(tasks, args, process_num=15):
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

def detail_log(log_name):
    log0 = Logger.add('main')
    log = Logger.add(log_name)
    logger.info(log_name)
    logger.remove(log)
    logger.remove(log0)
    return 

def main():
    task_ls = ['task1', 'task2', 'task3', 'task4', 'task5', 'task6', 'task7', 'task8', 'task9', 'task10']
    # log = Logger.add('main')
    HandleSYS.multi_processing(detail_log, task_ls, 10)
    # logger.remove(log)

if __name__ == '__main__':
    main()