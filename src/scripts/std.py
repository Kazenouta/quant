from fmz import *
import datetime
import pandas as pd
import time
import numpy as np
import datetime 
# 在botvs里 time.time()可以获取到回测的时间，而datetime.datetime.now()只能获取

# def get_trade_signal():
#     '''
#     @description: dual thrust生成开仓、平仓信号
#     @return: open_flag, close_flag
#     '''
#     print('get_trade_signal')
#     pass

class StdStg:
    @staticmethod
    def get_trade_signal():
        pass

    @staticmethod
    def get_tar_pos(open_flag, close_flag, pre_pos):
        '''
        @description: 根据开平仓信号以及前一个仓位状态，生成新的目标仓位
        @param {type} 
        @return: 
        '''
        amount = 10 # 目标仓位大小，这里写成定值
        # Log(open_flag, close_flag)
        if open_flag == 1:
            # 开多仓
            tar_pos = amount
        elif open_flag == -1:
            # 开空仓
            tar_pos = -amount
        else:
            if close_flag == 1:
                # 平空仓
                tar_pos = max(pre_pos, 0)
            elif close_flag == -1:
                # 平多仓
                tar_pos = min(pre_pos, 0)
            else:
                # 不变
                tar_pos = pre_pos
        return tar_pos

    @staticmethod
    def execute(tar_pos):
        '''
        @description: 这个执行程序还是一个雏形，目前只判断是否持仓，没有判断仓位是否等于目标仓位
        @param {type} 
        @return: 
        '''
        position_list = exchange.GetPosition()
        #Log(position)
        if len(position_list) == 0:
            # 如果空仓，根据tar pos开仓
            if tar_pos > 0:
                exchange.SetDirection("buy")
                exchange.Buy(g_tick.Sell+1, tar_pos)
                Log(exchange.GetPosition())
                Log(exchange.GetAccount())
            elif tar_pos < 0:
                exchange.SetDirection("sell")
                exchange.Sell(g_tick.Buy-1, -tar_pos)
                Log(exchange.GetPosition())
                Log(exchange.GetAccount())
        else:
            # 如果非空仓，根据tar pos平仓
            pos_long = 0
            pos_short = 0
            for pos in position_list:
                if pos["Type"] == 0:
                    pos_long += pos["Amount"]
                if pos["Type"] == 1:
                    pos_short += pos["Amount"]
            if pos_long != 0 and tar_pos <= 0:
                exchange.SetDirection("closebuy")
                exchange.Sell(g_tick.Buy-1, -tar_pos)
                Log(exchange.GetPosition())
                Log(exchange.GetAccount())
            elif pos_short != 0 and tar_pos >= 0:
                exchange.SetDirection("closesell")
                exchange.Buy(g_tick.Sell+1, tar_pos)
                Log(exchange.GetPosition())
                Log(exchange.GetAccount())
        return 

    @staticmethod
    def loop():
        '''
        @description: 主循环，循环内流程：生成信号->生成目标仓位->根据目标仓位执行交易
        @param {type} 
        @return: 
        '''
        global g_tick
        tar_pos = 0
        signal_date = None 
        while 1:
            g_tick = exchange.GetTicker()   
            # 为了平滑结果，只使用每日第一个信号
            now_time = datetime.datetime.fromtimestamp(time.time())
            if now_time.date() != signal_date:
                # 生成开平仓信号
                open_flag, close_flag = StdStg.get_trade_signal()
                signal_date = now_time.date()
            # 生成目标仓位
            curr_pos = tar_pos
            tar_pos = StdStg.get_tar_pos(open_flag, close_flag, curr_pos)
            if tar_pos == curr_pos:
                continue

            # 根据目标仓位执行交易
            StdStg.execute(tar_pos)
            Sleep(60*1000*1)

    @staticmethod
    def main(task):
        task = VCtx('''backtest
            start: 2018-01-01 00:00:00
            end: 2018-03-30 00:00:00
            period: 1h
            basePeriod: 15m
            exchanges: [{"eid":"Futures_OKCoin","currency":"BTC_USD"}]
        ''')

        exchange.SetContractType("this_week")
        Log(exchange.GetAccount())
        StdStg.loop()
