# fmz@3ca8a004eda30636312386249ff4b1ee

from fmz import *
import pandas as pd
import time, datetime
import numpy as np
 
# 在botvs里 time.time()可以获取到回测的时间，而datetime.datetime.now()只能获取


task = VCtx('''backtest
    start: 2018-01-01 00:00:00
    end: 2018-03-30 00:00:00
    period: 1h
    basePeriod: 15m
    exchanges: [{"eid":"Futures_OKCoin","currency":"BTC_USD"}]
    args: [["days",2],["k1",1],["k2",1]]
''')


def get_trade_signal(n, k1, k2):
    '''
    @description: dual thrust生成开仓、平仓信号
    @param n: 天数
    @param k1: 上轨阈值比例
    @param k2: 下轨阈值比例
    @return: open_flag, close_flag
    '''
    # 获取日k线，日k线倒数第一个是交易当天的，前n个需要获取n+1+1个k线
    bar_list = exchange.GetRecords(PERIOD_D1)[-(n+2):]
    #Log(len(bar_list),bar_list)
    h_list = [v["High"] for v in bar_list[:-2]] # 前n个k线[-n-2~-3]取hh
    l_list = [v["Low"] for v in bar_list[:-2]]
    c_list = [v["Close"] for v in bar_list[:-2]]
    hh = max(h_list)
    hc = max(c_list)
    ll = min(l_list)
    lc = min(c_list)
    # Log(hh, hc, ll ,lc)
    rg = max(hh-lc, hc-ll)
    upper_threshold = bar_list[-2]["Open"] + k1 * rg    # 除去当天以外，倒数第1天
    lower_threshold = bar_list[-2]["Open"] - k2 * rg
    #Log(bar_list)
    #Log(upper_threshold, lower_threshold)
    if g_tick.Last > upper_threshold:
        open_flag = 1   # 开多仓
        close_flag = 1  # 平空仓
        #Log(upper_threshold)
    elif g_tick.Last < lower_threshold:
        open_flag = -1  # 开空仓
        close_flag = -1 # 平空仓
    else:
        open_flag = 0   # 不动
        close_flag = 0  # 不动
    # Log(time.time(), ls_flag, tick.Last, vwap, len(bar_list))
    return open_flag, close_flag
    

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
            open_flag, close_flag = get_trade_signal(days, k1, k2)
            signal_date = now_time.date()
            
        
        # 生成目标仓位
        tar_pos = get_tar_pos(open_flag, close_flag, tar_pos)
        # if tar_pos != 0:
            # Log(tar_pos)

        # 根据目标仓位执行交易
        execute(tar_pos)
        Sleep(60*1000*1)

def main():
    exchange.SetContractType("this_week")
    Log(exchange.GetAccount())
    loop()

# ret = task.Join(True)

# price_buy = 16438.51
# price_sell = 14729
# contract_value = 1000
# margin_level = 10
# margin_buy = contract_value/price_buy/margin_level
# maigin_sell = margin_buy
# profit = (1 - price_buy/price_sell) * margin_buy * margin_level
# commission_buy = (margin_buy * price_buy * margin_level) * 0.0003 / price_buy
# commission_sell = commission_buy
# pos0 = 3
# pos1 = pos0 - margin_buy - commission_buy
# pos2 = pos1 - maigin_sell - commission_sell + profit + (margin_buy + maigin_sell)
# print(pos1, pos2)