import talib
import numpy as np

mp = 0

def get_close(r):
    arr = []
    for i in r:
        arr.append(i['Close'])
    return arr

def is_up(arr):
    if arr[-1] > arr[-2] and arr[-2] > arr[-3]:
        return True

def is_down(arr):
    if arr[-1] < arr[-2] and arr[-2] < arr[-3]:
        return True

def is_up_cross(arr1, arr2):
    if arr1[-2] < arr2[-2] and arr1[-1] > arr2[-1]:
        return True

def is_down_cross(arr1, arr2):
    if arr1[-2] > arr2[-2] and arr1[-1] < arr2[-1]:
        return True

def onTick():
    ama_short = 2
    ama_long = 30
    exchange.SetContractType('rb000')
    bar_arr = exchange.GetRecords()
    if len(bar_arr) < ama_long:
        return
    close_arr = get_close(bar_arr)
    np_close_arr = np.array(close_arr)
    ama1 = talib.KAMA(np_close_arr, ama_short).tolist()
    ama2 = talib.KAMA(np_close_arr, ama_long).tolist()
    last_close = close_arr[-1]

    global mp
    # open long
    if mp == 0 and is_up_cross(ama1, ama2) and is_up(ama2):
        exchange.SetDirection('buy') 
        exchange.Buy(last_close, 1)
        mp = 1
    # open short
    if mp == 0 and is_down_cross(ama1, ama2) and is_down(ama2):
        exchange.SetDirection('sell')
        exchange.Sell(last_close-1, 1)
        mp = -1
    # close long
    if mp == 1 and (is_down_cross(ama1, ama2) or is_down(ama1)):
        exchange.SetDirection('closebuy')
        exchange.Sell(last_close,-1, 1)
        mp = 0
    # close short
    if mp == -1 and (is_up_cross(ama1, ama2) or is_up(ama1)):
        exchange.SetDirection('closesell')
        exchange.Buy(last_close, 1)
        mp = 0

def main():
    while True:
        onTick()
        sleep(1000)