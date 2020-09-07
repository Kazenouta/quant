'''backtest
start: 2015-02-22 00:00:00
end: 2019-10-17 00:00:00
period: 1h
exchanges: [{"eid":"Futures_CTP","currency":"FUTURES"}]
'''

mp = 0  # 全局变量, 用于控制虚拟仓位

# macd 参数
long = 26
short = 12 
signal = 9


# 判断是否上升
def is_up(arr):
    if arr[-1] > arr[-2] and arr[-2] > arr[-3]:
        return True

# 判读是否下降
def is_down(arr):
    arr_len = len(arr)
    if arr[-1] < arr[-2] and arr[-2] < arr[-3]:
        return True

# 判断两个数组是否金叉
def is_up_cross(arr1, arr2):
    if arr1[-2] < arr2[-2] and arr1[-1] > arr2[-1]:
        return True

# 判断两个数组是否死叉
def is_down_cross(arr1, arr2):
    if arr1[-2] > arr2[-2] and arr1[-1] < arr2[-1]:
        return True

# 程序主函数
def onTick():
    exchange.SetContractType("rb000")
    bar_arr = exchange.GetRecords()
    if len(bar_arr) <  long + signal + 1:
        return
    all_macd = TA.MACD(bar_arr, short, long, signal)
    dif = all_macd[0]
    dif.pop()  # 删除最后一个元素
    dea = all_macd[1]
    dea.pop()
    last_close = bar_arr[-1]['Close']

    global mp
    # 开多
    if mp == 0 and dif[-1] > 0:
        exchange.SetDirection('buy')
        exchange.Buy(last_close, 1)
        mp = 1
    # 开空
    if mp == 0 and dif[-1] < 0:
        exchange.SetDirection('sell')
        exchange.Sell(last_close - 1, 1)
        mp = -1
    # 平多单
    if mp == 1 and is_down_cross(dif, dea):
        exchange.SetDirection('closebuy')
        exchange.Sell(last_close - 1, 1)
        mp = 0
    # 平空单
    if mp == -1 and is_up_cross(dif, dea):
        exchange.SetDirection('closesell')
        exchange.Buy(last_close, 1)
        mp = 0

def main():
    while True:
        onTick()
        Sleep(1000)
    