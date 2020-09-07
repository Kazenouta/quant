from src.backtest.pyalgotrade.strategy import *


# Load the yahoo feed from the CSV file
feed = yahoofeed.Feed()
feed.addBarsFromCSV('orcl', '/data/stock/stk_bar_1day/pat_000001.csv')

# Evaluate the strategy with the feed's bars.
myStrategy = RSI2(feed, "orcl", 100, 5, 2, 91, 20)
myStrategy.run()
print("Final portfolio value: $%.2f" % myStrategy.getBroker().getEquity())