from src.data.futures.tushare import *
from src.conf.config import *
import pandas as pd 

def main():
    data = get_data()
    print(data)


if __name__ == '__main__':
    main()