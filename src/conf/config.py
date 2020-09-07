from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
import os, pymysql


# 全局路径
PATH_STOCK = '/data/stock'

# 数据库
ENGINE_FUTURES = create_engine(
    'mysql+pymysql://quant:Qt2718281828.@101.200.177.119:3306/futures?charset=utf8mb4', encoding = 'utf8') 

ENGINE_BLOCKCHAIN = create_engine(
    'mysql+pymysql://quant:Qt2718281828.@101.200.177.119:3306/blockchain?charset=utf8mb4', encoding = 'utf8') 

ENGINE_RUNTIME = ENGINE_BLOCKCHAIN

# 可道云根目录
KOD_HOME = '/opt/lampp/htdocs/kodexplorer/data/User/admin/home'