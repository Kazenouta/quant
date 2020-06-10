from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
import os, pymysql

ENGINE = create_engine(
    'mysql+pymysql://quant:Qt2718281828.@101.200.177.119:3306/futures?charset=utf8mb4', encoding = 'utf8') 