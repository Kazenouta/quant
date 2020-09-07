from .cus_cons import *
import numpy as np
import pandas as pd
from datetime import datetime

def get_html(url):
    html = requests.get(url).text
    return html

# 加入代理
def get_html2(url):
    header = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
    ip_pool = ['121.61.3.166：9999','110.52.235.70：808','116.209.57.168：9999','58.55.193.176:9999']
    ip = random.choice(ip_pool)
    proxy = {'http': ip}
    html = requests.get(url,headers=headers,proxies=proxy).text
    return html

def get_soup_by_html(html):
    soup = BeautifulSoup(html,'lxml')
    return soup

def get_soup_by_url(url):
    html = requests.get(url).text
    soup = BeautifulSoup(html,'lxml')
    return soup

# 获取海关月报数据所有url
def get_urls(type):
    # type   0：人民币  1：美元
    url = 'http://www.customs.gov.cn/customs/302249/302274/302277/index.html'
    soup = get_soup_by_url(url)
    if type==0:
        table_ls = [soup.select('table')[i].select('tr')[1:] for i in range(2,11,2)]
    elif type==1:
        table_ls = [soup.select('table')[i].select('tr')[1:] for i in range(3,12,2)]
    url_dict = get_dict2(table_ls)
    return url_dict

# 获取表格数据
def get_table(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit,year_ls=None,step=None):
    url_dict = get_urls(curr_type)
    if year_ls==None:
        year_ls = ['2018','2017','2016','2015','2014']
    df0 = pd.DataFrame(columns=col_names)
    key_ls = list(url_dict.keys())[table_index::17]

    for i in range(len(year_ls)):
        year = year_ls[i]
        key = key_ls[i]
        url_ls = url_dict[key]
        for i in range(len(url_ls)):
            time.sleep(1)
            if i < 9:
                date = year + '.0' + str(i+1)
            else:
                date = year + '.' + str(i+1)
            print(date)
            try:
                url = url_ls[i]
                if url[0] == '/':
                    url = 'http://www.customs.gov.cn' + url
                if step==None:
                    df = pd.read_html(url)[0].iloc[iloc_x1:iloc_x2,iloc_y].rename(columns=col_name_dict)
                else:
                    df = pd.read_html(url)[0].iloc[iloc_x1:iloc_x2:step,iloc_y].rename(columns=col_name_dict)

                df['年月'] = date
                df0 = pd.concat([df0,df],ignore_index=True,sort=True)
            except:
                pass
    df0['货币单位'] = unit
    return df0    

def get_table2(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit,year_ls=None,step=None):
    if step==None:
        step=1

    url_dict = get_urls(curr_type)
    if year_ls==None:
        year_ls = ['2018','2017','2016','2015','2014']
    df0 = pd.DataFrame(columns=col_names)
    key_ls = list(url_dict.keys())[table_index::17]

    for i in range(len(year_ls)):
        year = year_ls[i]
        key = key_ls[i]
        url_ls = url_dict[key]
        for i in range(len(url_ls)):
            time.sleep(1)
            if i < 9:
                date = year + '.0' + str(i+1)
            else:
                date = year + '.' + str(i+1)
            print(date)
            try:
                url = url_ls[i]
                if url[0] == '/':
                    url = 'http://www.customs.gov.cn' + url

                first_row_ele1 = pd.read_html(url)[0].iloc[0,0]
                first_row_ele2 = pd.read_html(url)[0].iloc[0,1]

                # if step==None:  # 为了去除多余的行
                #     if type(first_row_ele1) == str and type(first_row_ele2) == str: # 为了兼容包含及不包含单元两种格式
                #         df = pd.read_html(url)[0].iloc[(iloc_x1-1):iloc_x2,iloc_y].rename(columns=col_name_dict)
                #     else:
                #         df = pd.read_html(url)[0].iloc[iloc_x1:iloc_x2,iloc_y].rename(columns=col_name_dict)
                # else:
                if type(first_row_ele1) == str and type(first_row_ele2) == str:
                    df = pd.read_html(url)[0].iloc[(iloc_x1-1):iloc_x2:step,iloc_y].rename(columns=col_name_dict)
                else:
                    df = pd.read_html(url)[0].iloc[iloc_x1:iloc_x2:step,iloc_y].rename(columns=col_name_dict)

                df['年月'] = date
                df0 = pd.concat([df0,df],ignore_index=True,sort=True)
            except:
                pass
    df0['货币单位'] = unit
    return df0    

# 用于 table16 和 table17
def get_table3(curr_type,col_names,table_index,unit,year_ls=None):
    url_dict = get_urls(curr_type)
    if year_ls==None:
        year_ls = ['2018','2017','2016','2015','2014']
    df0 = pd.DataFrame(columns=col_names)
    key_ls = list(url_dict.keys())[table_index::17]

    name_ls = ['缅甸', '中国香港', '印度', '印度尼西亚', '伊朗', '日本', '中国澳门', '马来西亚', '阿曼', '巴基斯坦', '菲律宾', '沙特阿拉伯', '新加坡', '韩国', '泰国', '土耳其', '阿拉伯联合酋长国', '越南', '中国台湾', '哈萨克斯坦', '南非', '欧洲联盟', '比利时', '丹麦', '英国', '德国', '法国', '意大利', '荷兰', '西班牙', '奥地利', '芬兰', '瑞典', '罗马尼亚', '瑞士', '俄罗斯联邦', '乌克兰', '阿根廷', '巴西', '智利', '加拿大', '美国', '澳大利亚', '新西兰']
    num_ls = [1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,51,53,55,57,59,61,63,65,67,69,71,73,75,77,79,81,83,85,87]

    for i in range(len(year_ls)):
        year = year_ls[i]
        key = key_ls[i]
        url_ls = url_dict[key]
        for i in range(len(url_ls)):
            time.sleep(1)
            if i < 9:
                date = year + '.0' + str(i+1)
            else:
                date = year + '.' + str(i+1)
            print(date)
            try:
                url = url_ls[i]
                if url[0] == '/':
                    url = 'http://www.customs.gov.cn' + url

                first_row_ele1 = pd.read_html(url)[0].iloc[0,0]
                first_row_ele2 = pd.read_html(url)[0].iloc[0,1]

                if type(first_row_ele1) == str and type(first_row_ele2) == str:
                    df = pd.read_html(url)[0]
                    df = pd.concat([df.iloc[2:,0:1].rename(columns={0:'类章'}),df.iloc[2:,1::2].rename(columns=dict(zip(num_ls,name_ls)))],axis=1)
                else:
                    df = pd.read_html(url)[0]
                    df = pd.concat([df.iloc[3:,0:1].rename(columns={0:'类章'}),df.iloc[3:,1::2].rename(columns=dict(zip(num_ls,name_ls)))],axis=1)
                
                df = df.melt(id_vars='类章', var_name='国家（地区）', value_name='金额')
                df['年月'] = date
                df0 = pd.concat([df0,df],ignore_index=True,sort=True)
                #print(len(df0))
            except:
                pass
    df0.reset_index(drop=True)
    df0['货币单位'] = unit
    return df0    

# 
# 用于 table16 和 table17, 使用代理（为了多进程）
def get_table3_v2(curr_type,year_num,col_names,table_index,unit,year_ls=None):
    year_ls = ['2018','2017','2016','2015','2014']
    url_dict = get_urls(curr_type)
    df0 = pd.DataFrame(columns=col_names)
    key_ls = list(url_dict.keys())[table_index::17]

    name_ls = ['缅甸', '中国香港', '印度', '印度尼西亚', '伊朗', '日本', '中国澳门', '马来西亚', '阿曼', '巴基斯坦', '菲律宾', '沙特阿拉伯', '新加坡', '韩国', '泰国', '土耳其', '阿拉伯联合酋长国', '越南', '中国台湾', '哈萨克斯坦', '南非', '欧洲联盟', '比利时', '丹麦', '英国', '德国', '法国', '意大利', '荷兰', '西班牙', '奥地利', '芬兰', '瑞典', '罗马尼亚', '瑞士', '俄罗斯联邦', '乌克兰', '阿根廷', '巴西', '智利', '加拿大', '美国', '澳大利亚', '新西兰']
    num_ls = [1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,51,53,55,57,59,61,63,65,67,69,71,73,75,77,79,81,83,85,87]

    year = year_ls[year_num]
    key = key_ls[year_num]
    url_ls = url_dict[key]
    for i in range(len(url_ls)):
        time.sleep(1)
        if i < 9:
            date = year + '.0' + str(i+1)
        else:
            date = year + '.' + str(i+1)
        print(date)
        try:
            url = url_ls[i]
            if url[0] == '/':
                url = 'http://www.customs.gov.cn' + url

            first_row_ele1 = pd.read_html(url)[0].iloc[0,0]
            first_row_ele2 = pd.read_html(url)[0].iloc[0,1]

            html = get_html2(url)
            if type(first_row_ele1) == str and type(first_row_ele2) == str:
                df = pd.read_html(html)[0]
                df = pd.concat([df.iloc[2:,0:1].rename(columns={0:'类章'}),df.iloc[2:,1::2].rename(columns=dict(zip(num_ls,name_ls)))],axis=1)
            else:
                df = pd.read_html(html)[0]
                df = pd.concat([df.iloc[3:,0:1].rename(columns={0:'类章'}),df.iloc[3:,1::2].rename(columns=dict(zip(num_ls,name_ls)))],axis=1)
            
            df = df.melt(id_vars='类章', var_name='国家（地区）', value_name='金额')
            df['年月'] = date
            df0 = pd.concat([df0,df],ignore_index=True,sort=True)
        except:
            pass
    df0.reset_index(drop=True)
    df0['货币单位'] = unit
    return df0    

# 将人民币表与美元表合并，然后存入数据库
def table_merge_tosql(func1,func2,col_name_dict,table):
    df1 = func1()
    df1 = df1.rename(columns=col_name_dict)
    df1 = df1[list(col_name_dict.values())]
    df1 = df1.replace('-',np.nan)
    df1['curr_type'] = 'CNY'

    df2 = func2()
    df2 = df2.rename(columns=col_name_dict)
    df2 = df2[list(col_name_dict.values())]
    df2 = df2.replace('-',np.nan)
    df2['curr_type'] = 'USD'

    to_sql2(df1,df2,table)

# 将人民币表与美元表合并，然后存入数据库
def table_merge_tosql2(func1,func2,col_name_dict,table):
    df1 = func1()
    df1 = df1.rename(columns=col_name_dict)
    df1 = df1[list(col_name_dict.values())]
    df1 = df1.replace('-',np.nan)
    df1['curr_type'] = 'CNY'
    to_sql(df1,table)

    df2 = func2()
    df2 = df2.rename(columns=col_name_dict)
    df2 = df2[list(col_name_dict.values())]
    df2 = df2.replace('-',np.nan)
    df2['curr_type'] = 'USD'
    to_sql(df2,table)