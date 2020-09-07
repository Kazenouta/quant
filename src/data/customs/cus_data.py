from .cus_func import *


# 1_1. 进出口商品总值表(人民币值) A:年度表
def get_table1_1():
    url = 'http://www.customs.gov.cn/customs/302249/302274/302276/2278772/index.html'
    df = pd.read_html(url)[0].iloc[3:,:5]
    df['单位'] = '亿元'
    return df
    
# 1_2. 进出口商品总值表(美元值) A:年度表
def get_table1_2():
    url = 'http://www.customs.gov.cn/customs/302249/302274/302276/2278926/index.html'
    df = pd.read_html(url)[0].iloc[3:,:5]
    df = df.rename(columns={0:'年月',1:'进出口',2:'出口',3:'进口',4:'贸易差额'})
    df['单位'] = '百万美元'
    return df

# 2_1. 进出口商品总值表(人民币值) B:月度表
def get_table2_1():
    col_name = {0:'年月',1:'进出口',2:'出口',3:'进口',4:'贸易差额'}
    url_18 = 'http://www.customs.gov.cn/customs/302249/302274/302276/2278775/index.html'
    url_17 = 'http://www.customs.gov.cn/customs/302249/302274/302276/1420951/index.html'
    url_16 = 'http://www.customs.gov.cn/customs/302249/302274/302276/631992/index.html'
    url_15 = 'http://www.customs.gov.cn/customs/302249/302274/302276/310672/index.html'
    url_14 = 'http://www.customs.gov.cn/customs/302249/302274/302276/310263/index.html'

    df = pd.read_html(url_18)[0].iloc[16:-1,:5].iloc[::3].reset_index(drop=True)
    df = pd.concat([df,pd.read_html(url_18)[0].iloc[3:15,:5]],ignore_index=True)
    df = pd.concat([df,pd.read_html(url_17)[0].iloc[3:15,:5]],ignore_index=True)
    df = pd.concat([df,pd.read_html(url_16)[0].iloc[3:15,:5]],ignore_index=True)
    df = pd.concat([df,pd.read_html(url_15)[0].iloc[3:15,:5]],ignore_index=True)
    df = pd.concat([df,pd.read_html(url_14)[0].iloc[3:15,:5]],ignore_index=True)
    df = df.rename(columns={0:'年月',1:'进出口',2:'出口',3:'进口',4:'贸易差额'})
    df['单位'] = '亿元'
    return df

# 2_2. 进出口商品总值表(美元值) B:月度表
def get_table2_2():
    col_name = {0:'年月',1:'进出口',2:'出口',3:'进口',4:'贸易差额'}
    url_18 = 'http://www.customs.gov.cn/customs/302249/302274/302276/2278964/index.html'
    url_17 = 'http://www.customs.gov.cn/customs/302249/302274/302276/1421234/index.html'
    url_16 = 'http://www.customs.gov.cn/customs/302249/302274/302276/632007/index.html'
    url_15 = 'http://www.customs.gov.cn/customs/302249/302274/302276/310689/index.html'
    url_14 = 'http://www.customs.gov.cn/customs/302249/302274/302276/310278/index.html'

    df = pd.read_html(url_18)[0].iloc[16:-1,:5].iloc[::3].reset_index(drop=True)
    df = pd.concat([df,pd.read_html(url_18)[0].iloc[3:15,:5]],ignore_index=True)
    df = pd.concat([df,pd.read_html(url_17)[0].iloc[3:15,:5]],ignore_index=True)
    df = pd.concat([df,pd.read_html(url_16)[0].iloc[3:15,:5]],ignore_index=True)
    df = pd.concat([df,pd.read_html(url_15)[0].iloc[3:15,:5]],ignore_index=True)
    df = pd.concat([df,pd.read_html(url_14)[0].iloc[3:15,:5]],ignore_index=True)
    df = df.rename(columns={0:'年月',1:'进出口',2:'出口',3:'进口',4:'贸易差额'})
    df['单位'] = '百万美元'
    return df

# 3_1. 进出口商品国别(地区)总值表（人民币）
def get_table3_1():
    curr_type = 0
    year_ls = ['2018']
    col_names = ['国家（地区）','进口','出口','年月']
    table_index = 2
    iloc_x1 = 3
    iloc_x2 = -1
    iloc_y = [0,3,5]
    col_name_dict = {0:'国家（地区）',3:'出口',5:'进口'}
    unit = '万元'
    df = get_table(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit)
    return df

# 3_2. 进出口商品国别(地区)总值表（美元）
def get_table3_2():
    curr_type = 1
    year_ls = ['2018']
    col_names = ['国家（地区）','进口','出口','年月']
    table_index = 2
    iloc_x1 = 3
    iloc_x2 = -1
    iloc_y = [0,3,5]
    col_name_dict = {0:'国家（地区）',3:'出口',5:'进口'}
    unit = '千美元'
    df = get_table(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit)
    return df

# 4_1. 进出口商品构成表（人民币）
def get_table4():
    url_dict = get_urls()
    year_ls = ['2018','2017','2016','2015','2014']
    df0 = pd.DataFrame(columns=['分类一','分类二','分类三','出口','进口','年月'])
    cat1_ls = ['初级产品','工业制品']
    cat2_ls = ['0类 食品及活动物','1类 饮料及烟类','2类 非食用原料（燃料除外）','3类 矿物燃料、润滑油及有关原料','4类 动植物油、脂及蜡','5类 化学成品及有关产品','6类 按原料分类的制成品','7类 机械及运输设备','8类 杂项制品','9类 未分类的商品']

    for year in year_ls:
        df00 = pd.DataFrame(columns=['分类一','分类二','分类三','出口','进口'])
        key = '(3){year}年进出口商品构成表'.format(year=year)        
        url_ls = url_dict[key]
        for i in range(len(url_ls)):
            if i < 9:
                date = year + '.0' + str(i+1)
            else:
                date = year + '.' + str(i+1)
            print(date)
            try:
                url = url_ls[i]
                if url[0] == '/':
                    url = 'http://www.customs.gov.cn' + url
                df = pd.read_html(url)[0]

                df10 = df.iloc[78:79,[0,1,3]]
                df10 = df10.rename(columns={0:'分类三',1:'出口',3:'进口'})
                df10['分类一'] = cat1_ls[1]
                df10['分类二'] = cat2_ls[9]
                df10['分类三'] = '90章 未分类商品'

                df9 = df.iloc[70:78,[0,1,3]]
                df9 = df9.rename(columns={0:'分类三',1:'出口',3:'进口'})
                df9['分类一'] = cat1_ls[1]
                df9['分类二'] = cat2_ls[8]

                df8 = df.iloc[60:69,[0,1,3]]
                df8 = df8.rename(columns={0:'分类三',1:'出口',3:'进口'})
                df8['分类一'] = cat1_ls[1]
                df8['分类二'] = cat2_ls[7]

                df7 = df.iloc[50:59,[0,1,3]]
                df7 = df7.rename(columns={0:'分类三',1:'出口',3:'进口'})
                df7['分类一'] = cat1_ls[1]
                df7['分类二'] = cat2_ls[6]

                df6 = df.iloc[40:49,[0,1,3]]
                df6 = df6.rename(columns={0:'分类三',1:'出口',3:'进口'})
                df6['分类一'] = cat1_ls[1]
                df6['分类二'] = cat2_ls[5]

                df1 = df.iloc[6:16,[0,1,3]]
                df1 = df1.rename(columns={0:'分类三',1:'出口',3:'进口'})
                df1['分类一'] = cat1_ls[0]
                df1['分类二'] = cat2_ls[0]

                df2 = df.iloc[17:19,[0,1,3]]
                df2 = df2.rename(columns={0:'分类三',1:'出口',3:'进口'})
                df2['分类一'] = cat1_ls[0]
                df2['分类二'] = cat2_ls[1]

                df3 = df.iloc[20:29,[0,1,3]]
                df3 = df3.rename(columns={0:'分类三',1:'出口',3:'进口'})
                df3['分类一'] = cat1_ls[0]
                df3['分类二'] = cat2_ls[2]

                df4 = df.iloc[30:34,[0,1,3]]
                df4 = df4.rename(columns={0:'分类三',1:'出口',3:'进口'})
                df4['分类一'] = cat1_ls[0]
                df4['分类二'] = cat2_ls[3]

                df5 = df.iloc[35:38,[0,1,3]]
                df5 = df5.rename(columns={0:'分类三',1:'出口',3:'进口'})
                df5['分类一'] = cat1_ls[0]
                df5['分类二'] = cat2_ls[4]

                df00 = pd.concat([df00,df1,df2,df3,df4,df5,df6,df7,df8,df9,df10])
                df00['年月'] = date
            except:
                pass
        df0 = pd.concat([df0,df00],ignore_index=True)
    df0['单位'] = '万元'
    return df0

def get_table4_1():
    curr_type = 0
    year_ls = ['2018']
    col_names = ['类章','出口','进口','年月']
    table_index = 3
    iloc_x1 = 3
    iloc_x2 = 79
    iloc_y = [0,1,3]
    col_name_dict = {0:'类章',1:'出口',3:'进口'}
    unit = '万元'
    df = get_table(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit)
    return df

# 4_2. 进出口商品构成表（美元值）
def get_table4_2():
    curr_type = 1
    year_ls = ['2018']
    col_names = ['类章','出口','进口','年月']
    table_index = 3
    iloc_x1 = 3
    iloc_x2 = 79
    iloc_y = [0,1,3]
    col_name_dict = {0:'类章',1:'出口',3:'进口'}
    unit = '千美元'
    df = get_table(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit)
    return df

# 5_1. 进出口商品类章总值表（人民币）
def get_table5_1():
    curr_type = 0
    year_ls = ['2018']
    col_names = ['类章','出口','进口','年月']
    table_index = 4
    iloc_x1 = 3
    iloc_x2 = -1
    iloc_y = [0,1,3]
    col_name_dict = {0:'类章',1:'出口',3:'进口'}
    unit = '万元'
    df = get_table(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit)
    return df

# 5_2. 进出口商品类章总值表（美元）
def get_table5_2():
    curr_type = 1
    year_ls = ['2018']
    col_names = ['类章','出口','进口','年月']
    table_index = 4
    iloc_x1 = 3
    iloc_x2 = -1
    iloc_y = [0,1,3]
    col_name_dict = {0:'类章',1:'出口',3:'进口'}
    unit = '千美元'
    df = get_table(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit)
    return df

# 6_1. 进出口商品贸易方式总值表（人民币）
def get_table6_1():
    curr_type = 0
    year_ls = ['2018']
    col_names = ['贸易方式','出口','进口','年月']
    table_index = 5
    iloc_x1 = 1
    iloc_x2 = -1
    step = 2
    iloc_y = [0,3,5]
    col_name_dict = {0:'贸易方式',3:'出口',5:'进口'}
    unit = '万元'
    df = get_table2(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit,step=step)
    return df

# 6_2. 进出口商品贸易方式总值表（美元）
def get_table6_2():
    curr_type = 1
    year_ls = ['2018']
    col_names = ['贸易方式','出口','进口','年月']
    table_index = 5
    iloc_x1 = 1
    iloc_x2 = -1
    step = 2
    iloc_y = [0,3,5]
    col_name_dict = {0:'贸易方式',3:'出口',5:'进口'}
    unit = '千美元'
    df = get_table2(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit,step=step)
    return df

# 7_1. 出口商品贸易方式企业性质总值表（人民币）
def get_table7_1():
    curr_type = 0
    year_ls = ['2018']
    col_names = ['贸易方式','国有企业','中外合作','中外合资','外商独资','私营企业','年月']
    table_index = 6
    iloc_x1 = 4
    iloc_x2 = -1
    step = 2
    iloc_y = [0,2,4,5,6,7]
    col_name_dict = {0:'贸易方式',2:'国有企业',4:'中外合作',5:'中外合资',6:'外商独资',7:'私营企业'}
    unit = '万元'
    df = get_table2(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit,step=step)
    return df

# 7_2. 出口商品贸易方式企业性质总值表（美元）
def get_table7_2():
    curr_type = 1
    year_ls = ['2018']
    col_names = ['贸易方式','国有企业','中外合作','中外合资','外商独资','私营企业','年月']
    table_index = 6
    iloc_x1 = 4
    iloc_x2 = -1
    step = 2
    iloc_y = [0,2,4,5,6,7]
    col_name_dict = {0:'贸易方式',2:'国有企业',4:'中外合作',5:'中外合资',6:'外商独资',7:'私营企业'}
    unit = '千美元'
    df = get_table2(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit,step=step)
    return df

# 8_1. 进口商品贸易方式企业性质总值表（人民币）
def get_table8_1():
    curr_type = 0
    year_ls = ['2018']
    col_names = ['贸易方式','国有企业','中外合作','中外合资','外商独资','私营企业','年月']
    table_index = 7
    iloc_x1 = 4
    iloc_x2 = -1
    step = 2
    iloc_y = [0,2,4,5,6,7]
    col_name_dict = {0:'贸易方式',2:'国有企业',4:'中外合作',5:'中外合资',6:'外商独资',7:'私营企业'}
    unit = '万元'
    df = get_table2(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit,step=step)
    return df

# 8_2. 进口商品贸易方式企业性质总值表（美元）
def get_table8_2():
    curr_type = 1
    year_ls = ['2018']
    col_names = ['贸易方式','国有企业','中外合作','中外合资','外商独资','私营企业','年月']
    table_index = 7
    iloc_x1 = 4
    iloc_x2 = -1
    step = 2
    iloc_y = [0,2,4,5,6,7]
    col_name_dict = {0:'贸易方式',2:'国有企业',4:'中外合作',5:'中外合资',6:'外商独资',7:'私营企业'}
    unit = '千美元'
    df = get_table2(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit,step=step)
    return df

# 9_1. 进出口商品经营单位所在地总值表（人民币）
def get_table9_1():
    curr_type = 0
    year_ls = ['2018']
    col_names = ['经营单位所在地','出口','进口','年月']
    table_index = 8
    iloc_x1 = 3
    iloc_x2 = -1
    step = 1
    iloc_y = [0,3,5]
    col_name_dict = {0:'经营单位所在地',3:'出口',5:'进口'}
    unit = '万元'
    df = get_table2(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit,step=step)
    return df

# 9_2. 进出口商品经营单位所在地总值表（美元）
def get_table9_2():
    curr_type = 1
    year_ls = ['2018']
    col_names = ['经营单位所在地','出口','进口','年月']
    table_index = 8
    iloc_x1 = 3
    iloc_x2 = -1
    step = 1
    iloc_y = [0,3,5]
    col_name_dict = {0:'经营单位所在地',3:'出口',5:'进口'}
    unit = '千美元'
    df = get_table2(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit,step=step)
    return df

# 10_1. 进出口商品境内的地/货源地总值表
def get_table10_1():
    curr_type = 0
    year_ls = ['2018']
    col_names = ['境内目的地／货源地','出口','进口','年月']
    table_index = 9
    iloc_x1 = 3
    iloc_x2 = None
    step = 1
    iloc_y = [0,3,5]
    col_name_dict = {0:'境内目的地／货源地',3:'出口',5:'进口'}
    unit = '万元'
    df = get_table2(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit,step=step)
    return df

# 10_2. 进出口商品境内的地/货源地总值表
def get_table10_2():
    curr_type = 1
    year_ls = ['2018']
    col_names = ['境内目的地／货源地','出口','进口','年月']
    table_index = 9
    iloc_x1 = 3
    iloc_x2 = None
    step = 1
    iloc_y = [0,3,5]
    col_name_dict = {0:'境内目的地／货源地',3:'出口',5:'进口'}
    unit = '千美元'
    df = get_table2(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit,step=step)
    return df

# 11_1. 进出口商品关别总值表
def get_table11_1():
    curr_type = 0
    year_ls = ['2018']
    col_names = ['关别','出口','进口','年月']
    table_index = 10
    iloc_x1 = 3
    iloc_x2 = -1
    step = 1
    iloc_y = [0,3,5]
    col_name_dict = {0:'关别',3:'出口',5:'进口'}
    unit = '万元'
    df = get_table2(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit,step=step)
    return df

# 11_2. 进出口商品关别总值表
def get_table11_2():
    curr_type = 1
    year_ls = ['2018']
    col_names = ['关别','出口','进口','年月']
    table_index = 10
    iloc_x1 = 3
    iloc_x2 = -1
    step = 1
    iloc_y = [0,3,5]
    col_name_dict = {0:'关别',3:'出口',5:'进口'}
    unit = '千美元'
    df = get_table2(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit,step=step)
    return df

# 12_1. 特定地区进出口总值表
def get_table12_1():
    curr_type = 0
    year_ls = ['2018']
    col_names = ['特定经济地区','出口','进口','年月']
    table_index = 11
    iloc_x1 = 3
    iloc_x2 = -1
    step = 1
    iloc_y = [0,3,5]
    col_name_dict = {0:'特定经济地区',3:'出口',5:'进口'}
    unit = '万元'
    df = get_table2(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit,step=step)
    return df

# 12_2. 特定地区进出口总值表
def get_table12_2():
    curr_type = 1
    year_ls = ['2018']
    col_names = ['特定经济地区','出口','进口','年月']
    table_index = 11
    iloc_x1 = 3
    iloc_x2 = -1
    step = 1
    iloc_y = [0,3,5]
    col_name_dict = {0:'特定经济地区',3:'出口',5:'进口'}
    unit = '千美元'
    df = get_table2(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit,step=step)
    return df

# 13_1. 外商投资企业进出口总值表
def get_table13_1():
    curr_type = 0
    year_ls = ['2018']
    col_names = ['外商投资企业','出口','进口','年月']
    table_index = 12
    iloc_x1 = 3
    iloc_x2 = -1
    step = 1
    iloc_y = [0,3,5]
    col_name_dict = {0:'外商投资企业',3:'出口',5:'进口'}
    unit = '万元'
    df = get_table2(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit,step=step)
    return df

# 13_2. 外商投资企业进出口总值表
def get_table13_2():
    curr_type = 1
    year_ls = ['2018']
    col_names = ['外商投资企业','出口','进口','年月']
    table_index = 12
    iloc_x1 = 3
    iloc_x2 = -1
    step = 1
    iloc_y = [0,3,5]
    col_name_dict = {0:'外商投资企业',3:'出口',5:'进口'}
    unit = '千美元'
    df = get_table2(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit,step=step)
    return df
 
# 14_1. 出口主要商品量值表
def get_table14_1():
    curr_type = 0
    year_ls = ['2018']
    col_names = ['商品名称','计量单位','数量','金额','年月']
    table_index = 13
    iloc_x1 = 3
    iloc_x2 = -1
    step = 1
    iloc_y = [0,1,2,3]
    col_name_dict = {0:'商品名称',1:'计量单位',2:'数量',3:'金额'}
    unit = '万元'
    df = get_table2(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit,step=step)
    df = df.loc[df.商品名称!='其中：'].reset_index(drop=True)
    return df

# 14_2. 出口主要商品量值表
def get_table14_2():
    curr_type = 1
    year_ls = ['2018']
    col_names = ['商品名称','计量单位','数量','金额','年月']
    table_index = 13
    iloc_x1 = 3
    iloc_x2 = -1
    step = 1
    iloc_y = [0,1,2,3]
    col_name_dict = {0:'商品名称',1:'计量单位',2:'数量',3:'金额'}
    unit = '千美元'
    df = get_table2(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit,step=step)
    df = df.loc[df.商品名称!='其中：'].reset_index(drop=True)
    return df

# 15_1. 进口主要商品量值表
def get_table15_1():
    curr_type = 0
    year_ls = ['2018']
    col_names = ['商品名称','计量单位','数量','金额','年月']
    table_index = 14
    iloc_x1 = 3
    iloc_x2 = -1
    step = 1
    iloc_y = [0,1,2,3]
    col_name_dict = {0:'商品名称',1:'计量单位',2:'数量',3:'金额'}
    unit = '万元'
    df = get_table2(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit,step=step)
    df = df.loc[df.商品名称!='其中：'].reset_index(drop=True)
    return df

# 15_2. 进口主要商品量值表
def get_table15_2():
    curr_type = 1
    year_ls = ['2018']
    col_names = ['商品名称','计量单位','数量','金额','年月']
    table_index = 14
    iloc_x1 = 3
    iloc_x2 = -1
    step = 1
    iloc_y = [0,1,2,3]
    col_name_dict = {0:'商品名称',1:'计量单位',2:'数量',3:'金额'}
    unit = '千美元'
    df = get_table2(curr_type,col_names,table_index,iloc_x1,iloc_x2,iloc_y,col_name_dict,unit,step=step)
    df = df.loc[df.商品名称!='其中：'].reset_index(drop=True)
    return df

# 16_1. 对部分国家(地区)出口商品类章金额表    
def get_table16_1():
    curr_type = 0 
    col_names = ['类章','国家（地区）','金额','年月']
    table_index = 15
    unit = '万元'
    df = get_table3(curr_type,col_names,table_index,unit)
   
    return df

# 16_2. 对部分国家(地区)出口商品类章金额表    
def get_table16_2():
    curr_type = 1 
    col_names = ['类章','国家（地区）','金额','年月']
    table_index = 15
    unit = '千美元'
    df = get_table3(curr_type,col_names,table_index,unit)
   
    return df

# 17_1. 对部分国家(地区)出口商品类章金额表    
def get_table17_1():
    curr_type = 0 
    col_names = ['类章','国家（地区）','金额','年月']
    table_index = 16
    unit = '万元'
    df = get_table3(curr_type,col_names,table_index,unit)
   
    return df

# 17_2. 对部分国家(地区)出口商品类章金额表    
def get_table17_2():
    curr_type = 1 
    col_names = ['类章','国家（地区）','金额','年月']
    table_index = 16
    unit = '千美元'
    df = get_table3(curr_type,col_names,table_index,unit)
   
    return df
