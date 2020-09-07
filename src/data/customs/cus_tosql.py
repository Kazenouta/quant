from .cus_data import *

# 1. 进出口商品总值表 A:年度表
def save_table1():
    df1 = get_table1_1()
    df1 = df1.rename(columns={'年月':'year','出口':'export_value','进口':'import_value','单位':'curr_unit'})
    df1 = df1[['year','export_value','import_value','curr_unit']]
    df1['curr_type'] = 'CNY'

    df2 = get_table1_2()
    df2 = df2.rename(columns={'年月':'year','出口':'export_value','进口':'import_value','单位':'curr_unit'})
    df2 = df2[['year','export_value','import_value','curr_unit']]
    df2['curr_type'] = 'USD'

    to_sql2(df1,df2,'cus_value_yearly')

# 2. 进出口商品总值表 B:月度表
def save_table2():
    df1 = get_table2_1()
    df1 = df1.rename(columns={'年月':'date','出口':'export_value','进口':'import_value','单位':'curr_unit'})
    df1 = df1[['date','export_value','import_value','curr_unit']]
    df1['curr_type'] = 'CNY'

    df2 = get_table2_2()
    df2 = df2.rename(columns={'年月':'date','出口':'export_value','进口':'import_value','单位':'curr_unit'})
    df2 = df2[['date','export_value','import_value','curr_unit']]
    df2['curr_type'] = 'USD'

    to_sql2(df1,df2,'cus_value_monthly')

# 3. 进出口商品国别(地区)总值表
def save_table3():
    df1 = get_table3_1()
    df1 = df1.rename(columns={'国家（地区）':'country','年月':'trade_date','出口':'export_value','进口':'import_value','货币单位':'curr_unit'})
    df1 = df1[['country','date','export_value','import_value','curr_unit']]
    df1 = df1.replace('-',0)
    df1['curr_type'] = 'CNY'

    df2 = get_table3_2()
    df2 = df2.rename(columns={'国家（地区）':'country','年月':'date','出口':'export_value','进口':'import_value','货币单位':'curr_unit'})
    df2 = df2[['country','date','export_value','import_value','curr_unit']]
    df2 = df2.replace('-',0)
    df2['curr_type'] = 'USD'

    to_sql2(df1,df2,'cus_value_country')

# 4. 进出口商品构成表
def save_table4():
    func1 = get_table4_1
    func2 = get_table4_2
    col_name_dict = {'类章':'commodity_class','出口':'export_value','进口':'import_value','年月':'trade_date','货币单位':'curr_unit'}
    table = 'cus_value_commodity1'
    table_merge_tosql(func1,func2,col_name_dict,table)

# 5. 进出口商品类章总值表
def save_table5():
    func1 = get_table5_1
    func2 = get_table5_2
    col_name_dict = {'类章':'commodity_class','出口':'export_value','进口':'import_value','年月':'trade_date','货币单位':'curr_unit'}
    table = 'cus_value_commodity2'
    table_merge_tosql(func1,func2,col_name_dict,table)

# 6. 进出口商品贸易方式总值表
def save_table6():
    func1 = get_table6_1
    func2 = get_table6_2
    col_name_dict = {'贸易方式':'trade_type','出口':'export_value','进口':'import_value','年月':'trade_date','货币单位':'curr_unit'}
    table = 'cus_value_tradetype'
    table_merge_tosql(func1,func2,col_name_dict,table)

# 7. 出口商品贸易方式企业性质总值表
def save_table7():
    col_name_dict = {'贸易方式':'trade_type','货币单位':'curr_unit','企业类型':'corp_type','金额':'export_value','年月':'trade_date'}
    
    df1 = get_table7_1()
    df1 = df1.melt(id_vars=['贸易方式','货币单位','年月'],var_name='企业类型',value_name='金额')
    df1 = df1.rename(columns=col_name_dict)
    df1 = df1[list(col_name_dict.values())]
    df1 = df1.replace('-',0)
    df1['curr_type'] = 'CNY'

    df2 = get_table7_2()
    df2 = df2.melt(id_vars=['贸易方式','货币单位','年月'],var_name='企业类型',value_name='金额')
    df2 = df2.rename(columns=col_name_dict)
    df2 = df2[list(col_name_dict.values())]
    df2 = df2.replace('-',0)
    df2['curr_type'] = 'USD'

    table = 'cus_value_corptype_export'
    to_sql2(df1,df2,table)

#8. 进口商品贸易方式企业性质总值表
def save_table8():
    col_name_dict = {'贸易方式':'trade_type','货币单位':'curr_unit','企业类型':'corp_type','金额':'import_value','年月':'trade_date'}
    
    df1 = get_table8_1()
    df1 = df1.melt(id_vars=['贸易方式','货币单位','年月'],var_name='企业类型',value_name='金额')
    df1 = df1.rename(columns=col_name_dict)
    df1 = df1[list(col_name_dict.values())]
    df1 = df1.replace('-',0)
    df1['curr_type'] = 'CNY'

    df2 = get_table8_2()
    df2 = df2.melt(id_vars=['贸易方式','货币单位','年月'],var_name='企业类型',value_name='金额')
    df2 = df2.rename(columns=col_name_dict)
    df2 = df2[list(col_name_dict.values())]
    df2 = df2.replace('-',0)
    df2['curr_type'] = 'USD'

    table = 'cus_value_corptype_import'
    to_sql2(df1,df2,table)

# 9. 进出口商品经营单位所在地总值表
def save_table9():
    func1 = get_table9_1
    func2 = get_table9_2
    col_name_dict = {'经营单位所在地':'economic_region','出口':'export_value','进口':'import_value','年月':'trade_date','货币单位':'curr_unit'}
    table = 'cus_value_management_units_location'
    table_merge_tosql(func1,func2,col_name_dict,table)

# 10. 进出口商品境内的地/货源地总值表
def save_table10():
    func1 = get_table10_1
    func2 = get_table10_2
    col_name_dict = {'境内目的地／货源地':'economic_region','出口':'export_value','进口':'import_value','年月':'trade_date','货币单位':'curr_unit'}
    table = 'cus_value_commodity_destination/origin'
    table_merge_tosql(func1,func2,col_name_dict,table)

# 11. 进出口商品关别总值表
def save_table11():
    func1 = get_table11_1
    func2 = get_table11_2
    col_name_dict = {'关别':'customs','出口':'export_value','进口':'import_value','年月':'trade_date','货币单位':'curr_unit'}
    table = 'cus_value_customs'
    table_merge_tosql(func1,func2,col_name_dict,table)

# 12. 特定地区进出口总值表
def save_table12():
    func1 = get_table12_1
    func2 = get_table12_2
    col_name_dict = {'特定经济地区':'economic_region','出口':'export_value','进口':'import_value','年月':'trade_date','货币单位':'curr_unit'}
    table = 'cus_value_specific_regions'
    table_merge_tosql(func1,func2,col_name_dict,table)

# 13. 外商投资企业进出口总值表
def save_table13():
    func1 = get_table13_1
    func2 = get_table13_2
    col_name_dict = {'外商投资企业':'foreign_invested_enterprise','出口':'export_value','进口':'import_value','年月':'trade_date','货币单位':'curr_unit'}
    table = 'cus_value_foreign_invested_enterprise'
    table_merge_tosql(func1,func2,col_name_dict,table)

# 14. 出口主要商品量值表
def save_table14():
    func1 = get_table14_1   
    func2 = get_table14_2
    col_name_dict = {'商品名称':'commodity_name','计量单位':'measurement_unit','数量':'export_quantity','年月':'trade_date','金额':'export_value','货币单位':'curr_unit'}
    table = 'cus_quantity_value_commodity_export'
    table_merge_tosql(func1,func2,col_name_dict,table)

# 15. 进口主要商品量值表
def save_table15():
    func1 = get_table15_1   
    func2 = get_table15_2
    col_name_dict = {'商品名称':'commodity_name','计量单位':'measurement_unit','数量':'import_quantity','年月':'trade_date','金额':'import_value','货币单位':'curr_unit'}
    table = 'cus_quantity_value_commodity_import'
    table_merge_tosql(func1,func2,col_name_dict,table)



# 16. 对部分国家(地区)出口商品类章金额表
def save_table16():
    func1 = get_table16_1
    func2 = get_table16_2   
    col_name_dict = {'类章':'commodity_class','国家（地区）':'country','金额':'export_value','年月':'trade_date','货币单位':'curr_unit'}
    table = 'cus_value_commodity_country_export'
    table_merge_tosql2(func1,func2,col_name_dict,table)

# 17. 对部分国家(地区)出口商品类章金额表
def save_table17():
    func1 = get_table17_1
    func2 = get_table17_2   
    col_name_dict = {'类章':'commodity_class','国家（地区）':'country','金额':'import_value','年月':'trade_date','货币单位':'curr_unit'}
    table = 'cus_value_commodity_country_import'
    table_merge_tosql2(func1,func2,col_name_dict,table)


'''
1. 进出口商品总值表 A:年度表          cus_value_yearly
1. 进出口商品总值表 B:月度表          cus_value_monthly
2. 进出口商品国别(地区)总值表         cus_value_country
3. 进出口商品构成表                  cus_value_commodity1
4. 进出口商品类章总值表               cus_value_commodity2
5. 进出口商品贸易方式总值表           cus_value_tradetype
6. 出口商品贸易方式企业性质总值表      cus_value_corptype_export
7. 进口商品贸易方式企业性质总值表      cus_value_corptype_import
8. 进出口商品经营单位所在地总值表      cus_value_management_units_location
9. 进出口商品境内的地/货源地总值表     cus_value_commodity_destination/origin
10.进出口商品关别总值表               cus_value_customs
11.特定地区进出口总值表               cus_value_specific_regions
12.外商投资企业进出口总值表            cus_value_foreign_invested_enterprise
13.出口主要商品量值表                 cus_quantity_value_commodity_export
14.进口主要商品量值表                 cus_quantity_value_commodity_import
15.对部分国家(地区)出口商品类章金额表  cus_value_commodity_country_export
16.对部分国家(地区)进口商品类章金额表  cus_value_commodity_country_import
'''