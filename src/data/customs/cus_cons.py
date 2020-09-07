
# 获取单个年度url数据
def get_dict(table_ls):
    url_dict = {}
    for i in range(17):
        title = table_ls[i].select('td')[0].text
        urls = [table_ls[i].select('td')[1].select('a')[j].get('href') for j in range(12)]
        url_dict[title] = urls
    return url_dict

# 获取所有年度url数据
def get_dict2(table_ls):
    url_dict = {}
    for i in range(len(table_ls)):
        url_dict = dict(url_dict,**get_dict(table_ls[i]))
    return url_dict

def get_dict3(table_ls):
    url_dict2 = {}
    for k in range(5):
        url_dict = {}
        for i in range(17):
            print(i)
            title = table_ls[k][i].select('td')[0].text
            urls = [table_ls[k][i].select('td')[1].select('a')[j].get('href') for j in range(12)]
            url_dict[title] = urls
        url_dict2 = dict(url_dict2,**url_dict)
    return url_dict2