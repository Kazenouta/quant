from src.utils.funcs import *
from src.conf.config import *

def save_url():
    for i in range(42, 230):
        url = f'http://qdbot.com/?list-62-{i}.html'
        html = requests.get(url).content.decode('gb18030')

        soup = BeautifulSoup(html,'lxml')
        tds = soup.findAll('td',{'height':'30'})
        titles = [td.find('a').get('title') for td in tds]
        hrefs = [td.find('a').get('href') for td in tds]  
        df = pd.DataFrame({
            'title': titles,
            'href': hrefs
        })
        print(i)
        df.to_sql('urea_daily_url2',ENGINE_FUTURES, if_exists='append',index=False)

def save_text():
    sql = 'select type, href from urea_news_url'
    df = pd.read_sql(sql, ENGINE_FUTURES)
    df = df.loc[df.type=='日评']
    save_path = os.path.join(KOD_HOME, '期货数据/尿素日评')
    urls = df.href.tolist()
    
    for i in range(540, len(urls)):
        url = urls[i]
        print(i, url)
        ret = requests.get(url)
        if ret.status_code != 200:
            continue
        html = ret.content.decode('gb18030')

        soup = BeautifulSoup(html,'lxml')

        title = soup.find('td', attrs={'align':'center', 'class':'hct02', 'height':'52'}).text
        title = title.replace('/', '')
        date = soup.find('td', {'align':'center','bgcolor':'#F4F2E8'}).text.split('\xa0')[6].strip()
        date = re.sub('[\u4E00-\u9FA5\\s：]+', '', date)

        try:
            content = soup.find('font', attrs={'id':'font_word'}).text.strip()
        except:
            content = soup.find('td', {'height':'300'}).text
        content = re.sub('[\xa0\n]+', '\t\n', content)

        print(date, pd.to_datetime(date))
        file_path = os.path.join(save_path, f'{date}_{title}.txt')
        with open(file_path, 'w') as f:
            f.write(content)

