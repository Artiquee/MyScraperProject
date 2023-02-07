import re
import difflib
import requests, json
import pandas
import bs4
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from joblib import Parallel, delayed

book = load_workbook('./data/my_books.xlsx')
excel_data_df = pandas.read_excel('./data/my_books.xlsx', sheet_name='Лист1')
amz_links = excel_data_df['LINKS'].tolist()
sheet = book.active

def similarity(s1, s2):
    normalized1 = s1.lower()
    normalized2 = s2.lower()
    matcher = difflib.SequenceMatcher(None, normalized1, normalized2)
    return matcher.ratio()

def title_transform(title, brand=''):
    black_list = [brand, 'new', 'for', 'fits', 'free', 'shipping']
    amz_title = title.split()
    new_title = [x.lower() for x in amz_title if x not in black_list]
    title = '+'.join(new_title)
    return title

def get_ebay(s):
    url = 'https://www.ebay.com/sch/i.html?_from=R40&_nkw=%s&_sacat=0&LH_TitleDesc=1&_sop=15&_stpos=10001&_fcid=1&rt=nc&LH_PrefLoc=3' % s

    try:
        page = requests.get(url, timeout=5)
    except requests.exceptions.ConnectionError:
        print("Connection refused")
        requests.status_code = "Connection refused"

    soup = BeautifulSoup(page.text, 'html.parser')
    links = []
    titles = []
    prices = []
    imgs = []

    for price in soup.find_all(class_='s-item__price'):
        p = price.text.replace('$', '')
        prices.append(p)
    for title in soup.find_all(class_='s-item__title'):
        titles.append(title.text)
    for link in soup.find_all(href=re.compile('ebay.com/itm/'), class_='s-item__link'):
        l = link.get('href').split('?')
        links.append(l[0])
    for img in soup.find_all(class_='s-item__image-wrapper'): #s-item__image-img
        imgs.append(img.get('src'))
    ebayItems = {titles[i]: (links[i], prices[i], imgs[i]) for i in range(len(titles))}
    # TODO Проверка на страну поставщика
    return ebayItems

amz_list = [
    (x, sheet['B%s' % (i + 2)].value, sheet['C%s' % (i + 2)].value or 0, sheet['D%s' % (i + 2)].value or 'No brand') for
    i, x in enumerate(amz_links)]

def get_ebay_list(amz):
    _amz_link, amz_title, amz_price, amz_brand = amz

    amz_title_e = title_transform(amz_title)
    ebayItems = get_ebay(amz_title_e)
    ebayLinks = {}

    for i in ebayItems:
        sim = similarity(amz_title, i)
        if sim > 0.50:
            ebayLinks[ebayItems[i][0]] = {"name": i, "similarity": sim, "price": ebayItems[i][1],
                                          "img": ebayItems[i][2], 'link': ebayItems[i][0]}

    if len(ebayLinks) == 0:
        amz_title_e1 = amz_title_e.split('+')
        amz_title_e2 = '+'.join(amz_title_e1[:len(amz_title_e1) // 2])
        ebayItems = get_ebay(amz_title_e2)

        for j in ebayItems:
            sim = similarity(amz_title, j)
            if sim > 0.50:
                if ebayItems[j][0] not in ebayLinks:
                    ebayLinks[ebayItems[j][0]] = {"name": j, "similarity": sim, "price": ebayItems[j][1],
                                                  "img": ebayItems[j][2], 'link': ebayItems[j][0]}

    link_list = sorted(ebayLinks.items(), key=lambda x: x[1]['similarity'], reverse=True)

    # Ebay item block
    result = []
    for link in link_list:
        try:
            page = requests.get(link[0], timeout=5)
        except requests.exceptions.ConnectionError:
            print("Connection refused")
            requests.status_code = "Connection refused"

        soup = BeautifulSoup(page.text, 'html.parser')

        # Ebay item available
        try:
            ebayAvailable = soup.find('div', {'class': 'd-quantity__availability'}).text
            if 'Last One' in ebayAvailable:
                ebayAvailable = 1
            else:
                ebayAvailable = ebayAvailable.split()
                ebayAvailable = int(ebayAvailable[0])
        except:
            ebayAvailable = 0
        ebayShippingBlackList = ['more', 'than', 'available', ' ', '%', 'positive', 'feedback', 'US', 'AU', '$', 'GBP',
                                 'EUR', 'C', ',']
        # Ebay item rating
        try:
            ebayRatingP = soup.find(string=re.compile('Positive feedback'))
            ebayRatingP = ebayRatingP.lower()
            for x in ebayShippingBlackList:
                ebayRatingP = ebayRatingP.replace(x, '')
        except:
            ebayRatingP = 1
        ebayRating = soup.find_all(class_='ux-textspans ux-textspans--PSEUDOLINK')
        for i in ebayRating:
            if i.text.isnumeric():
                ebayRating = int(i.text)
        if type(ebayRating) == bs4.element.ResultSet:
            ebayRating = 1000
        if type(ebayRatingP) != int:
            if len(ebayRatingP) > 10:
                ebayRatingP = 96

        try:
            ebay_price = soup.find('span', {'itemprop': 'price'}).text
        except:
            print(link)
        ebay_price = str(ebay_price).replace('/ea', '')

        for i in ebayShippingBlackList:
            ebay_price = ebay_price.replace(i, "")
        ebay_price = float(ebay_price)
        margin = amz_price * 0.85 - ebay_price
        roi = margin / float(ebay_price) * 100

        if roi > 15 and ebayAvailable >= 5 and ebayRating >= 100 and float(ebayRatingP) > 95:
            result.append({"link": ebayLinks[link[0]]})

    return {"title": amz_title, "price": amz_price, "link": _amz_link, "ebay": result}

final = Parallel(n_jobs=-1)(delayed(get_ebay_list)(x) for x in amz_list)
ff = [x for x in final if x['ebay']]

new_f = []

for x in ff:
    max_sim = max([y['link']['similarity'] for y in x['ebay']])
    t = x
    t['max_sim'] = max_sim
    new_f.append(t)

ff = sorted(new_f, key=lambda x: x['max_sim'], reverse=True)
s = json.dumps(ff)
with open('./data/3.json', 'w') as f:
    f.write(s)