import asyncio
import re
import time
import aiohttp


import apiclient
import httplib2
import psycopg2
import requests
from aiohttp import ClientTimeout
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By

from config import host, user, password, db_name


FILE_JSON = 'bitriks-413311-b6a6348d8b48.json'
TABLE = "bitriks"

def writing_to_the_excel():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        FILE_JSON,
        ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/drive'])
    httpAuth = credentials.authorize(httplib2.Http())
    service = apiclient.discovery.build('sheets', 'v4', http=httpAuth)

    # подключение к БД
    connection = None
    cursor = None
    list = []
    try:
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
        )
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute('SELECT * FROM "the_company"')
        list = cursor.fetchall()

    except Exception as _ex:
        print("[INFO] Error while working with PostgreSQL", _ex)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("[INFO] PostgreSQL connection closed")

    j = 0
    for i in list:
        values = service.spreadsheets().values().batchUpdate(
            spreadsheetId='1MLa5aAUoMVk1J3kqQf7tRNHpjewjxBvv4fGriAv_lIc',
            body={
                'valueInputOption': 'USER_ENTERED',
                'data': [
                    {
                        'range': f'A{i[0] + 1}',
                        'majorDimension': 'COLUMNS',
                        'values': [[i[0]], [i[1]], [i[2]], [i[3]], [i[4]], [i[5]], [i[6]], [i[7]], [i[8]], [i[9]]]

                    }
                ]
            }
        ).execute()
        print(j)
        if j % 30 == 0:
            time.sleep(30)
        j += 1




def writing_to_the_database(list):
    connection = None
    cursor = None
    try:

        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
        )
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        print(f"Server version: {cursor.fetchone()}")


        cursor.execute('SELECT * FROM "the_company"')
        list_bd = cursor.fetchall()
        #writing_to_the_excel(list_bd)
        flag = True
        flag_overwrites = False
        #проверка на повторную запись
        for i in list:
            for j in list_bd:
                if i[0] == j[1] and i[1] == j[2] and i[4] == j[5] and \
                        i[6] == j[7] and i[8] == j[9]:

                    flag = False
                else:
                    # проверка на перезапись существующей информации
                    if i[0] == j[1]:
                        id = j[0]
                        flag_overwrites = True
                        flag = False

        # если повторной записи нет, записываем в бд
        if flag:
            cursor.execute(
                'INSERT INTO the_company ("Name", "Description", "Rating", "Number of reviews", "Link", "API", "Link API",'
                '"Affiliate Program", "Link Affiliate Program") VALUES '
                '(%s,%s,%s,%s,%s,%s,%s,%s,%s)', (i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8]))
        if flag_overwrites:
            cursor.execute(
                'UPDATE the_company SET "Name" = %s, "Description" = %s, "Rating" = %s, "Number of reviews" = %s, '
                '"Link" = %s, "API" = %s, "Link API" = %s,'
                '"Affiliate Program" = %s, "Link Affiliate Program" = %s WHERE "ID" = %s',
                (i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], id))
        cursor.execute('SELECT * FROM "the_company"')



    except Exception as _ex:
        print("[INFO] Error while working with PostgreSQL", _ex)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("[INFO] PostgreSQL connection closed")

#####################################################
URL = 'https://www.producthunt.com/categories'
URL_content = 'https://www.producthunt.com'
HEADERS = {'user-agent' : 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/93.0.4577.82 YaBrowser/21.9.0.1044 Yowser/2.5 Safari/537.36',
           'accept': '*/*'}



content_url = ''
button_protection_url = ''


def get_html(url, params = None) :
    r = requests.get(url, headers=HEADERS, params=params)
    return r



html_api = '-'
html_affiliate = '-'
bool_api = 'No'
bool_affiliate = 'No'
#парсинг информации о компании
async def get_card(html, session):
    list = []

    headers = {
        "User-Agent": "Mozilla / 5.0(Windows NT 10.0; Win64; x64) AppleWebKit "
                      "/ 537.36(KHTML, like Gecko) Chrome / 116.0 .5845 .686 YaBrowser / 23.9 .0 .0 Safari / 537.36"
    }
    print(html + "111111111")
    async with session.get(url=html, headers=headers) as response:
        html1 = await response.text()
        soup = BeautifulSoup(html1, 'html.parser')

        items = soup.find('h1', class_='color-darker-grey md:fontSize-32 sm:fontSize-32 fontSize-18 fontWeight-700')

        #парсинг названии компании
        name = items.get_text()
        print(name)

        #парсинг описания
        des = soup.find_all('div', class_='flex flex-column')

        descrip1 = des[1].find('div', 'mb-3 color-darker-grey fontSize-16 fontWeight-600')
        descrip2 = des[1].find('div', 'mb-6 color-lighter-grey fontSize-16 fontWeight-400')

        descrip = descrip1.get_text() + descrip2.get_text()
        print(descrip)

        #парсинг рейтинга
        rat = soup.find_all('div', class_='flex flex-row')
        number_of_stars = rat[2].find_all('svg', class_='pr-1 styles_yellowStar__RI1fH')
        number_of_stars = len(number_of_stars)
        print(number_of_stars)

        number_of_reviews = soup.find('a', class_ = 'color-lighter-grey fontSize-14 fontWeight-400 styles_count___6_8F')
        number_of_reviews = re.findall(r'\d+', number_of_reviews.get_text(strip=True))
        if len(number_of_reviews) == 0:
            number_of_reviews.append(0)
        print(number_of_reviews[0])

        #получении главной ссылки компании
        href = soup.find('a', class_='styles_reset__1_PU9 styles_button__7X8Df styles_primary__ZcjWw styles_button__vE9cf')
        try:
            href = href.get('href')
            current_url = href
            if current_url[8:12] == 'play':
                current_url = href
            else:
                # проверка на защиту если есть то берем адресс с контактов, если нету то переходим по ссылки и берем адрес с адресной строки
                if href == '#':
                    current_url = soup.find_all('td', class_='application-page-contact-left')
                    if current_url:
                        current_url = current_url[0].find('div')
                        current_url = current_url.get_text()[6::]
                    # если кнопка заблокирована и в контактах нет ссылки, то берем ее с адресной строки(не реализована, берем ее с функции get_content)
                    else:
                        current_url = button_protection_url
                else:
                    # проверка на ссылку, если она точная сразу записываем, если нет то переходит по ссылки на сайт и парсим саму ссылку
                    # с адресной строки убирая все лишнее
                    if current_url[0:6] == 'https:' or current_url[0:5] == 'http:':
                        # провекра на лишние символы и то что идет после него (/, ?)
                        if current_url.find('t.me') or current_url.find('vk.cc'):
                            if current_url.count('?') > 0:
                                current_url = '?'.join(current_url.split('?')[:-1])
                                # print(current_url)
                        else:
                            if current_url.count('/') > 2:
                                current_url = '/'.join(current_url.split('/')[:-1])
                                # print(current_url)
                            if current_url.count('?') > 0:
                                current_url = '?'.join(current_url.split('?')[:-1])
                                # print(current_url)
                    else:
                        URL1 = URL_content + href
                        # print(URL1)
                        url = requests.get(URL1)
                        current_url = url.url
                        # print(current_url)

                    # проверка если после / какие либо символы, если есть подставляем их
                    try:
                        if get_html(current_url + '/main').status_code == 200:
                            current_url = current_url + '/main'
                        if current_url.find('/app') > 0:
                            test_url = '/'.join(current_url.split('/')[:-1])
                            if get_html(current_url).status_code == 200:
                                current_url = test_url
                    except:
                        pass
        except:
            current_url = '-'
            print(current_url)

        # получение ссылки от API и партнерской программы
        # подлючаемся к selenium

    #task = asyncio.create_task(URL_API_affiliate(current_url))
    #await asyncio.gather(task)


        list.append([name, descrip, number_of_stars, number_of_reviews[0], current_url, bool_api,
                     html_api, bool_affiliate, html_affiliate])
        print(list)
        #writing_to_the_database(list)


async def URL_API_affiliate(html):
    global bool_api, html_api, bool_affiliate, html_affiliate
    print(html+"222222222")
    current_url = html
    try:
        headers = {
            "User-Agent": "Mozilla / 5.0(Windows NT 10.0; Win64; x64) AppleWebKit "
                          "/ 537.36(KHTML, like Gecko) Chrome / 116.0 .5845 .686 YaBrowser / 23.9 .0 .0 Safari / 537.36"
        }
        async with aiohttp.ClientSession() as session2:
            async with session2.get(url=html, headers=headers) as response1:
                html_par = await response1.text()
                soup1 = BeautifulSoup(html_par, 'html.parser')
                items1 = soup1.find_all('a')
                html_api = '-'
                html_affiliate = '-'
                bool_api = 'No'
                bool_affiliate = 'No'
                # поиск ссылки API  изменения ссылки по необходимости
                for item in items1:
                    if 'API' in str(item.text):
                        if '/main' in current_url:
                            html_api = current_url.replace('/main', item.get('href'))
                            bool_api = 'Yes'

                        elif 'https' in item.get('href'):
                            html_api = item.get('href')
                            bool_api = 'Yes'

                        else:
                            html_api = current_url + item.get('href')
                            bool_api = 'Yes'

                    # поиск партнерской программы как у API
                    if 'Партнерская программа' in str(item.text) or 'Аффилиатная программа' in str(item.text):
                        if 'https' in item.get('href'):
                            html_affiliate = item.get('href')
                            bool_affiliate = 'Yes'

                        elif '/main' in current_url:
                            html_affiliate = current_url.replace('/main', item.get('href'))
                            bool_affiliate = 'Yes'

                        else:
                            html_affiliate = current_url + item.get('href')
                            bool_affiliate = 'Yes'
    except Exception as _ex:
        html_api = '--'
        html_affiliate = '--'
        bool_api = 'No'
        print(_ex)
        bool_affiliate = 'No'
    print(current_url, bool_affiliate, html_affiliate, bool_api, html_api)


async def get_content(html) :
    global content_url, button_protection_url
    driver = webdriver.Chrome()
    driver.get(html)
    driver.maximize_window()
    lenpg = driver.execute_script("window.scrollTo(0, document.body.scrollHeight);\
                                                  var lenpg=document.body.scrollHeight;return lenpg;")
    match = False

    # кролинг стр
    while match == False:
        button = driver.find_element(By.XPATH, '//*[@id="__next"]/div[3]/main/div[3]/button')

        # clicking on the button
        button.click()
        lst = lenpg
        time.sleep(1.5)
        lenpg = driver.execute_script("window.scrollTo(0, document.body.scrollHeight);\
                                                      var lenpg=document.body.scrollHeight;return lenpg;")

        if lst == lenpg:
            match = True

    html1 = driver.page_source
    #time.sleep(2)
    driver.close()


    soup = BeautifulSoup(html1, 'html.parser')
    page = soup.find_all('div', class_='flex flex-column mb-10 sm:mb-15')
    timeout = ClientTimeout(total=600)
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=3), timeout=timeout) as session:
        tasks = []
        for item in page:
            item = item.find('a', class_='color-darker-grey fontSize-16 fontWeight-400')
            print(item.get('href'))
            task = asyncio.create_task(get_card(URL_content + item.get('href'), session))
            tasks.append(task)
        await asyncio.gather(*tasks)


#выбор категории
def get_page(html) :

    driver = webdriver.Chrome()
    driver.get(html)
    driver.maximize_window()
    lenpg = driver.execute_script("window.scrollTo(0, document.body.scrollHeight);\
                                          var lenpg=document.body.scrollHeight;return lenpg;")
    match = False
    # кролинг стр
    while match == False:
        lst = lenpg
        time.sleep(1.5)
        lenpg = driver.execute_script("window.scrollTo(0, document.body.scrollHeight);\
                                              var lenpg=document.body.scrollHeight;return lenpg;")
        if lst == lenpg:
            match = True

    html1 = driver.page_source
    driver.close()
    soup = BeautifulSoup(html1, 'html.parser')
    items = soup.find_all('div', class_ = 'flex flex-column gap-3 mb-10 sm:mb-16')
    for item in items:
        URL1 = item.find("a").get('href')
        print(URL1)
        asyncio.run(get_content(URL_content + URL1))




def parse() :
    get_page(URL)

parse()
writing_to_the_excel()