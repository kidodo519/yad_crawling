import sys
from datetime import date, datetime, timedelta, tzinfo
import requests
import io
import datetime
from bs4 import BeautifulSoup
import os
import math
import csv
import yaml
import psycopg2
from psycopg2 import extras
import traceback
import re


def remove_between_strings(input_string, start_string, end_string):
    start_index = str(input_string).find(start_string)
    end_index = str(input_string).find(end_string)
    if start_index != -1 and end_index != -1:
        output_string = str(input_string)[:start_index] + str(input_string)[end_index + len(end_string):]
        return output_string
    else:
        return input_string

def mid(text, n, m):
        return text[n-1:n+m-1]

def get_between_text(t,a):
        search_text = re.search(str(a) + r'(.+)',t).group(1)
        return re.sub(r"\D", "",search_text)

def get_between_text2(t,a,b):
        return re.search(str(a) + r'(.+)' + str(b),t).group(1)

def post_webhook(config, content):
    requests.post(config['webhook']['url'], json={
        'text': content
    })

def date_text_to_date(text):
    date_list = text.split("-")
    year = int(date_list[0])
    month = int(date_list[1])
    day = int(date_list[2])

    return date(year, month, day)

def make_record_from_row(row, mapping):
    ret = {}
    for db_key, csv_key in mapping['mappings']['reservation_price']['string'].items():
        v = row[csv_key].strip()
        ret[db_key] = v if v != '' else None    

    for db_key, csv_key in mapping['mappings']['reservation_price']['integer'].items():
        v = row[csv_key]
        ret[db_key] = int(v) if v != '' else None

    for db_key, csv_key in mapping['mappings']['reservation_price']['date'].items():
        v = row[csv_key].strip()
        ret[db_key] = str(v) if v != '' and v != '0' else None

    return ret


sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding = 'utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding = 'utf-8')

base_path = os.path.dirname(__file__)
config_path = os.path.join(base_path, 'config.yaml')
with open(config_path, 'r', encoding='utf-8') as fp:
        config = yaml.safe_load(fp)


#driver_path = os.path.join(base_path, 'chromedriver-win32\chromedriver.exe')
#driver_path2 = r'C:\Users\城所 宏次\OneDrive\デスクトップ\Program\python\jalan_crawling\jalan_crawling\v2\chromedriver-win32\chromedriver.exe'

#options = Options()
#options.add_argument('--headless')

#try:
#       driver = webdriver.Chrome(driver_path, options=options)
#except:
#        driver = webdriver.Chrome(service=ChromeService(driver_path))


today_date = str(datetime.date.today())
year = int(today_date[0:4], 10)
month = int(today_date[5:7], 10)
day = int(today_date[8:10], 10)

yado_number = []
res_price = []
day_list = []

for lt in range(0,int(config['conditions']['reservation_counts']['lead_time_max'])+1):
        for ht in config['conditions']['reservation_counts']['holiday_type']:
                if ht == "平日":
                        reservation_date = datetime.date.today() + datetime.timedelta(days=+4) + datetime.timedelta(days=+(int(lt)*7))
                elif ht == "休前日":
                        reservation_date = datetime.date.today() + datetime.timedelta(days=+6) + datetime.timedelta(days=+(int(lt)*7))
                day_dict = {'リードタイム':lt,'予約日':reservation_date, '平休区分':ht, '年':int(str(reservation_date)[0:4], 10), '月':int(str(reservation_date)[5:7], 10), '日':int(str(reservation_date)[8:10], 10)}
                day_list.append(day_dict)

#ページ数取得
print('価格取得開始')
for dl in day_list:
        dly = dl['年']
        dlm = dl['月']
        dld = dl['日']
        for ac in list(config['code']['area_code']):
                prefecture_name = list(config['code']['ken_code'].keys())[0]
                prefecture_code = format(config['code']['ken_code'][prefecture_name], '06')
                area_code = format(config['code']['area_code'][ac], '06')
                area_name = str(ac)
                print('エリアCD: ' + str(area_code))
                mainURL = f'https://www.jalan.net/{prefecture_code}/LRG_{area_code}/?stayYear={dly}&stayMonth={dlm}&stayDay={dld}&stayCount=1&roomCount=1&mealType=3&adultNum=2&ypFlg=1&kenCd={prefecture_code}&screenId=UWW1380&roomCrack=200000&lrgCd={area_code}&distCd=01&rootCd=04'
                getdata1 = requests.get(mainURL)

                soup = BeautifulSoup(getdata1.content, "html.parser")
                elems_yad_count = soup.find_all(class_='jlnpc-listInformation--count')

                #宿数取得
                if elems_yad_count != []:
                        yado_count = str(elems_yad_count[0]).replace("<span class=\"jlnpc-listInformation--count\">", "").replace("</span>", "")
                        page_count = math.ceil(int(yado_count) / 30)

                        #ページごとに宿番号取得
                        for i in range(1, page_count+1):
                                page_URL = f'https://www.jalan.net/{prefecture_code}/LRG_{area_code}/page{i}.html?screenId=UWW1402&distCd=01&activeSort=1&mvTabFlg=1&mealType=3&rootCd=04&stayYear={dly}&stayMonth={dlm}&stayDay={dld}&stayCount=1&roomCount=1&adultNum=2&roomCrack=200000&kenCd={prefecture_code}&lrgCd={area_code}&vosFlg=6&idx={(i-1)*30}'
                                getdata_page = requests.get(page_URL)
                                soup_page = BeautifulSoup(getdata_page.content, "html.parser")
                                elems_yad_num = soup_page.find_all(class_='jlnpc-yadoCassette__link')
                                elems_yad_name = soup_page.find_all('h2', class_='p-searchResultItem__facilityName')
                                elems_yad_url = soup_page.find_all(class_='p-searchResultItem__planName')
                                elems_res_price = soup_page.find_all(class_='p-searchResultItem__lowestPriceValue')

                                #data-href属性で宿番号と宿名を取得、リストに追加
                                for element1, element2 in zip(elems_yad_num, elems_yad_name):
                                        href_values = element1.get('data-href')[4:10]
                                        h2_values = element2.get_text
                                        facility_name = str(h2_values).replace('\n','').replace(' ','').replace('<h2class="p-searchResultItem__facilityName">','').replace('</h2>','').replace('<boundmethodPageElement.get_textof','').replace('>','')
                                        if str.isdigit(href_values):
                                                d = {'都道府県CD': str(prefecture_code), '都道府県': str(prefecture_name), 'エリアCD': str(area_code), 'エリア名': str(area_name),'宿番号': str(href_values).replace(" ","").replace("　",""), '宿名': str(facility_name), '価格': 0,'掲載ページ':str(i), 'プラン名': '', '平休区分': str(dl['平休区分']), 'リードタイム': str(dl['リードタイム']), '予約日': str(dl['予約日']),'予約週': str(today_date),'URL': str(page_URL) }
                                                if d['宿番号'] is not None or d['宿名'] is not None:
                                                        yado_number.append(d)

                                #data-href属性で宿番号と価格を取得、リストに追加
                                for element1, element2 in zip(elems_yad_num, elems_res_price):
                                        href_values = element1.get('data-href')[4:10]
                                        h2_values = element2.get_text
                                        price_values = get_between_text(str(h2_values),'Value">').replace(",","")
                                        if str.isdigit(href_values):
                                                d2 = {'宿番号': str(href_values), '価格': str(price_values),'予約日': str(dl['予約日'])}
                                                print(d2)
                                                if d2['宿番号'] is not None or d2['価格'] is not None:
                                                        res_price.append(d2)

                else:
                        pass

"""
csv_name = 'res_price.csv'
csv_path = os.path.join(base_path, csv_name)
field_name = ['宿番号',  '価格', '予約日']
with open(csv_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames= field_name)
        writer.writeheader()
        writer.writerows(res_price)
"""

for y in res_price:
        for x in yado_number:
                if str(y['宿番号']) + str(y['予約日']) == str(x['宿番号']) + str(x['予約日']):
                        x['価格'] = y['価格']
                        break
for dp in yado_number:
        if dp['価格'] is None or dp['価格'] == '' or dp['価格'] == 0:
              yado_number.remove(dp)


n_records = len(yado_number)

"""
csv_name2 = 'yado_number.csv'
csv_path2 = os.path.join(base_path, csv_name2)
field_name = ['都道府県CD', '都道府県', 'エリアCD', 'エリア名','宿番号', '宿名', '価格','掲載ページ', 'プラン名', '平休区分', 'リードタイム', '予約日','予約週','URL']
with open(csv_path2, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames= field_name)
        writer.writeheader()
        writer.writerows(yado_number)
"""

print('価格' + str(n_records) + '件取得終了')
print('------------------------------------------------')

ordered_keys = [
        *config['mappings']['reservation_price']['string'].keys(),
        *config['mappings']['reservation_price']['integer'].keys(),
        *config['mappings']['reservation_price']['date'].keys()
]

conn = psycopg2.connect(
    host=config['db']['host'],
    port=config['db']['port'],
    user=config['db']['user'],
    password=config['db']['password'],
    database=config['db']['database']
)

mapping_keys = ', '.join(ordered_keys)
table_name = 'area_facility_reservation_prices'
table_insert_query = f'INSERT INTO {table_name}({mapping_keys}) VALUES %s'

try:
        cursor = conn.cursor()
        buf = []
        for row in yado_number:
                record = make_record_from_row(row, config)         
                buf.append([record[k] for k in ordered_keys])
                
        extras.execute_values(cursor, table_insert_query, buf)
        conn.commit()           
        post_webhook(config, f'価格クローリング: DBにインポートしました。 (レコード数: {n_records})')

except Exception as ex:
        msg = traceback.format_exc()
        post_webhook(config, f'価格クローリング: インポート実行中にエラー: {msg}')
        print(msg)
        conn.rollback()
        exit(1)


