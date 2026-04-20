import sys
import requests
import io
import datetime
from bs4 import BeautifulSoup
import os
import math
import yaml
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
import psycopg2
from psycopg2 import extras
import traceback
import csv

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

def post_webhook(config, content, status):
        if status == 'error':
                requests.post(config['webhook']['error']['url'], json={
                'text': content
                })
        else:
                requests.post(config['webhook']['success']['url'], json={
                'text': content
                })

def make_record_from_row(row, mapping):
    ret = {}
    for db_key, csv_key in mapping['mappings']['reservation_counts']['string'].items():
        v = row[csv_key].strip()
        ret[db_key] = v if v != '' else None    

    for db_key, csv_key in mapping['mappings']['reservation_counts']['integer'].items():
        v = row[csv_key]
        ret[db_key] = int(v) if v != '' else None

    for db_key, csv_key in mapping['mappings']['reservation_counts']['date'].items():
        v = row[csv_key].strip()
        ret[db_key] = str(v) if v != '' and v != '0' else None

    return ret

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding = 'utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding = 'utf-8')

base_path = os.path.dirname(__file__)
config_path = os.path.join(base_path, 'config.yaml')
with open(config_path, 'r', encoding='utf-8') as fp:
        config = yaml.safe_load(fp)


today_date = str(datetime.date.today())
reservation_date = datetime.date.today() + datetime.timedelta(days=-1)

yado_number = []
yad_plan_url = []
res_count = []

#ページ数取得
print('宿番号取得開始')
for ac in list(config['code']['area_code']):
        prefecture_name = list(config['code']['ken_code'].keys())[0]
        prefecture_code = format(config['code']['ken_code'][prefecture_name], '06')
        area_code = format(config['code']['area_code'][ac], '06')
        area_name = str(ac)
        print('エリアCD: ' + str(area_code))
        mainURL = f'https://www.jalan.net/{prefecture_code}/LRG_{area_code}/?stayYear=&stayMonth=&stayDay=&dateUndecided=1&stayCount=1&roomCount=1&adultNum=2&ypFlg=1&kenCd={prefecture_code}&screenId=UWW1380&roomCrack=200000&lrgCd={area_code}&distCd=01&rootCd=04&yadRk=1&yadHb=1'
        getdata1 = requests.get(mainURL)

        soup = BeautifulSoup(getdata1.content, "html.parser")
        elems_yad_count = soup.find_all(class_='jlnpc-listInformation--count')

        #宿数取得（旅館0の場合pass）
        if elems_yad_count != []:
                yado_count = str(elems_yad_count[0]).replace("<span class=\"jlnpc-listInformation--count\">", "").replace("</span>", "")
                page_count = math.ceil(int(yado_count) / 30)

                #ページごとに宿番号取得
                for i in range(1, page_count+1):
                        page_URL = f'https://www.jalan.net/{prefecture_code}/LRG_{area_code}/page{i}.html?screenId=UWW1402&distCd=01&activeSort=0&mvTabFlg=1&rootCd=04&stayYear=&stayMonth=&stayDay=&stayCount=1&roomCount=1&dateUndecided=1&adultNum=2&roomCrack=200000&kenCd={prefecture_code}&lrgCd={area_code}&vosFlg=6&idx={(i-1)*30}&yadRk=1&yadHb=1'
                        getdata_page = requests.get(page_URL)
                        soup_page = BeautifulSoup(getdata_page.content, "html.parser")
                        elems_yad_num = soup_page.find_all(class_='jlnpc-yadoCassette__link')
                        elems_yad_name = soup_page.find_all('h2', class_='p-searchResultItem__facilityName')
                        elems_yad_url = soup_page.find_all(class_='p-searchResultItem__planName')

                        #data-href属性で宿番号を取得、リストに追加
                        for element1, element2 in zip(elems_yad_num, elems_yad_name):
                                href_values = element1.get('data-href')[4:10]
                                h2_values = element2.get_text
                                facility_name = str(h2_values).replace('\n','').replace(' ','').replace('<h2class="js-yadNamep-searchResultItem__facilityName"','').replace(r'\u','').replace('<boundmethodPageElement.get_textof>','').replace('</h2>>','')
                                if str.isdigit(href_values):
                                        d = {'都道府県CD': prefecture_code, '都道府県': prefecture_name, 'エリアCD': area_code, 'エリア名': area_name,'宿番号': href_values, '宿名': facility_name, 'プランCD':'', '部屋タイプCD':'','掲載ページ':i}
                                        print(d)
                                        if d['宿番号'] is not None or d['宿名'] is not None:
                                                yado_number.append(d)

                        #href属性でURLから宿番号とプランCDを取得、リストに追加

                        for element in elems_yad_url:
                                href_values = element.get('href')                     
                                elem_values = str(element.get_text)

                                start_plancd = elem_values.find('planCd=')+8
                                start_roomcd = elem_values.find('roomTypeCd=')+12
                                start_yadno = elem_values.find('yadNo=')+7

                                if start_plancd == 7 or start_yadno == 6 :
                                        pass
                                else:
                                        facility_code = mid(elem_values,start_yadno, 6)
                                        plan_code = mid(elem_values,start_plancd, 8)
                                        room_code = mid(elem_values,start_roomcd, 7)
                                        print('宿番号：' + str(facility_code))
                                        print('プランCD：' + str(plan_code))
                                        print('部屋タイプCD：' + str(room_code))
                                        if str.isdigit(facility_code) and str.isdigit(plan_code) and str.isdigit(room_code):
                                                d2 = {'宿番号': facility_code, 'プランCD': plan_code, '部屋タイプCD': room_code}
                                                if d2['宿番号'] is not None and d2['プランCD'] is not None and d2['部屋タイプCD'] is not None:
                                                        print(d2)
                                                        if len(yad_plan_url) == 0:
                                                                yad_plan_url.append(d2)
                                                        else:
                                                                for index,yn in enumerate(yad_plan_url):
                                                                        if yn['宿番号'] == facility_code:
                                                                                break
                                                                        if len(yad_plan_url)-1 == index:
                                                                                yad_plan_url.append(d2)
        else:
                pass
print('宿番号' + str(len(yado_number)) + '件取得終了')
print('------------------------------------------------')

for y in yad_plan_url:
        for x in yado_number:
                if y['宿番号'] == x['宿番号']:
                        x['プランCD'] = y['プランCD']
                        x['部屋タイプCD'] = y['部屋タイプCD']
                        break


print('予約件数取得開始')

driver_path = config['settings']['driver_path']
options = Options()
# options.add_argument('--headless')
driver = webdriver.Chrome(service=ChromeService(driver_path))
# driver.maximize_window()

for cryn in yado_number:
        yad_number = format(cryn['宿番号'], '06') if cryn['宿番号'] is not None else print('')
        plan_code = format(cryn['プランCD'], '08') if cryn['プランCD'] is not None else print('')
        room_code = format(cryn['部屋タイプCD'], '07') if cryn['部屋タイプCD'] is not None else print('')
        print('yad_number: ' + str(yad_number))
        print('plan_code: ' + str(plan_code))
        print('room_code: ' + str(room_code))

        rc_URL = f'https://www.jalan.net/uw/uwp3200/uww3201init.do?roomCrack=200000&stayYear=&stayMonth=&stayDay=&roomCount=1&adultNum=2&rootCd=04&distCd=01&stayCount=1&screenId=UWW1402&yadNo={yad_number}&planCd={plan_code}&roomTypeCd={room_code}&pageListNumPlan=44_1_1_1&callbackHistFlg=1&ccnt=yadlist_cp_n_0_sale_n_0_pp_n_0'
        driver.get(rc_URL)
        driver.implicitly_wait(3)

        try:
                number_element = driver.find_element(By.CSS_SELECTOR, "li.jlnpc-yado__notify--inn-reserved em")
                print(f"取得成功: 予約数 {number_element.text.strip()} 名")
        except Exception as e:
                number_element = None
                print("予約数が取得できませんでした")

        reservation_count = 0
        if number_element is not None:
                reservation_count = number_element.text.strip()

                d = {'宿番号': yad_number, 'エリア名': cryn['エリア名'], '宿名': cryn['宿名'], '予約件数':reservation_count, 'プランCD': plan_code, '部屋タイプCD': room_code, '予約日': str(reservation_date)}
                print(d)

                if d['予約件数'] is not None:
                        res_count.append(d)

driver.quit()

n_records = len(res_count)

print('予約件数' + str(n_records) + '件取得終了')
print('------------------------------------------------')

if config['settings']['csv_download']:
        csv_path = os.path.join(base_path, 'reservation_count_crawling.csv')
        field_name = ['宿番号', 'エリア名', '宿名', '予約件数', 'プランCD', '部屋タイプCD', '予約日']
        with open(csv_path, 'w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames= field_name)
                writer.writeheader()
                writer.writerows(res_count)


if config['settings']['db_import']:
        ordered_keys = [
        *config['mappings']['reservation_counts']['string'].keys(),
        *config['mappings']['reservation_counts']['integer'].keys(),
        *config['mappings']['reservation_counts']['date'].keys()
        ]

        conn = psycopg2.connect(
        host=config['db']['host'],
        port=config['db']['port'],
        user=config['db']['user'],
        password=config['db']['password'],
        database=config['db']['database']
        )

        mapping_keys = ', '.join(ordered_keys)
        table_name = 'crawling_reservation_counts'
        table_insert_query = f'INSERT INTO {table_name}({mapping_keys}) VALUES %s'

        try:
                cursor = conn.cursor()
                buf = []
                for row in res_count:
                        record = make_record_from_row(row, config)         
                        buf.append([record[k] for k in ordered_keys])
                        
                extras.execute_values(cursor, table_insert_query, buf)
                conn.commit()           
                post_webhook(config, f'予約数クローリング: DBにインポートしました。 (レコード数: {n_records})', 'success')

        except Exception as ex:
                msg = traceback.format_exc()
                post_webhook(config, f'予約数クローリング: インポート実行中にエラー: {msg}', 'error')
                print(msg)
                conn.rollback()
                exit(1)
