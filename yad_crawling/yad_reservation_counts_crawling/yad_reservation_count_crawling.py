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
from urllib.parse import urlparse, parse_qs
import re

DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
}

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


def build_session(config):
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    override_headers = config.get('settings', {}).get('http_headers', {})
    if override_headers:
        session.headers.update(override_headers)
    return session


def fetch_soup(session, url):
    response = session.get(url, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    title = soup.title.get_text(strip=True) if soup.title else ''
    page_text = soup.get_text(" ", strip=True)
    if any(k in page_text for k in ['アクセスを制限', '不正アクセス', 'Access Denied', 'captcha', 'reCAPTCHA']):
        print('警告: アクセス制限の可能性があります')
        print('URL: ' + url)
        print('title: ' + title)
    return soup


def get_text_or_empty(element):
    if element is None:
        return ''
    return element.get_text(strip=True)


def parse_query_params(url):
    if not url:
        return {}
    query = urlparse(url).query
    return parse_qs(query)


def normalize_code(value, length):
    if value is None:
        return None
    s = str(value).strip()
    if not s.isdigit():
        return None
    return s.zfill(length)


def extract_facility_code(raw_url):
    if not raw_url:
        return None
    s = str(raw_url)
    # /yad123456/, ...?yadNo=123456 などを許容
    m = re.search(r'yad(?:No=)?(\d{6})', s, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r'(\d{6})', s)
    if m:
        return m.group(1)
    return None


def get_base_path():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(__file__)


def load_config():
    base_path = get_base_path()
    config_path = os.path.join(base_path, 'config.yaml')
    with open(config_path, 'r', encoding='utf-8') as fp:
        config = yaml.safe_load(fp)
    return base_path, config


def parse_count_text(raw_text):
    if raw_text is None:
        return 0
    digits = ''.join(re.findall(r'\d+', str(raw_text)))
    return int(digits) if digits else 0


def build_prefecture_area_targets(config):
    """
    新形式:
      code:
        prefectures:
          兵庫県:
            ken_code: '280000'
            area_code:
              城崎・竹野・豊岡: '281100'
              ...

    旧形式(後方互換):
      code:
        ken_code:
          兵庫県: '280000'
        area_code:
          城崎・竹野・豊岡: '281100'
          ...
    """
    targets = []
    prefectures_map = config.get('code', {}).get('prefectures', {})
    ken_code_map = config.get('code', {}).get('ken_code', {})

    if prefectures_map:
        source_map = prefectures_map
    else:
        source_map = ken_code_map

    for prefecture_name, prefecture_value in source_map.items():
        if isinstance(prefecture_value, dict):
            prefecture_code_raw = prefecture_value.get('ken_code') or prefecture_value.get('code')
            area_code_map = prefecture_value.get('area_code', {})
        else:
            prefecture_code_raw = prefecture_value
            area_code_map = config.get('code', {}).get('area_code', {})

        prefecture_code = normalize_code(prefecture_code_raw, 6)
        if prefecture_code is None:
            print(f'警告: 都道府県コードが不正です: {prefecture_name} / {prefecture_code_raw}')
            continue

        for area_name, area_code_raw in area_code_map.items():
            area_code = normalize_code(area_code_raw, 6)
            if area_code is None:
                print(f'警告: エリアコードが不正です: {prefecture_name} / {area_name} / {area_code_raw}')
                continue

            targets.append({
                'prefecture_name': prefecture_name,
                'prefecture_code': prefecture_code,
                'area_name': area_name,
                'area_code': area_code
            })

    return targets

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding = 'utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding = 'utf-8')

base_path, config = load_config()


today_date = str(datetime.date.today())
reservation_date = datetime.date.today() + datetime.timedelta(days=-1)

yado_number = []
yado_seen = set()
yad_plan_map = {}
res_count = []

session = build_session(config)

#ページ数取得
print('宿番号取得開始')
targets = build_prefecture_area_targets(config)
print('対象エリア数: ' + str(len(targets)))
for target in targets:
        prefecture_name = target['prefecture_name']
        prefecture_code = target['prefecture_code']
        area_code = target['area_code']
        area_name = target['area_name']
        print('エリアCD: ' + str(area_code))
        mainURL = f'https://www.jalan.net/{prefecture_code}/LRG_{area_code}/?stayYear=&stayMonth=&stayDay=&dateUndecided=1&stayCount=1&roomCount=1&adultNum=2&ypFlg=1&kenCd={prefecture_code}&screenId=UWW1380&roomCrack=200000&lrgCd={area_code}&distCd=01&rootCd=04&yadRk=1&yadHb=1'
        soup = fetch_soup(session, mainURL)
        elems_yad_count = soup.find_all(class_='jlnpc-listInformation--count')

        #宿数取得（旅館0の場合pass）
        if elems_yad_count != []:
                yado_count_text = elems_yad_count[0].get_text(strip=True)
                yado_count = parse_count_text(yado_count_text)
                page_count = math.ceil(yado_count / 30) if yado_count > 0 else 0
                print('宿件数テキスト: ' + str(yado_count_text) + ' / 解析値: ' + str(yado_count))

                #ページごとに宿番号取得
                for i in range(1, page_count+1):
                        page_URL = f'https://www.jalan.net/{prefecture_code}/LRG_{area_code}/page{i}.html?screenId=UWW1402&distCd=01&activeSort=0&mvTabFlg=1&rootCd=04&stayYear=&stayMonth=&stayDay=&stayCount=1&roomCount=1&dateUndecided=1&adultNum=2&roomCrack=200000&kenCd={prefecture_code}&lrgCd={area_code}&vosFlg=6&idx={(i-1)*30}&yadRk=1&yadHb=1'
                        soup_page = fetch_soup(session, page_URL)
                        elems_yad_num = soup_page.find_all(class_='jlnpc-yadoCassette__link')
                        elems_yad_name = soup_page.find_all('h2', class_='p-searchResultItem__facilityName')
                        elems_yad_url = soup_page.find_all(class_='p-searchResultItem__planName')

                        #data-href属性で宿番号を取得、リストに追加
                        for idx, element1 in enumerate(elems_yad_num):
                                data_href = element1.get('data-href') or ''
                                href_values = extract_facility_code(data_href)
                                element2 = elems_yad_name[idx] if idx < len(elems_yad_name) else None
                                facility_name = get_text_or_empty(element2)
                                if href_values is not None and str.isdigit(href_values):
                                        d = {'都道府県CD': prefecture_code, '都道府県': prefecture_name, 'エリアCD': area_code, 'エリア名': area_name,'宿番号': href_values, '宿名': facility_name, 'プランCD':None, '部屋タイプCD':None,'掲載ページ':i}
                                        print(d)
                                        if (d['宿番号'] is not None or d['宿名'] is not None) and d['宿番号'] not in yado_seen:
                                                yado_number.append(d)
                                                yado_seen.add(d['宿番号'])
                        if len(elems_yad_num) != len(elems_yad_name):
                                print('警告: 宿番号要素数と宿名要素数が不一致です: 宿番号=' + str(len(elems_yad_num)) + ' 宿名=' + str(len(elems_yad_name)))

                        #href属性でURLから宿番号とプランCDを取得、リストに追加

                        for element in elems_yad_url:
                                href_values = element.get('href') or ''
                                params = parse_query_params(href_values)
                                facility_code = normalize_code(params.get('yadNo', [None])[0], 6)
                                plan_code = normalize_code(params.get('planCd', [None])[0], 8)
                                room_code = normalize_code(params.get('roomTypeCd', [None])[0], 7)
                                print('宿番号：' + str(facility_code))
                                print('プランCD：' + str(plan_code))
                                print('部屋タイプCD：' + str(room_code))
                                if facility_code and plan_code and room_code:
                                        d2 = {'宿番号': facility_code, 'プランCD': plan_code, '部屋タイプCD': room_code}
                                        print(d2)
                                        if facility_code not in yad_plan_map:
                                                yad_plan_map[facility_code] = d2
        else:
                pass
print('宿番号' + str(len(yado_number)) + '件取得終了')
print('------------------------------------------------')

missing_plan_count = 0
for x in yado_number:
        plan_info = yad_plan_map.get(x['宿番号'])
        if plan_info:
                x['プランCD'] = plan_info['プランCD']
                x['部屋タイプCD'] = plan_info['部屋タイプCD']
        else:
                missing_plan_count += 1

print('プラン/部屋タイプ未取得件数: ' + str(missing_plan_count))


print('予約件数取得開始')

driver_path = config['settings']['driver_path']
options = Options()
if config.get('settings', {}).get('headless', True):
        options.add_argument('--headless=new')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--disable-gpu')
driver = webdriver.Chrome(service=ChromeService(driver_path), options=options)
# driver.maximize_window()

for cryn in yado_number:
        yad_number = normalize_code(cryn['宿番号'], 6)
        plan_code = normalize_code(cryn['プランCD'], 8)
        room_code = normalize_code(cryn['部屋タイプCD'], 7)
        if not (yad_number and plan_code and room_code):
                print('スキップ: 宿番号/プランCD/部屋タイプCDが不足しています: ' + str(cryn))
                continue
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
