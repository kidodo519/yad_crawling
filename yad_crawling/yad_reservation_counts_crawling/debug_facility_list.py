import csv
import datetime
import io
import os
import re
import sys
from urllib.parse import urlparse, parse_qs

import requests
import yaml
from bs4 import BeautifulSoup

DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
}


sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')


def get_base_path():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(__file__)


def load_config():
    base_path = get_base_path()
    config_path = os.path.join(base_path, 'config.yaml')
    print('設定ファイル読込: ' + config_path)
    with open(config_path, 'r', encoding='utf-8') as fp:
        config = yaml.safe_load(fp)
    return base_path, config


def normalize_code(value, length):
    if value is None:
        return None
    s = str(value).strip()
    if not s.isdigit():
        return None
    return s.zfill(length)


def parse_count_text(raw_text):
    if raw_text is None:
        return 0
    digits = ''.join(re.findall(r'\d+', str(raw_text)))
    return int(digits) if digits else 0


def extract_yado_count(soup):
    selectors = [
        '.jlnpc-listInformation--count',
        '.p-searchResultTitle__count',
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if node is None:
            continue
        text = node.get_text(strip=True)
        count = parse_count_text(text)
        if count > 0:
            return count, text
    return 0, ''


def build_prefecture_area_targets(config):
    targets = []
    prefectures_map = config.get('code', {}).get('prefectures', {})
    ken_code_map = config.get('code', {}).get('ken_code', {})

    source_map = prefectures_map if prefectures_map else ken_code_map

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
                'area_code': area_code,
            })

    return targets


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
    soup = BeautifulSoup(response.content, 'html.parser')
    title = soup.title.get_text(strip=True) if soup.title else ''
    page_text = soup.get_text(' ', strip=True)
    blocked = any(k in page_text for k in ['アクセスを制限', '不正アクセス', 'Access Denied', 'captcha', 'reCAPTCHA', '回線が混み合って'])
    return soup, title, blocked


def extract_facility_code(raw_url):
    if not raw_url:
        return None
    s = str(raw_url)
    m = re.search(r'yad(?:No=)?(\d{6})', s, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r'(\d{6})', s)
    if m:
        return m.group(1)
    return None


def parse_query_params(url):
    if not url:
        return {}
    query = urlparse(url).query
    return parse_qs(query)


def main():
    base_path, config = load_config()
    session = build_session(config)

    targets = build_prefecture_area_targets(config)
    print('対象エリア数: ' + str(len(targets)))

    facilities = []
    seen = set()

    for target in targets:
        prefecture_name = target['prefecture_name']
        prefecture_code = target['prefecture_code']
        area_name = target['area_name']
        area_code = target['area_code']

        main_url = (
            f'https://www.jalan.net/{prefecture_code}/LRG_{area_code}/'
            f'?stayYear=&stayMonth=&stayDay=&dateUndecided=1&stayCount=1&roomCount=1&adultNum=2'
            f'&ypFlg=1&kenCd={prefecture_code}&screenId=UWW1380&roomCrack=200000&lrgCd={area_code}'
            f'&distCd=01&rootCd=04&yadRk=1&yadHb=1'
        )
        soup, title, blocked = fetch_soup(session, main_url)

        if blocked:
            print(f'警告: 制限/混雑ページの可能性 エリア={area_name} URL={main_url} title={title}')

        yado_count, yado_count_text = extract_yado_count(soup)
        print(f'エリア={area_name} ({area_code}) 件数テキスト={yado_count_text} 解析件数={yado_count}')

        if yado_count <= 0:
            preview = soup.get_text(' ', strip=True)[:120]
            print(f'警告: 宿件数が取得できません エリア={area_name} title={title} preview={preview}')
            continue

        page_count = (yado_count + 29) // 30

        for page in range(1, page_count + 1):
            page_url = (
                f'https://www.jalan.net/{prefecture_code}/LRG_{area_code}/page{page}.html'
                f'?screenId=UWW1402&distCd=01&activeSort=0&mvTabFlg=1&rootCd=04&stayYear=&stayMonth=&stayDay='
                f'&stayCount=1&roomCount=1&dateUndecided=1&adultNum=2&roomCrack=200000&kenCd={prefecture_code}'
                f'&lrgCd={area_code}&vosFlg=6&idx={(page - 1) * 30}&yadRk=1&yadHb=1'
            )
            soup_page, page_title, page_blocked = fetch_soup(session, page_url)
            if page_blocked:
                print(f'警告: 一覧ページで制限/混雑の可能性 エリア={area_name} page={page} title={page_title}')

            elems_yad_num = soup_page.find_all(class_='jlnpc-yadoCassette__link')
            elems_yad_name = soup_page.find_all('h2', class_='p-searchResultItem__facilityName')
            elems_yad_url = soup_page.find_all(class_='p-searchResultItem__planName')

            plan_map = {}
            for element in elems_yad_url:
                href_values = element.get('href') or ''
                params = parse_query_params(href_values)
                facility_code = normalize_code(params.get('yadNo', [None])[0], 6)
                plan_code = normalize_code(params.get('planCd', [None])[0], 8)
                room_code = normalize_code(params.get('roomTypeCd', [None])[0], 7)
                if facility_code and plan_code and room_code and facility_code not in plan_map:
                    plan_map[facility_code] = {
                        'plan_code': plan_code,
                        'room_type_code': room_code,
                    }

            for idx, elem in enumerate(elems_yad_num):
                facility_code = extract_facility_code(elem.get('data-href') or '')
                if not facility_code or not facility_code.isdigit():
                    continue

                facility_name = ''
                if idx < len(elems_yad_name):
                    facility_name = elems_yad_name[idx].get_text(strip=True)

                key = (area_code, facility_code)
                if key in seen:
                    continue

                plan_info = plan_map.get(facility_code, {})
                facilities.append({
                    '取得日時UTC': datetime.datetime.utcnow().isoformat(timespec='seconds'),
                    '都道府県CD': prefecture_code,
                    '都道府県': prefecture_name,
                    'エリアCD': area_code,
                    'エリア名': area_name,
                    '宿番号': facility_code,
                    '宿名': facility_name,
                    'プランCD': plan_info.get('plan_code', ''),
                    '部屋タイプCD': plan_info.get('room_type_code', ''),
                    '一覧URL': page_url,
                })
                seen.add(key)

    output_path = os.path.join(base_path, 'debug_facility_list.csv')
    fields = ['取得日時UTC', '都道府県CD', '都道府県', 'エリアCD', 'エリア名', '宿番号', '宿名', 'プランCD', '部屋タイプCD', '一覧URL']
    with open(output_path, 'w', newline='', encoding='utf-8') as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        writer.writerows(facilities)

    print('宿番号リスト出力: ' + output_path)
    print('総件数: ' + str(len(facilities)))


if __name__ == '__main__':
    main()
