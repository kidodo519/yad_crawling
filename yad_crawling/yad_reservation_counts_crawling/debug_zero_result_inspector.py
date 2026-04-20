import datetime
import io
import os
import requests
import yaml
from bs4 import BeautifulSoup

DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
}

def build_session(config):
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    override_headers = config.get('settings', {}).get('http_headers', {})
    if override_headers:
        session.headers.update(override_headers)
    return session


def dump_response(debug_dir, file_prefix, response):
    os.makedirs(debug_dir, exist_ok=True)
    html_path = os.path.join(debug_dir, f'{file_prefix}.html')
    meta_path = os.path.join(debug_dir, f'{file_prefix}.meta.txt')

    with open(html_path, 'wb') as fp:
        fp.write(response.content)

    soup = BeautifulSoup(response.content, "html.parser")
    title = soup.title.get_text(strip=True) if soup.title else ''
    count_nodes = len(soup.select('.jlnpc-listInformation--count'))
    yad_nodes = len(soup.select('.jlnpc-yadoCassette__link'))
    plan_nodes = len(soup.select('.p-searchResultItem__planName'))
    preview = soup.get_text(' ', strip=True)[:500]

    with open(meta_path, 'w', encoding='utf-8') as fp:
        fp.write(f'status_code={response.status_code}\n')
        fp.write(f'final_url={response.url}\n')
        fp.write(f'title={title}\n')
        fp.write(f'count_nodes={count_nodes}\n')
        fp.write(f'yad_nodes={yad_nodes}\n')
        fp.write(f'plan_nodes={plan_nodes}\n')
        fp.write(f'preview={preview}\n')
        fp.write('response_headers:\n')
        for k, v in response.headers.items():
            fp.write(f'  {k}: {v}\n')

    print(f'保存完了: {html_path}')
    print(f'保存完了: {meta_path}')
    print(f'status={response.status_code} title={title}')
    print(f'count_nodes={count_nodes} yad_nodes={yad_nodes} plan_nodes={plan_nodes}')


def main():
    import sys
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

    base_path = os.path.dirname(__file__)
    config_path = os.path.join(base_path, 'config.yaml')
    with open(config_path, 'r', encoding='utf-8') as fp:
        config = yaml.safe_load(fp)

    debug_dir = os.path.join(base_path, 'debug_logs', datetime.datetime.now().strftime('%Y%m%d_%H%M%S'))
    session = build_session(config)

    print('=== 0件検証デバッグ開始 ===')
    for ac in list(config['code']['area_code']):
        prefecture_name = list(config['code']['ken_code'].keys())[0]
        prefecture_code = format(config['code']['ken_code'][prefecture_name], '06')
        area_code = format(config['code']['area_code'][ac], '06')
        main_url = f'https://www.jalan.net/{prefecture_code}/LRG_{area_code}/?stayYear=&stayMonth=&stayDay=&dateUndecided=1&stayCount=1&roomCount=1&adultNum=2&ypFlg=1&kenCd={prefecture_code}&screenId=UWW1380&roomCrack=200000&lrgCd={area_code}&distCd=01&rootCd=04&yadRk=1&yadHb=1'
        print(f'取得中 area={ac}({area_code})')
        response = session.get(main_url, timeout=30)
        dump_response(debug_dir, f'area_{area_code}_main', response)

    print('=== 0件検証デバッグ終了 ===')
    print('出力先: ' + debug_dir)


if __name__ == '__main__':
    main()
