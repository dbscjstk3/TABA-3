import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pymysql

def joongang():
    # MySQL 연결
    conn = pymysql.connect(host='127.0.0.1', user='root', password='0000', db='taba', charset='utf8mb4')
    cur = conn.cursor()

    # 최신 날짜 불러오기
    date_sql = 'SELECT DATE FROM NEWS_ARTICLES ORDER BY DATE DESC LIMIT 1'
    cur.execute(date_sql)
    ck = cur.fetchone()
    if ck is not None:
        already_date = ck[0]
    else:
        already_date = datetime.min
    conn.close()

    # 크롤링 데이터 리스트
    data_list = []

    # 이전 데이터 크롤링 완료 체크
    already_data = False

    for page in range(1,3):
        if already_data is True:
            break

        url = f'https://www.joongang.co.kr/money?page={page}'
        res = requests.get(url)

        for index in range(1, 25):
            try:
                link = BeautifulSoup(res.text, 'html.parser').select_one(f'#story_list > li:nth-child({index}) > div > h2 > a').get('href')
                resource = requests.get(link)
                soup = BeautifulSoup(resource.text, 'html.parser')

                insert_time_element = soup.find('time', itemprop='datePublished')
                update_time_element = soup.find('time', itemprop='dateModified')

                if insert_time_element and update_time_element:  # 입력날짜와 업데이트 날짜가 둘 다 있는 경우
                    update_datetime_str = update_time_element['datetime']
                    update_to_datetime = datetime.strptime(update_datetime_str, '%Y-%m-%dT%H:%M:%S%z')
                    update_to_str = update_to_datetime.strftime('%Y-%m-%d %H:%M')
                    again_to_datetime = datetime.strptime(update_to_str, '%Y-%m-%d %H:%M')
                    date = again_to_datetime
                    if date < already_date:
                        already_data = True
                        break
                elif insert_time_element:  # 입력 날짜만 있는 경우
                    insert_datetime_str = insert_time_element['datetime']
                    insert_to_datetime = datetime.strptime(insert_datetime_str, '%Y-%m-%dT%H:%M:%S%z')
                    insert_to_str = insert_to_datetime.strftime('%Y-%m-%d %H:%M')
                    re_to_datetime = datetime.strptime(insert_to_str, '%Y-%m-%d %H:%M')
                    date = re_to_datetime
                    if date < already_date:
                        already_data = True
                        break
                else:
                    date = datetime.min

                title_element = soup.select_one('#container > section > article > header > h1')
                if title_element:
                    title = title_element.text.replace("\n", "")
                else:
                    title = ""

                img_url = ''
                try:
                    img_element = soup.select_one('#article_body > div.ab_photo.photo_center > div > img')
                    if img_element:
                        img_url = img_element['src']
                except Exception as e:
                    print(f"Image Error: {e}")

                contents = soup.select_one('#article_body')
                for tag in contents.select('div'):
                    tag.decompose()
                content = contents.text.replace("  ", "").strip().replace("\n","").replace("\xa0","")

                insert_list = [date, title, content, '중앙일보', link, img_url]
                data_list.append(insert_list)
            except Exception as e:
                print(f"Error: {e}")
                continue

    return data_list

joongang()







