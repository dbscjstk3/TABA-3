from bs4 import BeautifulSoup
import requests
from datetime import datetime
import pymysql

def donga():
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

    for page in range(1,11,10):
        if already_data is True:
            break

        url = f'https://www.donga.com/news/Economy?p={page}&prod=news&ymd=&m='
        res = requests.get(url)

        for index in range(3,13):
            try:
                # 뉴스 리스트 URL 추출
                link = BeautifulSoup(res.text, 'html.parser').select_one(
                    f'#content > div.section_mid > div.section_con > div:nth-child({index}) > div.rightList > span.tit > a').get(
                    'href')
                resource = requests.get(link)
                soup = BeautifulSoup(resource.text, 'html.parser')

                # 추출한 URL에 들어가서 날짜 추출
                date_str = soup.select_one(
                    '#container > div.article_title > div.title_foot > span:nth-child(2)').get_text().strip(
                    "업데이트").strip() if soup.select_one(
                    '#container > div.article_title > div.title_foot > span:nth-child(2)') else ''

                date = datetime.strptime(date_str, '%Y-%m-%d %H:%M')

                # 이전 날짜 크롤링 중단
                if date < already_date:
                    already_data = True
                    break

                # 추출한 URL에 들어가서 제목 추출
                title = soup.select_one('#container > div.article_title > h1').get_text()


                # 이미지 추출
                try:
                    img_element = soup.select_one('#article_txt > div.articlePhotoC > span > img')
                    img_url = img_element['src'] if img_element else ''
                except Exception as e:
                    print(f"Image Error: {e}")
                    img_url = ''

                # 본문 추출
                content_tags = soup.select_one('#article_txt')
                for tag in content_tags.select(
                        'div.articlePhotoC, strong.sub_title, div.article_issue, div.view_ads01, div.article_footer'):
                    tag.decompose()
                content = content_tags.get_text().strip().replace("\n", "")

                # 크롤링 데이터 리스트
                insert_list = [date, title, content, '동아일보', link, img_url]
                data_list.append(insert_list)
            except Exception as e:
                print(f"Error: {e}")
                continue

    return data_list

donga()

