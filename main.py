from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
import datetime
import pymysql
from pymysql import IntegrityError
from DONGA import donga
from joongang import joongang


def insert_articles():

    print('크롤링 시작')
    conn = pymysql.connect(host='127.0.0.1', user='root', password='0000', db='taba', charset='utf8mb4')
    cur = conn.cursor()

    commit_cnt = 0
    data_list = [[] for _ in range(0)]

    data_list += donga()
    data_list += joongang()

    data_list = sorted(data_list, key=lambda x: x[0])
    for data in data_list:
        try:
            # DB에 저장
            sql = ('INSERT INTO NEWS_ARTICLES(DATE, TITLE, CONTENT, PRESS, URL, IMG_URL) '
                   'VALUES(%s, %s, %s, %s, %s, %s)')
            cur.execute(sql, data)
            commit_cnt += 1

        except IntegrityError as e:
            if e.args[0] == 1062:  # MySQL 에러 코드 1062인 경우 처리
                continue

        conn.commit()
    print(f'{commit_cnt}개 커밋 완료')

    conn.close()

"""
def main():

    scheduler = BlockingScheduler()

    trigger = IntervalTrigger(hours=1, start_date=datetime.datetime.now())
    scheduler.add_job(insert_articles, trigger=trigger)

    scheduler.start()



if __name__ == '__main__':
    main()
"""

insert_articles()