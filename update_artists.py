# -*- coding: utf-8 -*-
# 수동으로 artists 데이터를 업데이트하는 스크립트
# 기존 아티스트에 대해 popularity, followers를 업데이트하기 위함
import sys, os, logging
import boto3 # aws python library
import requests
import base64
import json, pickle
import pymysql
from datetime import datetime
import time

# api 사용 정보, db 정보 가져오기
with open('dbinfo.pickle', 'rb') as f:
    data = pickle.load(f)

for key, value in data.items():
    globals()[key] = value

    
def update_row(cursor, data, table, artist_id):

    # data: {"column1": value1, "column2": value2} 형태
    # sql에는 update할 값이 column=value, column=value, column=value 형태로 들어가야 함
    values = []
    for k,v in data.items():
        if type(v) == int:
            values.append('{}={}'.format(k, v))
        elif type(v) == str:
            values.append("{}='{}'".format(k, v))
    key_values = ', '.join(values)

    sql = "UPDATE %s SET %s WHERE id = '%s'" % (table, key_values, artist_id)
    # print(sql) # 아래와 같은 형태
    """
    UPDATE artists
    SET followers=123, popularity=89, ...
    WHERE id = 'id'
    """
    cursor.execute(sql)

def get_headers(client_id, client_secret):

    endpoint = "https://accounts.spotify.com/api/token"
    encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')

    headers = {
        "Authorization": "Basic {}".format(encoded)
    }

    payload = {
        "grant_type": "client_credentials"
    }

    r = requests.post(endpoint, data=payload, headers=headers)

    access_token = json.loads(r.text)['access_token']

    headers = {
        "Authorization": "Bearer {}".format(access_token)
    }

    return headers


def main():
    start = time.time()

    # connect MySQL
    try:
        conn = pymysql.connect(host, user=username, passwd=password, db=database, port=port, use_unicode=True, charset='utf8')
        cursor = conn.cursor()
    except:
        logging.error("could not connect to rds")
        sys.exit(1)

    headers = get_headers(client_id, client_secret)

    ### fetch random artists 테스트
    # URL = "https://api.spotify.com/v1/search"
    # params = {
    #     'q': 'year:0000-9999',
    #     'type': 'artist'
    # }

    # for i in range(5):
    #     headers = get_headers(client_id, client_secret)
    #     r = requests.get(URL, params=params, headers=headers)
    #     raw = json.loads(r.text)
    #     # print(len(raw['artists']))
    #     # print(raw['artists'].keys())
    #     temp = [data['name'] for data in raw['artists']['items']]
    #     print(len(temp))
    #     print(temp)
    #     # break
    # sys.exit(0)

    ###

    # 1. RDS - 아티스트 데이터 가져옴
    cursor.execute("SELECT id FROM artists")

    # 2. artists API에서 popularity, followers 가져와 RDS - 아티스트 테이블에 업데이트
    ids = []
    for (artist_id,) in cursor.fetchall():
        ids.append(artist_id)

    for i in range(0, len(ids), 50):
        # Get Several Artists API를 이용하여 50개씩 쿼리(최대 50개씩 쿼리할 수 있음)
        # 데이터 할당(50개)
        part = ids[i:i+50]

        # API 쿼리
        URL = "https://api.spotify.com/v1/artists"
        params = {
            'ids': ','.join(part) # comma separated 형식으로 주어야 함
        }

        headers = get_headers(client_id, client_secret)
        r = requests.get(URL, params=params, headers=headers)
        raw = json.loads(r.text)['artists']

        # 아티스트별로 테이블에 업데이트
        for data in raw:

            artist = {}
            artist.update(
                {
                    'followers': data['followers']['total'],
                    'popularity': data['popularity'],
                    'url': data['external_urls']['spotify']
                }
            )
            if data['images']:
                artist.update(
                    {'image_url': data['images'][0]['url']}
                )

            update_row(cursor, artist, 'artists', data['id'])

        print("{}번째 아티스트 업데이트. 실행 시간: {:.1f}s".format(i+50, time.time() - start))


    conn.commit()

    print("artists table update complete!")
    print("실행 시간: {:.1f}s".format(time.time() - start))
    # 총 18초 소요
    

if __name__=='__main__':
    main()
