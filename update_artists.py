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

    # data: {"key1": value1, "key2": value2} 형태
    # sql에는 update할 값이 key=value, key=value, key=value 형태로 들어가야 함
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

    # 1. RDS - 아티스트 데이터 가져옴
    cursor.execute("SELECT id FROM artists")

    # 2. artists API에서 popularity, followers 가져와 RDS - 아티스트 테이블에 업데이트
    ids = []
    for (artist_id,) in cursor.fetchall():
        ids.append(artist_id)

    for i in range(0, len(ids), 50):
        # 데이터 할당. 최대 50개씩 쿼리할 수 있기 때문에, 50개씩 나누기
        part = ids[i:i+50]

        # API 쿼리
        URL = "https://api.spotify.com/v1/artists/"
        params = {
            'ids': part
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

        # 100번째 아티스트까지는 25초 걸림
        print("{}번째 아티스트 업데이트. 실행 시간: {}s".format(i+50, round(time.time() - start, 1)))
    
    # 이렇게 하면 총 5초 걸림!!
        
    conn.commit()

    # for (artist_id, ) in cursor.fetchall():
    #     i += 1
    #     URL = "https://api.spotify.com/v1/artists/{}".format(artist_id)
        
    #     # Get Several Artists에 필요한 query parameter
    #     # params = {
    #     #     'ids': 'US'
    #     # }

    #     headers = get_headers(client_id, client_secret)
    #     # r = requests.get(URL, params=params, headers=headers)
    #     r = requests.get(URL, headers=headers)
    #     raw = json.loads(r.text)

    #     artist = {}
    #     artist.update(
    #         {
    #             'followers': raw['followers']['total'],
    #             'popularity': raw['popularity'],
    #             'url': raw['external_urls']['spotify']
    #         }
    #     )
    #     if raw['images']:
    #         artist.update(
    #             {'image_url': raw['images'][0]['url']}
    #         )

    #     update_row(cursor, artist, 'artists', artist_id)

    #     # 100번째 아티스트까지는 25초 걸림
    #     if i % 50 == 0:
    #         print("{}번째 아티스트 업데이트. 실행 시간: {}s".format(i, round(time.time() - start, 1)))
    #         # break
    #     if i == 300:
    #         break # 테스트

    # conn.commit()

    print("artists table update complete!")
    print("실행 시간: {}s".format(round(time.time() - start, 1)))
    # 총 965초. 한 아티스트당 평균 1초가 넘게 걸림
    # 어디서 보틀넥이 걸리는 거지? Get Several Artists API를 이용해 볼까. 50개씩 가능
    # bulk로 update 하는 쿼리하면 빠른가?

if __name__=='__main__':
    main()
