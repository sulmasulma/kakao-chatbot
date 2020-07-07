import sys, os, logging
import boto3 # aws python library
import requests
import base64
import json, pickle
import pymysql
from datetime import datetime
import pandas as pd # 데이터를 parquet화
# import jsonpath  # pip3 install jsonpath --user

# api 사용 정보, db 정보 가져오기
with open('dbinfo.pickle', 'rb') as f:
    data = pickle.load(f)

for key, value in data.items():
    globals()[key] = value


def main():

    # connect MySQL
    try:
        conn = pymysql.connect(host, user=username, passwd=password, db=database, port=port, use_unicode=True, charset='utf8')
        cursor = conn.cursor()
    except:
        logging.error("could not connect to rds")
        sys.exit(1)

    headers = get_headers(client_id, client_secret)

    # 1. RDS - 아티스트 데이터 가져옴
    cursor.execute("SELECT * FROM artists")
    print("1. RDB scan completed!")

    # artists 데이터를 S3로 load
    colnames = [d[0] for d in cursor.description] # description 안에 열 이름이 있는 듯
    artists = [dict(zip(colnames, row)) for row in cursor.fetchall()] # artist들에 대한 데이터가 list of dictionaries로 저장
    artists = pd.DataFrame(artists)
    artists.to_parquet('artists.parquet', engine='pyarrow', compression='snappy')

    # S3에 저장 - top-tracks 폴더
    s3 = boto3.resource('s3')
    dt = datetime.utcnow().strftime('%Y-%m-%d')
    object = s3.Object('spotify-artists-matt', 'artists/dt={}/artists.parquet'.format(dt))
    data = open('artists.parquet', 'rb')
    object.put(Body=data)

    print("2. artists storage completed!")


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


if __name__=='__main__':
    main()
