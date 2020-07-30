# -*- coding: utf-8 -*-
# top_tracks, audio_features 데이터를 매일 s3에 저장하는 코드
import sys, os, logging, time
import boto3
import requests
import base64
import json, pickle
import pymysql
from datetime import datetime
import pandas as pd # 데이터를 parquet화
import jsonpath  # pip3 install jsonpath --user

# api 사용 정보, db 정보 가져오기
with open('dbinfo.pickle', 'rb') as f:
    data = pickle.load(f)

for key, value in data.items():
    globals()[key] = value


def main():
    start = time.time()

    # connect MySQL
    try:
        conn = pymysql.connect(host, user=username, passwd=password, db=database, port=port, use_unicode=True, charset='utf8')
        cursor = conn.cursor()
    except:
        logging.error("could not connect to rds")
        sys.exit(1)

    # connect DynamoDB
    try:
        dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-2', endpoint_url='http://dynamodb.ap-northeast-2.amazonaws.com')
    except:
        logging.error('could not connect to dynamodb')
        sys.exit(1)

    headers = get_headers(client_id, client_secret)

    # 1. RDS - 아티스트 ID를 가져옴. 일단 10개 테스트
    # cursor.execute("SELECT id FROM artists LIMIT 10")
    cursor.execute("SELECT id, name FROM artists")
    print("1. RDB scan completed!")

    # 2-1. top_tracks

    # 먼저 DynamoDB 전체 데이터 삭제
    # 전체 삭제보다는, 없는 건 추가하고 있는 건 업데이트
    # 업데이트 기준: 0-2번 탑 트랙이 변경되었을 경우?
    table = dynamodb.Table('top_tracks3')
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan['Items']:
            batch.delete_item(Key={
                'artist_id': item['artist_id'],
                'id': item['id']
            })

    # jsonpath 패키지 이용하여, 원하는 value들만 가져오도록 key path를 설정.
    top_track_keys = {
        "id": "id", # track id
        "name": "name",
        "popularity": "popularity",
        "external_url": "external_urls.spotify",
        "album_name": "album.name",
        "image_url": "album.images[1].url"
        # url, image_url은 nested 구조. 안의 flat한 값만 가져 옴
    }

    top_tracks = []

    # 한 행씩 처리
    for (artist_id, name) in cursor.fetchall():

        URL = "https://api.spotify.com/v1/artists/{}/top-tracks".format(artist_id)
        params = {
            'country': 'US'
        }

        r = requests.get(URL, params=params, headers=headers)
        raw = json.loads(r.text)
        # top_tracks_dynamo.extend(raw['tracks'])
        #append와의 차이점: append는 단순히 추가. extend는 raw['tracks']의 element들(각 track)을 추가함
        
        for i in raw['tracks']: # i는 하나의 트랙

            # S3용 flatten data
            top_track = {}
            for k, v in top_track_keys.items():
                value = jsonpath.jsonpath(i, v) # [0]으로 리스트가 아닌 요소를 저장하면, 'bool' object is not subscriptable 에러 남
                if type(value) == bool:
                    # print(i, k, v, value) # 딱 한 개 있음(False)
                    continue # 결과 값이 없으면 False가 나옴. 이럴 경우 넘어감
                top_track.update({k: value}) # path(v)에 맞게 API에서 찾아 그 위치의 value를 가져옴
                top_track.update({'artist_id': artist_id}) # key 값을 위해 아티스트 id도 넣어줌
            top_tracks.append(top_track)

        # DynamoDB용 raw data
        # 근데 S3는 필요한 데이터만 있고, DynamoDB는 raw data?? 뭔가 이상한데. 아예 구조를 다시 생각해 볼까?

        resp = invoke_lambda('top-tracks', payload={
                'artist_name': name, # 로그 용도로 이름까지 보냄
                'artist_id': artist_id,
                'data': raw
            })
        # 프로비전 용량 초과로 오류 나면, 재시도?
        # 이러면 재귀 구문? 근데 여기서 재귀 말고, 일단 모두 요청 보낸 후 top-tracks 람다에서 재귀해야 할 듯
        # 이렇게 하면, StatusCode가 오기 전에 지나가 버려서 그런지, 아래 루프로 들어가지 않음.
        # print("status code:", resp['StatusCode'])

        # while resp['StatusCode'] not in [200, 202, 204]:
        #     print("{} 저장시 프로비전 용량 초과!".format(name))
        #     time.sleep(60)
        #     resp = invoke_lambda('top-tracks', payload={
        #         'artist_name': name, # 로그 용도로 이름까지 보냄
        #         'artist_id': artist_id,
        #         'data': raw
        #     })

        # print(resp) # 출력하기엔 횟수가 너무 많음
        

    # 2-2. json 으로 저장 (연습)
    # top_tracks는 list of dictionary 형태 -> json 데이터 저장하여 S3에 Load
    # with open('top_tracks.json', 'w') as f:
    #     for i in top_tracks:
    #         json.dump(i, f)
    #         f.write(os.linesep) # 한 줄씩 dict를 넣음

    # 2-2. parquet 형태로 저장
    # 뒤의 audio_features에 사용할 track_ids 변수 생성
    track_ids = [i['id'][0] for i in top_tracks] # jsonpath 사용하면 ['id'] 형태로 저장 -> [0]으로 벗겨야 함
    top_tracks = pd.DataFrame(top_tracks)
    # print(top_tracks.iloc[0]) # 첫 행 확인해 보기
    top_tracks.to_parquet('top-tracks.parquet', engine='pyarrow', compression='snappy')
    # pyarrow라는 엔진 사용(패키지 설치 필요)
    # raw data 그대로 가져오면, nested 값(키 안에 값이 아닌, 리스트 같은 struct 타입)을 저장하는 데 parquet이 문제가 있다고 뜸!
    # 따라서 가장 raw data는 json, 정제된 평평한 데이터만 parquet으로 사용해야 함.
    # compression은 압축 방식. 압축하여 저장 용량은 줄이고 parquet으로 퍼포먼스도 개선
    # -> top_tracks를 raw 그대로 말고 선택된 key들(top_track_keys)만 가져오면, 2차원 데이터이므로 오류 나지 않음

    # S3에 저장 - top-tracks 폴더
    s3 = boto3.resource('s3')
    dt = datetime.utcnow().strftime('%Y-%m-%d') # UTC 기준 현재 시간으로. "2020-03-23" 형태
    object = s3.Object('spotify-artists-matt', 'top-tracks/dt={}/top_tracks.parquet'.format(dt)) # 새로운 폴더(파티션)가 생성이 되는 것
    data = open('top-tracks.parquet', 'rb')
    object.put(Body=data)
    # dt는 datetime. 파티션 형식을 정의해 주는 것. 데이터를 쪼개서 스캔할 때 사용.
    # {} -> 현재 날짜별 폴더가 생성이 됨. 날짜별 파티션 만드는 이유: top_tracks가 매일 바뀔 수 있기 때문!!
    # 파티션은 형식을 만들어 놔야 Spark, Hadoop 등에서 파티션으로 인식함.

    print("2. top-tracks storage completed!")


    # 2-2. audio_features: batch 형식으로. 100개씩 저장
    tracks_batch = [track_ids[i: i+100] for i in range(0, len(track_ids), 100)]

    audio_features = []

    for i in tracks_batch:
        ids = ','.join(i) # API 호출에 맞는 형식(comma-separated list)
        URL = "https://api.spotify.com/v1/audio-features/?ids={}".format(ids)

        r = requests.get(URL, headers=headers)
        raw = json.loads(r.text) # audio_features는 flat한 구조라, raw data를 그대로 저장하면 됨.

        #audio_features 데이터는 nested 구조 없이 각 item들이 key-value 형식으로 되어 있음
        audio_features.extend(raw['audio_features'])
        # append와 extend 차이: extend는 리스트를 통째로 넣는 게 아니고, 리스트의 각 요소를 넣어 줌
        # [].extend([a,b,c]) -> [[a,b,c]]가 아니고 [a,b,c]가 됨

    audio_features = pd.DataFrame(audio_features)
    audio_features.to_parquet('audio-features.parquet', engine='pyarrow', compression='snappy')

    # S3에 저장 - audio_features 폴더
    s3 = boto3.resource('s3')
    dt = datetime.utcnow().strftime('%Y-%m-%d') # UTC 기준 현재 시간으로. "2020-03-23" 형태
    object = s3.Object('spotify-artists-matt', 'audio-features/dt={}/audio_features.parquet'.format(dt)) # 새로운 폴더(파티션)가 생성이 되는 것
    data = open('audio-features.parquet', 'rb')
    object.put(Body=data)

    print("3. audio-features storage completed!")
    print("실행 시간: {}s".format(round(time.time() - start, 1)))


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


def invoke_lambda(fxn_name, payload, invocation_type = 'Event'):
    # invocation_type: 동기식으로 하려면 RequestResponse
    lambda_client = boto3.client('lambda')
    invoke_response = lambda_client.invoke(
        FunctionName = fxn_name,
        InvocationType = invocation_type,
        Payload = json.dumps(payload)
    )

    if invoke_response['StatusCode'] not in [200, 202, 204]:
        logging.error('ERROR: Invoking lambda function: {} failed'.format(fxn_name))

    return invoke_response



if __name__=='__main__':
    main()
