# -*- coding: utf-8 -*-
import sys, os, logging, pickle
import boto3 # athena 필요
import time, math # time.sleep 사용
from datetime import datetime
import pymysql

# mysql 정보
with open('dbinfo.pickle', 'rb') as f:
    data = pickle.load(f)

# 계정 정보
for key in data.keys():
    globals()[key] = data[key]

try:
    conn = pymysql.connect(host, user=username, passwd=password, db=database, port=port, use_unicode=True, charset='utf8')
    cursor = conn.cursor()
except:
    logging.error("could not connect to rds")
    sys.exit(1)

athena = boto3.client('athena')


# 정규화 계산 함수
def normalize(x, x_min, x_max):

    normalized = (x-x_min) / (x_max-x_min)
    return normalized

# Athena에서 사용할 데이터 configure 하는 함수
def query_athena(query, athena):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': 'production'
        },
        ResultConfiguration={ # 저장 위치
            'OutputLocation': "s3://athena-panomix-tables-matt/repair/", # response 저장하는 버킷 생성해서 거기다 저장
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3'
            }
        }
    )

    return response

# Athena 쿼리 결과(response) 반환하는 함수. response는 s3에 저장
def get_query_result(query_id, athena):

    response = athena.get_query_execution(
        QueryExecutionId=str(query_id)
    )

    # 쿼리가 완료될 때까지 충분한 시간 대기
    while response['QueryExecution']['Status']['State'] != 'SUCCEEDED':
        if response['QueryExecution']['Status']['State'] == 'FAILED':
            logging.error('QUERY FAILED')
            break
        time.sleep(5) # 데이터의 양을 보면, Athena에서 처리 시간을 통해 어느 정도 걸리는지 알 수 있음 -> 5초
        response = athena.get_query_execution(
            QueryExecutionId=str(query_id)
        )

    response = athena.get_query_results(
        QueryExecutionId=str(query_id),
        MaxResults=1000 # Athena는 MaxResults가 1000
    )

    return response


# 내가 수정한 코드. 실행 시간은 큰 차이 안 남
def process_data(results):

    data = results['ResultSet']
    columns = [col['VarCharValue'] for col in data['Rows'][0]['Data']]
    # columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]

    listed_results = []
    for row in data['Rows'][1:]: # 행별로 저장
        values = []
        for col in row['Data']:
            try:
                values.append(col['VarCharValue']) # 각 칼럼의 값들이 {'VarCharValue': value} 형식
            except: # null일 경우?
                values.append('')
        listed_results.append(dict(zip(columns, values)))

    return listed_results

# mysql의 table에 데이터 insert 하는 함수
def insert_row(cursor, data, table):

    placeholders = ', '.join(['%s'] * len(data))
    columns = ', '.join(data.keys())
    key_placeholders = ', '.join(['{0}=%s'.format(k) for k in data.keys()])
    # 기본적으로 insert 하되, 키가 같으면 update
    sql = "INSERT INTO %s ( %s ) VALUES ( %s ) ON DUPLICATE KEY UPDATE %s" % (table, columns, placeholders, key_placeholders)
    cursor.execute(sql, list(data.values())*2) # *2는 왜 들어간 거지?

############################

def main():
    start = time.time()
    
    # 1. top_tracks 데이터 업데이트
    query = """
        create external table if not exists top_tracks(
        id string,
        artist_id string,
        name string,
        album_name string,
        popularity int,
        image_url string
        ) partitioned by (dt string)
        stored as parquet location 's3://spotify-artists-matt/top-tracks' tblproperties("parquet.compress" = "snappy")
    """
    r = query_athena(query, athena)

    if r['ResponseMetadata']['HTTPStatusCode'] == 200:
        query = 'msck repair table top_tracks'
        r = query_athena(query, athena)
        if r['ResponseMetadata']['HTTPStatusCode'] == 200:
            result = get_query_result(r['QueryExecutionId'], athena)
            print('top_tracks partition update!') # 신규 파티션 생성 

    # 2. audio_features 데이터 업데이트
    query = """
        create external table if not exists audio_features(
        duration_ms int,
        key int,
        mode int,
        time_signature int,
        acousticness double,
        danceability double,
        energy double,
        instrumentalness double,
        liveness double,
        loudness double,
        speechiness double,
        valence double,
        tempo double,
        id string
        ) partitioned by (dt string)
        stored as parquet location 's3://spotify-artists-matt/audio-features' tblproperties("parquet.compress" = "snappy")
    """
    r = query_athena(query, athena)

    if r['ResponseMetadata']['HTTPStatusCode'] == 200:
        query = 'msck repair table audio_features'
        r = query_athena(query, athena)
        if r['ResponseMetadata']['HTTPStatusCode'] == 200:
            result = get_query_result(r['QueryExecutionId'], athena)
            print('audio_features partition update!') # 신규 파티션 생성 


    # 3. 아티스트별 평균 수치 계산. 최근 날짜 데이터 사용
    # 근데 평균으로 하면 잃어버리는 정보가 너무 많은데, 더 좋은 방법 없나? 트랙 각각 수치 살릴 수 있는.
    query = """
        SELECT
            artist_id,
            avg(danceability) as danceability,
            avg(energy) as energy,
            avg(loudness) as loudness,
            avg(speechiness) as speechiness,
            avg(acousticness) as acousticness,
            avg(instrumentalness) as instrumentalness
        FROM
            top_tracks t1
        JOIN
            audio_features t2 on t2.id = t1.id
        WHERE
            t1.dt = (select max(dt) from top_tracks)
            and t2.dt = (select max(dt) from audio_features)
        GROUP BY
            t1.artist_id
    """

    r = query_athena(query, athena)
    results = get_query_result(r['QueryExecutionId'], athena)
    artists = process_data(results)


    # 정규화 위해 수치별 최대, 최소값 계산. 가장 최근 날짜 데이터 사용
    query = """
        SELECT
            MIN(danceability) AS danceability_min,
            MAX(danceability) AS danceability_max,
            MIN(energy) AS energy_min,
            MAX(energy) AS energy_max,
            MIN(loudness) AS loudness_min,
            MAX(loudness) AS loudness_max,
            MIN(speechiness) AS speechiness_min,
            MAX(speechiness) AS speechiness_max,
            ROUND(MIN(acousticness),4) AS acousticness_min,
            MAX(acousticness) AS acousticness_max,
            MIN(instrumentalness) AS instrumentalness_min,
            MAX(instrumentalness) AS instrumentalness_max
        FROM
            audio_features
        WHERE
            dt = (select max(dt) from audio_features)
    """

    r = query_athena(query, athena)
    results = get_query_result(r['QueryExecutionId'], athena)
    avgs = process_data(results)[0]

    metrics = ['danceability', 'energy', 'loudness', 'speechiness', 'acousticness', 'instrumentalness']

    for i in range(len(artists)):
        dist_min = 500 # 최소값 갱신을 위해 충분히 큰 값을 초기값으로 설정

        # idx += 1
        temp = artists.copy() # temp: 자기 자신 뺀 것.
        mine = temp.pop(i) # mine: 자기 자신.
        for other in temp:
            dist = 0
            for m in metrics:
                # mine과 other 간 거리 계산
                x = float(mine[m])
                x_norm = normalize(x, float(avgs[m + '_min']), float(avgs[m + '_max']))
                y = float(other[m])
                y_norm = normalize(y, float(avgs[m + '_min']), float(avgs[m + '_max']))
                dist += math.sqrt((x_norm - y_norm)**2)

            # 해당 거리가 최소값보다 작으면, 데이터를 갱신. 최종 데이터를 삽입
            # 날짜는 삽입 당시 timestamp로 넣도록 테이블에서 설정해 놓았으므로, 신경 쓰지 않아도 됨.
            # 근데 날짜/시간도 업데이트 되도록 해야 할 것 같은데?? 안 되는 듯.
            # 같은 아티스트가 중복 등록되어 있어서 거리가 0인 경우가 있음!! 이 경우는 제외하기(sg 워너비, 윤도현)
            if dist < dist_min and dist != 0: 
                temp = {
                    'artist_id': mine['artist_id'],
                    'y_artist': other['artist_id'],
                    'distance': dist
                }
                dist_min = dist
        # 이렇게 각각 삽입 말고, 한번에 할 수 있나? df.to_sql?
        insert_row(cursor, temp, 'related_artists')
        # data.append(temp)

    conn.commit()
    print('related_artists 테이블 삽입 완료!')
    print("실행 시간: {}s".format(round(time.time() - start, 1)))


if __name__ == "__main__":
    main()