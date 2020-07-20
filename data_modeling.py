# audio_features 가지고 아티스트들 간의 distance를 계산
# 프로세스 순서: Athena에서 데이터 가져옴 → mysql에 저장 → 이 테이블 가지고 서비스
# 수치들의 scale이 다름. loudness 이런건 0~1인데, tempo는 200보다 커지기도 함
import sys, os, logging, pickle
import boto3 # athena 필요
import time, math # time.sleep 사용
import pymysql

# mysql 정보
with open('dbinfo.pickle', 'rb') as f:
    data = pickle.load(f)

# 계정 정보
for key in data.keys():
    globals()[key] = data[key]


def main():

    # connect MySQL
    try:
        conn = pymysql.connect(host, user=username, passwd=password, db=database, port=port, use_unicode=True, charset='utf8')
        cursor = conn.cursor()
    except:
        logging.error("could not connect to rds")
        sys.exit(1)

    athena = boto3.client('athena')

    # 1. 아티스트별 avg 데이터
    # query: 모든 metric이 아닌, 필요한 것만 가져옴
    # 날짜 조건: 하드코딩하지 않고 = CURRENT_DATE - INTERVAL '1' DAY 할 수도 있음
    # 100개는 아무 기준이 없네. popularity 상위? -> top_tracks에
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
            audio_features t2 on t2.id = t1.id and cast(t1.dt as date) = CURRENT_DATE and cast(t2.dt as date) = CURRENT_DATE
        GROUP BY
            t1.artist_id
        ORDER BY
            t1.popularity desc
        LIMIT 100
    """

    r = query_athena(query, athena)
    results = get_query_result(r['QueryExecutionId'], athena) # QueryExecutionId는 뭐지?
    artists = process_data(results) # list of dicts 형태. 행별 dict들이 있음
    # print(artists)
    # sys.exit(0)

    # 2. normalization용 쿼리: 전체 track의 수치별 min, max 가져옴
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
    """

    r = query_athena(query, athena)
    results = get_query_result(r['QueryExecutionId'], athena)

    avgs = process_data(results)[0] # max, min 값들은 한 행만 있으므로 [0]으로 가져옴

    # metric들에 대해 for loop
    # 참고: 각 수치들은 '0.11' 이렇게 string으로 저장됨!! -> float으로 변환 필요
    metrics = ['danceability', 'energy', 'loudness', 'speechiness', 'acousticness', 'instrumentalness']
    index = 0
    for i in artists:
        for j in artists: # 다른 artist들. 근데 이건 i 빼고 해야 되지 않나?
            dist = 0
            for k in metrics:
                x = float(i[k])
                x_norm = normalize(x, float(avgs[k+'_min']), float(avgs[k+'_max']))
                y = float(j[k])
                y_norm = normalize(y, float(avgs[k+'_min']), float(avgs[k+'_max']))
                dist += (x_norm - y_norm)**2

            dist = math.sqrt(dist)
            data = {
                'artist_id': i['artist_id'],
                'y_artist': j['artist_id'],
                'distance': dist
            }

            insert_row(cursor, data, 'related_artists')
        index += 1
        if index % 10 == 0: # 이 할당량(10)은 아티스트 개수에 따라 수정해야 하는데, 하드코딩?
            print("{}th artist complete!".format(index))

    conn.commit()
    cursor.close()
    print("complete!")

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

# json 데이터 형태 변환: list of dicts로 행별 데이터 반환하는 함수
def process_data(results):

    columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]

    listed_results = [] # empty list 생성
    # 행별로 저장. ['ResultSet']['Rows']는 0행은 columns. 두 번째 행부터 값이 들어 있음
    for res in results['ResultSet']['Rows'][1:]:
        values = []
        for field in res['Data']:
            try:
                values.append(list(field.values())[0]) # value 들이 [value] 이런 식으로 반환되는 듯
            except:
                values.append(list(' '))
        listed_results.append(dict(zip(columns, values)))

    return listed_results

# mysql의 table에 데이터 insert 하는 함수
def insert_row(cursor, data, table):

    placeholders = ', '.join(['%s'] * len(data))
    columns = ', '.join(data.keys())
    key_placeholders = ', '.join(['{0}=%s'.format(k) for k in data.keys()])
    sql = "INSERT INTO %s ( %s ) VALUES ( %s ) ON DUPLICATE KEY UPDATE %s" % (table, columns, placeholders, key_placeholders)
    cursor.execute(sql, list(data.values())*2)


if __name__ == "__main__":
    main()
