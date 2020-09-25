# 챗봇 메시징 코드
import sys
sys.path.append('./libs') # libs 안에 있는 것 사용하도록 configure
import boto3, pymysql
from boto3.dynamodb.conditions import Key
import logging, pickle, requests, json, base64
from urllib import parse
from googletrans import Translator

logger = logging.getLogger() # cloudwatch 로그 보기?
logger.setLevel(logging.INFO)
raw = () # db 결과 저장하는 변수
base_url = "https://www.youtube.com/results?" # YouTube 검색 결과 링크

# AWS mysql 정보 -> 환경 변수로?
# 환경변수 사용법 예시: import os 하고 region = os.environ['AWS_REGION']
with open('dbinfo.pickle', 'rb') as f:
    data = pickle.load(f)

for key in data.keys():
    globals()[key] = data[key]

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

# bot = fb_bot.Bot(PAGE_TOKEN)

# main 함수를 호출하는 것이 아니므로, 다른 함수들은 lambda_handler보다 위에 써야 함
# API 쿼리 위한 header
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

# MySQL에 데이터 삽입
def insert_row(cursor, data, table):

    # data의 개수에 맞게 넣어 줌
    placeholders = ', '.join(['%s'] * len(data)) # 형태: '%s, %s, %s, ...'
    columns = ', '.join(data.keys())
    key_placeholders = ', '.join(['{0}=values({0})'.format(k) for k in data.keys()])
    # 반복적인 인자들을 %s에 넣어줌
    sql = "INSERT INTO %s ( %s ) VALUES ( %s ) ON DUPLICATE KEY UPDATE %s" % (table, columns, placeholders, key_placeholders)

    # print(sql) # 아래와 같은 형태
    """
    INSERT INTO artists ( id, name, followers, popularity, url, image_url )
    VALUES ( %s, %s, %s, %s, %s, %s )
    ON DUPLICATE KEY UPDATE id=values(id), name=values(name), followers=values(followers),
    popularity=values(popularity), url=values(url), image_url=values(image_url)
    """

    cursor.execute(sql, list(data.values()))
    # 여기서 list(data.values()) 말고 그냥 data.values() 하면 오류 남: 'dict_values' object has no attribute 'translate'
    # cursor.execute 안에 넣을 수 없는 데이터 형식(dict_values)인 듯.
    # print(data.values())

# 다른 람다를 호출(invoke)하는 함수. payload 부분이 event로 들어가는 부분
# IAM을 통해 이 lambda function에 AWSLambdaFullAccess 권한을 주어야 함
# invocation_type = 'Event' -> 비동기!!
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
    # invoke_response 형식 예시. StatusCode가 202 -> 비동기 응답
    # {'ResponseMetadata': {
    #     'RequestId': '9c43412b-4eae-4334-9072-846d296430c7', 
    #     'HTTPStatusCode': 202, 
    #     'HTTPHeaders': {
    #         'date': 'Fri, 12 Jun 2020 14:56:44 GMT', 'content-length': '0', 'connection': 'keep-alive',
    #         'x-amzn-requestid': '9c43412b-4eae-4334-9072-846d296430c7', 'x-amzn-remapped-content-length': '0', 
    #         'x-amzn-trace-id': 'root=1-5ee397ac-b822719b1f62900344f882ed;sampled=0'}, 
    #     'RetryAttempts': 0}, 
    # 'StatusCode': 202, 
    # 'Payload': <botocore.response.StreamingBody object at 0x7fbe6f4bd350>
    # }

    return invoke_response

# DynamoDB에서 top_tracks 데이터 호출하는 함수. ListCard 형태에 맞게 리턴
def get_top_tracks_db(artist_id, artist_name):

    table = dynamodb.Table('top_tracks')
    response = table.query(
        KeyConditionExpression=Key('artist_id').eq(artist_id)
    )
    # 결과를 popularity 내림차순 정렬하여, 상위 3개 보여 줌
    # api 결과는 popularity 내림차순으로 나오므로, DB 결과만 정렬하면 됨
    response['Items'].sort(key=lambda x: x['popularity'], reverse=True)

    items = []

    for ele in response['Items'][:3]:
        name = ele['name']
        query = {
            'search_query': '{} {}'.format(artist_name, name)
        }

        youtube_url = base_url + parse.urlencode(query, encoding='UTF-8', doseq=True)
        # youtube_url = 'https://www.youtube.com/results?search_query={}+{}'.format(
        #     artist_name.replace(' ', '+'), name.replace(' ', '+'))
        
        temp_dic = {
            "title": name,
            "description": ele['album']['name'],
            "imageUrl": ele['album']['images'][1]['url'],
            "link": {
                "web": youtube_url
            }
        }

        items.append(temp_dic)
    
    # print(items)

    return items

# API에서 top_tracks 호출하는 함수. ListCard 형태에 맞게 리턴
def get_top_tracks_api(artist_id, artist_name):
    URL = "https://api.spotify.com/v1/artists/{}/top-tracks".format(artist_id)
    params = {
        'country': 'US'
    }

    headers = get_headers(client_id, client_secret)
    r = requests.get(URL, params=params, headers=headers)
    raw = json.loads(r.text)
    globals()['data_for_dynamodb'] = raw

    items = []

    # top_tracks가 있을 때에만, 즉 raw['tracks']가 있을 때에만 루프가 실행됨
    for ele in raw['tracks'][:3]:
        name = ele['name']
        query = {
            'search_query': '{} {}'.format(artist_name, name)
        }

        youtube_url = base_url + parse.urlencode(query, encoding='UTF-8', doseq=True)
        # youtube_url = 'https://www.youtube.com/results?search_query={}+{}'.format(
        #     artist_name.replace(' ', '+'), name.replace(' ', '+'))
        
        temp_dic = {
            "title": name,
            "description": ele['album']['name'],
            "imageUrl": ele['album']['images'][1]['url'], # images는 같은 앨범 이미지에 대해서 크기별로 넣어 놓은 것. 1이 적당한 사이즈(300x300)라 고름
            "link": {
                "web": youtube_url
            }
        }
        items.append(temp_dic)

    return items

# 해외 아티스트를 한국어로 검색했을 때 결과가 나오지 않을 경우, 영어로 번역해서 다시 검색 시도
def translate_artist(korean):
    translator = Translator() # 번역기
    return translator.translate(korean, dest="en").text

# 관련 아티스트의 id와 이름 가져오기
# 해당 아티스트의 관련 아티스트가 아직 저장되지 않은 경우 return
def related_artist(artist_id):
    try:
        query = """
            select t1.y_artist, t2.name, t2.image_url from related_artists t1
            join artists t2 on t1.y_artist = t2.id
            where t1.artist_id = '{}' order by t1.distance
            limit 3
        """.format(artist_id)
        # query = 'select y_artist from related_artists where artist_id="{}"'.format(artist_id)
        cursor.execute(query)
        return cursor.fetchall()
    except:
        return

#### 카카오톡 메시지 타입별 함수 ####

# SimpleText 메시지
def simple_text(msg):
    return {
        "simpleText": {
            "text": msg
        }
    }

# ListCard 메시지
def list_card(title, imageUrl, items, webLinkUrl):
    return {
        "listCard": {
            "header": {
                "title": title,
                "imageUrl": imageUrl
            },
            "items": items,
            "buttons": [
                {
                "label": "다른 노래도 보기",
                "action": "webLink",
                "webLinkUrl": webLinkUrl
                }
            ]
        }
    }

# Carousel (여러 장의 카드 메시지)
# carousel의 type은 필요하면 수정할 수 있도록, 기본값(현재 listCard)을 넣음
def carousel(items, card_type = "listCard"):
    return {
        "carousel": {
            "type": card_type,
            "items": items
        }
    }

# 챗봇 메시지
def message(outputs):
    return {
        "version": "2.0",
        "template": {
            "outputs": outputs # 여기에 메시지 카드들이 들어감(list로)
        }
    }

# 최종 json result
def json_result(result):
    return {
        'statusCode': 200,
        'body': json.dumps(result),
        'headers': {
            'Access-Control-Allow-Origin': '*',
        }
    }

##############################

# 검색어와 DB에 있는 아티스트 이름이 일치하지 않을 경우, API에서 검색하는 함수
def search_artist(cursor, artist_name):

    headers = get_headers(client_id, client_secret) # id, secret은 globals()로 생성

    ## Spotify Search API
    params = {
        "q": artist_name,
        "type": "artist",
        "limit": "1"
    }

    r = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)
    raw = json.loads(r.text)

    # 검색 결과가 없을 경우, ['artists']['items']가 empty list - []가 됨
    # 검색 단어와 저장 단어가 다를 경우, DB에는 있음. 이 데이터를 주면 됨
    if raw['artists']['items'] == []:
        # 번역해서 다시 검색해 보고, 있으면 넘어가기. 그래도 없으면 리턴
        params = {
            "q": translate_artist(artist_name), # 여기를 번역 결과로!! 함수 만들어서
            "type": "artist",
            "limit": "1"
        }

        r = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)
        raw = json.loads(r.text)

        if raw['artists']['items'] == []:
            print("없는 아티스트")
            return [simple_text('아티스트를 찾을 수 없습니다. 다시 입력해 주세요.')]

    artist_raw = raw['artists']['items'][0]
    # logger.info(artist_raw)

    # 검색 결과가 DB에 있는지 테스트함. 이미 있으면 나가야 함
    query = 'select id, name, image_url from artists where name = "{}"'.format(artist_raw['name'])
    logger.info(query) # 수정된 쿼리
    cursor.execute(query)
    db_result = cursor.fetchall()
    
    if len(db_result) > 0: # 이미 있는 데이터면, DB 데이터를 저장하고 나감
        print("이미 있는 데이터 가져오기")
        globals()['raw'] = db_result
        # global raw 하고 raw = db_result 하고 싶은데, 함수를 실행하기도 전에, 할당 전에 사용했다는 오류 발생
        return

    print("새로운 데이터 저장")
    # DB에 없는 데이터면, DB에 저장해야 함
    artist = {}
    # 검색 결과와 검색어가 일치할 경우만 데이터를 저장했었는데, 지금은 일단 엔티티를 통과하면 다 넣기
    # if artist_raw['name'].lower() == params['q'].lower(): # 소문자로 변환하여 비교하기

    temp_artist_url = ""
    if artist_raw['images']:
        temp_artist_url = artist_raw['images'][0]['url']
    else:
        # images가 없는 아티스트도 있는데(나은 Naeun), 문제는 basiccard에서 image_url이 항상 필요함
        # 이럴 경우 이미지로 basicCard 예시 코드에 있는 profile-imageUrl 주기
        temp_artist_url = "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcT4BJ9LU4Ikr_EvZLmijfcjzQKMRCJ2bO3A8SVKNuQ78zu2KOqM"

    artist.update(
        {
            'id': artist_raw['id'],
            'name': artist_raw['name'],
            'followers': artist_raw['followers']['total'],
            'popularity': artist_raw['popularity'],
            'url': artist_raw['external_urls']['spotify'],
            'image_url': temp_artist_url
        }
    )
    
    
    # 아티스트가 있는데 장르가 없는 경우도 있음(예: Andrew W.K.). 이 경우는 장르는 따로 처리하지 않음
    # 장르가 있을 경우, artist_genres 테이블 먼저 insert
    if len(artist_raw['genres']) != 0:
        for i in artist_raw['genres']:
            insert_row(cursor, {'artist_id': artist_raw['id'], 'genre': i}, 'artist_genres')

    # 이제 artists 테이블 insert
    insert_row(cursor, artist, 'artists')
    conn.commit()
    
    temp = []
    temp_text = simple_text("{}의 노래를 들어보세요.".format(artist_raw['name']))
    temp.append(temp_text)

    temp_text = simple_text("아티스트가 추가되었습니다. 처리 시간 동안 기다려주셔서 감사합니다.")
    temp.append(temp_text)

    # # basic card 내용을 반환하여, lambda_handler 함수에서 응답에 append 할 수 있도록 하기
    # # db에 저장한걸 또 cursor로 가져오지 말고, 여기서는 api 결과를 사용
    # temp_card = {
    #     "basicCard": {
    #         "title": artist_raw['name'],
    #         "description": ", ".join(artist_raw['genres']), # 여기에 장르 담기
    #         "thumbnail": {
    #             "imageUrl": temp_artist_url
    #         },
    #         "buttons": [
    #             {
    #                 "action": "webLink",
    #                 "label": "YouTube에서 듣기", # label은 최대 8자
    #                 "webLinkUrl": youtube_url
    #             },
    #         ]
    #     }
    # }

    # temp.append(temp_card)
 
    temp_top_tracks = get_top_tracks_api(artist_raw['id'], artist_raw['name'])
    
    # top_tracks 데이터가 있을 경우에만 DynamoDB의 top-tracks 테이블에 insert
    if temp_top_tracks:
        resp = invoke_lambda('top-tracks', payload={
            'artist_name': artist_raw['name'], # 로그 용도로 이름까지 보냄
            'artist_id': artist_raw['id'],
            'data': globals()['data_for_dynamodb']
        })
        # 응답 결과: 해당 람다에서 리턴한 값이 아니라, 아래와 같이 찍힘
        # print("top tracks INSERT:", resp)

        query = {
            'search_query': artist_raw['name']
        }
        youtube_url = base_url + parse.urlencode(query, encoding='UTF-8', doseq=True)
        temp_list = list_card(artist_raw['name'], temp_artist_url, temp_top_tracks, youtube_url)
        temp.append(temp_list)

    else:
        temp_text = simple_text("{}의 노래가 없습니다. 한국어로 검색하셨다면, 영어로도 검색해 보세요.".format(artist_raw['name']))
        temp.append(temp_text)

    return temp


def lambda_handler(event, context):

    request_body = json.loads(event['body'])
    # user_id = request_body['userRequest']['user']['id'] # user id. 추후 필요시 사용하기
    logger.info(request_body)
    params = request_body['action']['params'] # 오픈빌더는 action > params 안에 input 데이터가 들어있다.
    if params:
        for key in params.keys():
            # test = params[key] # 이건 이름만 인식하므로, \n 제거 안해도 됨
            print("인식한 artist name: {} ({})".format(params[key], key)) # 인식했을 때만 출력해 보기. 아직 실제 사용하지는 않음

    # 메시지는 뒤에 \n이 붙어서, 제거
    artist_name = request_body['userRequest']['utterance'].rstrip("\n")

	# input 으로 받아온 데이터로 원하는 결과를 생성하는 코드 작성
    # url을 먼저 가져와서 있으면 아티스트 정보를 보여주고 장르로 넘어가고, 없으면 에러 처리
    query = 'select id, name, image_url from artists where name = "{}"'.format(artist_name) # 원래는 url 칼럼도 담았었는데, spotify link 사용할거 아니므로 뺌
    logger.info(query)
    cursor.execute(query)
    globals()['raw'] = cursor.fetchall()

    # 아티스트가 DB에 없을 경우 DB에 추가하는 작업
    if len(raw) == 0:
        search_result = search_artist(cursor, artist_name) # 새로운 데이터 db에 저장할 때 안내 메시지 띄움

        # 새로운 데이터가 추가되었을 경우의 메시지 상태. 기존 데이터를 사용할 경우 아래로 내려감
        if search_result:
            # print("대체 메시지")
            result = message(search_result)
            return json_result(result)
    
    logger.info(globals()['raw'])
    artist_id, db_artist_name, image_url = raw[0]
    temp_artist_name = db_artist_name # 이 변수를 아티스트 이름에 ' 있을 때만 할당할 수는 없나?

    # sql 쿼리를 위해, Girls' Generation같이 이름에 '가 들어가면 ''로 수정하여 쿼리 가능하게 함
    if "'" in db_artist_name:
        db_artist_name = db_artist_name.replace("'", "''")
    
    query = {
        'search_query': temp_artist_name
    }
    youtube_url = base_url + parse.urlencode(query, encoding='UTF-8', doseq=True)
    # youtube_url = 'https://www.youtube.com/results?search_query={}'.format(temp_artist_name.replace(' ', '+'))

    ########## 장르 가져오기 ##########
    # query = """
    #     select t2.genre from artists t1 join artist_genres t2 on t2.artist_id = t1.id
    #     where t1.name = '{}'
    # """.format(db_artist_name)
    # cursor.execute(query)

    # genres = []
    # for (genre, ) in cursor.fetchall():
    #     genres.append(genre)

    ################################

    # 메시지 결과 저장하는 변수
    temp = []

    # top tracks 데이터가 DynamoDB에 없는 아티스트가 있음. 처음에 MySQL에 추가할 때 같이 삽입이 되지 않은 듯.
    # 확인하고 없으면 데이터 삽입
    temp_top_tracks = get_top_tracks_db(artist_id, temp_artist_name)
    if not temp_top_tracks:
        temp_top_tracks = get_top_tracks_api(artist_id, temp_artist_name)

        # 마이크로닷: DB에도 없고 API에도 없음. 안내 메시지 보내고 리턴
        if not temp_top_tracks:
            temp_text = simple_text("{}의 노래가 없습니다. 한국어로 검색하셨다면, 영어로도 검색해 보세요.".format(temp_artist_name))
            temp.append(temp_text)
            result = message(temp)

            return json_result(result)

        # API 결과가 있으면 DB 삽입
        resp = invoke_lambda('top-tracks', payload={
            'artist_name': db_artist_name, # 로그 용도로 이름까지 보냄
            'artist_id': artist_id,
            'data': globals()['data_for_dynamodb']
        })
        print("top tracks INSERT:", resp)


    # 요청받은 아티스트의 카드 (관련 아티스트 있는 경우, 없는 경우 공통)
    carousel_items = []
    # 해당 아티스트 먼저 넣기
    card_this_artist = list_card(temp_artist_name, image_url, temp_top_tracks, youtube_url)['listCard']
    carousel_items.append(card_this_artist)

    # 1. 관련 아티스트가 저장되어 있을 경우(매일 밤 배치 처리를 통해 저장): 안내 메시지 + 요청받은 아티스트 + 관련 아티스트
    if related_artist(artist_id):
        # 1. SimpleText
        temp_text = simple_text("{} + 관련 아티스트들의 노래를 들어보세요.".format(temp_artist_name))
        temp.append(temp_text)

        # 2. Carousel (관련 아티스트 카드 3개)
        rel_artists = related_artist(artist_id)
        for artist in rel_artists:
            rel_id, rel_name, rel_image_url = artist
            rel_top_tracks = get_top_tracks_db(rel_id, rel_name)
            query2 = {
                'search_query': rel_name
            }
            youtube_url2 = base_url + parse.urlencode(query2, encoding='UTF-8', doseq=True)

            # Carousel에 들어갈 ListCard의 형태는 ListCard만 단독으로 보낼 때보다 한 단계 적음. json의 'listCard' 부분만 사용
            card_rel_artist = list_card(rel_name, rel_image_url, rel_top_tracks, youtube_url2)['listCard']
            carousel_items.append(card_rel_artist)

        temp.append(carousel(carousel_items))

    # 2. 관련 아티스트가 아직 저장되어 있지 않을 경우: 안내 메시지 + 요청받은 아티스트
    else:
        # 1. SimpleText
        temp_text = simple_text("{}의 노래를 들어보세요.".format(temp_artist_name))
        temp.append(temp_text)

        # 2. Carousel (해당 아티스트 카드 1개)
        temp_carousel = carousel(card_this_artist)
        temp.append(temp_carousel)
    

    # 최종 메시지
    result = message(temp)

    # 메시지 리턴
    return json_result(result)