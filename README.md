# AWS Lambda를 이용한 Serverless 카카오톡 챗봇
- Spotify(음원 서비스) API의 아티스트/음원 데이터를 이용하여, 아티스트를 입력하면 관련 아티스트를 추천해 주는 카카오톡 챗봇 개발
- 리눅스 `crontab`을 이용하여 데이터 처리 자동화
- 사용자의 request(카카오톡 메시지)가 있을 때에만 작동하는 Serverless 방식. 입력받은 아티스트와 유사도가 가장 큰 아티스트를 response로 답해 줌
- 데이터 처리 과정
  1. `ttandaudio_to_s3.py`: API에서 가져온 raw data(아티스트별 인기 트랙, 트랙별 음원 특성)를 Amazon S3에 저장하여 Data Lake 구성
  2. `related_artists.py`: S3 데이터를 Athena를 통해 쿼리. 트랙별 음원 특성 벡터를 이용하여 아티스트들 사이의 유사도(Euclidean distance)를 계산한 후, MySQL에 저장하여 Data Mart로 사용
  3. `lambda/lambda_function.py`: AWS Lambda function. 사용자가 카카오톡에서 아티스트를 입력하면, 해당 아티스트 및 유사도가 가장 큰 3개 아티스트의 인기 트랙을 응답해 줌
- 번외
  - `update_artists.py`: MySQL의 artists 테이블 업데이트. 챗봇에 사용되는 데이터는 아니지만, 아티스트의 최신화된 popularity, 팔로워 수 파악하기 위한 용도
- 사용 기술: Python, AWS 서비스 (Lambda, EC2, S3, Athena, MySQL, DynamoDB)
- [블로그](https://sulmasulma.github.io/data/2020/06/03/kakaotalk-chatbot.html)에 정리해 놓았습니다. 

챗봇 사용해 보기: https://pf.kakao.com/_xgubvxb
