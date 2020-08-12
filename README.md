# kakao-chatbot
- Spotify(음원 서비스) API의 아티스트/음원 데이터를 이용하여, 아티스트를 입력하면 관련 아티스트를 추천해 주는 카카오톡 챗봇 개발
- 리눅스 crontab을 이용하여 데이터 처리 자동화
- 사용자의 request(카카오톡 메시지)가 있을 때에만 작동하는 Serverless 방식. 입력받은 아티스트와 유사도가 가장 큰 아티스트를 response로 답해 줌
- 데이터 처리 과정
  1. API에서 가져온 raw data를 Amazon S3에 저장하여 Data Lake 구성
  2. S3에 저장된 아티스트별 인기 트랙의 음원 정보를 Athena를 통해 쿼리
  3. 아티스트들 사이의 유사도(Euclidean distance)를 계산한 후, MySQL에 저장하여 Data Mart로 사용
- 사용 기술: Python, AWS 서비스(Lambda, EC2, S3, Athena, MySQL, DynamoDB)
- [블로그](https://sulmasulma.github.io/data/2020/06/03/kakaotalk-chatbot.html)에 정리해 놓았습니다. 

챗봇 사용해 보기: https://pf.kakao.com/_xgubvxb
