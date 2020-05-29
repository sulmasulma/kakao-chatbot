#!/bin/bash

rm kakao.zip
zip kakao.zip -r *

aws s3 rm s3://spotify-lambda-matt/kakao.zip
aws s3 cp kakao.zip s3://spotify-lambda-matt/kakao.zip
aws lambda update-function-code --function-name spotify-kakao --s3-bucket spotify-lambda-matt --s3-key kakao.zip
