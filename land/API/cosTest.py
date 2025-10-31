import requests
import pandas as pd

# API 설정
api_key = 'T8RDD17FMQU8DIDXS0VA'  # 발급받은 인증키로 대체하세요
stat_code = '722Y001'     # M2 통화량 통계표 코드
item_code = 'BBGS00'      # 통계항목 코드
start_date = '202001'     # 조회 시작일자
end_date = '202412'       # 조회 종료일자

# API 요청 URL 구성
url = f'http://ecos.bok.or.kr/api/{api_key}/json/kr/1/100/{stat_code}/M/{start_date}/{end_date}/{item_code}'

# API 요청

print("url:",url)
response = requests.get(url)
data = response.json()

# 데이터 추출 및 DataFrame 변환
if 'StatisticSearch' in data:
    rows = data['StatisticSearch']['row']
    df = pd.DataFrame(rows)
    df = df[['TIME', 'DATA_VALUE']].rename(columns={'TIME': '날짜', 'DATA_VALUE': 'M2 통화량'})
    df['날짜'] = pd.to_datetime(df['날짜'], format='%Y%m')
    df['M2 통화량'] = df['M2 통화량'].astype(float)
    print(df.head())
else:
    print("데이터를 불러오는 데 실패했습니다.")