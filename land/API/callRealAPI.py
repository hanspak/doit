# -*- coding: utf-8 -*-
"""
부동산 실거래가 자동 수집기
- 국토교통부 OpenAPI 호출
- XML → DataFrame 변환
- SQLite 저장
"""
import os
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import time
from sqlalchemy import create_engine
from pathlib import Path
from datetime import datetime

# ----------------------------------------
# 설정
# ----------------------------------------
DB_PATH = Path(os.environ["DB_PATH"])  # C:\db\dbTHEH.db
INPUT_FILE = Path("c:/doit/land/code.xlsx")
SHEET_NAME = "list"
SERVICE_KEY = "UIRiUedSrUkb68ntUOKaIkSoppX9Pc1aY3jLtDG4OYFF6PJU7P3pPDybDDYgzaPDGG+0O4lkmzMhz7/o5J0oWw=="
TABLE_NAME = "tbl_real_estate"
NUM_OF_ROWS = 9999
REASON_CODE =""


# DB 연결
engine = create_engine(f"sqlite:///{DB_PATH}")

# ----------------------------------------
# API 호출 함수
# ----------------------------------------
def fetch_trade_data(service_key, lawd_cd, deal_ymd, page_no=1, num_of_rows=NUM_OF_ROWS):
    global REASON_CODE
    url = "http://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
    params = {
        'serviceKey': service_key,
        'pageNo': page_no,
        'numOfRows': num_of_rows,
        'LAWD_CD': lawd_cd,
        'DEAL_YMD': deal_ymd
    }

    try:
        #print("url:",url)
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)

        result_code = root.findtext(".//resultCode")
        if result_code != "000":
            msg = root.findtext(".//returnAuthMsg") or "API 오류 발생"

            REASON_CODE = root.findtext(".//returnReasonCode")  # API 요청이 초과(23:LIMITED_NUMBER_OF_SERVICE_REQUESTS_PER_SECOND_EXCEEDS_ERROR) 아닌 경우는 해당 API 계속 호출
            
            raise ValueError(f"[API Error] {msg}")

        items = root.findall('.//item')

        data = []
        
        for item in items:
            now1: datetime = datetime.now()
            data.append({
                '법정동시군구코드': item.findtext('sggCd'),  #===>key법정동시군구코드 
                '법정동읍면동코드': item.findtext('umdCd'),  #법정동읍면동코드
                '법정동지번코드': item.findtext('landCd'),   #법정동지번코드
                '법정동본번코드': item.findtext('bonbun'),   #법정동본번코드
                '법정동부번코드': item.findtext('bubun'),    #법정동부번코드
    
                '도로명': item.findtext('roadNm'),
                '도로명시군구코드': item.findtext('roadNmSggCd'),
                '도로명코드': item.findtext('roadNmCd'),
                '도로명일련번호코드': item.findtext('roadNmSeq'),
                '도로명지상지하코드': item.findtext('roadNmbCd'),
    
                '도로명건물본번호코드': item.findtext('roadNmBonbun'),
                '법정동': item.findtext('umdNm'),
                '단지명': item.findtext('aptNm'),
                '지번': item.findtext('jibun'),
                '전용면적': item.findtext('excluUseAr'),#===>key 
    
                '계약년도': item.findtext('dealYear'), #===>key
                '계약월': item.findtext('dealMonth'),  #===>key
                '계약일': item.findtext('dealDay'),    #===>key
                '거래금액': item.findtext('dealAmount'),
                '층': item.findtext('floor'),         #===>key
    
                '단지일련번호': item.findtext('aptSeq'), #===>key
                '해제여부': item.findtext('cdealType'), #===>key
                '해제사유발생일': item.findtext('cdealDay'),
                '거래유형': item.findtext('dealingGbn'),
                '중개사소재지': item.findtext('estateAgentSggNm'),
    
                '등기일자': item.findtext('rgstDate'),
                '아파트동명': item.findtext('aptDong'),
                '매도자': item.findtext('slerGbn'),
                '매수자': item.findtext('buyerGbn'),
                '토지임대부아파트여부': item.findtext('landLeaseholdGbn'),
                '생성일자' : now1.strftime('%Y-%m-%d %H:%M:%S')
             })

        # 데이터프레임으로 변환
        df = pd.DataFrame(data)

        return df

    except Exception as e:
        print(f"❌ API 호출 실패 ({lawd_cd}, {deal_ymd}): {e}")
        return pd.DataFrame()

# ----------------------------------------
# DB 저장 함수
# ----------------------------------------
def save_to_sqlite(df: pd.DataFrame, engine, table: str):
    if not df.empty:
        df.to_sql(table, con=engine, if_exists='append', index=False)
        print(f"✅ 저장 완료 ({len(df)}건)")
    else:
        print("⚠️ 저장할 데이터 없음")

# ----------------------------------------
# 실행 루프
# ----------------------------------------
if __name__ == '__main__':
    try:    
        df_input = pd.read_excel(INPUT_FILE, sheet_name=SHEET_NAME)
        # 2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016,2015,2014,2013,2012,2011,2010,2009,2008,2007
        #             
        # 2016(LAWD_CD:DEAL_YMD 26350 201612), 2019 X . 확인 필요함.  2017,2018, 2020, 2021,2022,2023,2024 완료, 2025년 1~3월(완료)

        for idx, row in df_input.iterrows():
            lawd_cd = str(row['code'])
            p1_code = str(row.get('p1_code', '')).strip()

            #print("p1_code:", p1_code)
            if not p1_code or p1_code == '0.0' or p1_code == '0' or lawd_cd=='0':
                continue

            for year in range(2025, 2026):
                for month in range(4, 7):  # 1~4월
                    deal_ymd = f"{year}{str(month).zfill(2)}"
                    print(f"🔍 요청중: LAWD_CD={lawd_cd}, DEAL_YMD={deal_ymd}")

                    isCon = True
                    while isCon:
                        df = fetch_trade_data(SERVICE_KEY, lawd_cd, deal_ymd)
                        print("REASON_CODE:",REASON_CODE)
                        if REASON_CODE =='23' :  #한도 초과 인 경우
                            raise ValueError("[API 호출 한도 초과 Error]")
                        else:  
                            isCon = False

                    save_to_sqlite(df, engine, TABLE_NAME)
                    time.sleep(1)  # 요청 간격 조절
    except Exception as e:
        print(f"❌ 한도초과로 API 호출 실패 ({lawd_cd}, {deal_ymd}): {e}")
