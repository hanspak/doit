# -*- coding: utf-8 -*-
"""
한국은행 경제지표 자동 수집기
- 한국은행 OpenAPI 호출
  SERVICE_KEY : T8RDD17FMQU8DIDXS0VA
- XML → DataFrame 변환
- SQLite 저장
"""
#--------------------------------------------------------------
"""
에러 및 정보배치
정보-100 : 인증키가 유효하지 않습니다. 인증키를 확인하십시오! 인증키가 없는 경우 인증키를 신청하십시오!
정보-200 : 해당하는 데이터가 없습니다.
에러-100 : 필수 값이 누락되어 있습니다. 필수 값을 확인하십시오! 필수 값이 누락되어 있으면 오류를 발생합니다. 요청 변수를 참고 하십시오!
에러-101 : 주기와 다른 형식의 날짜 형식입니다.
에러-200 : 파일타입 값이 누락 혹은 유효하지 않습니다. 파일타입 값을 확인하십시오! 파일타입 값이 누락 혹은 유효하지 않으면 오류를 발생합니다. 요청 변수를 참고 하십시오!
에러-300 : 조회건수 값이 누락되어 있습니다. 조회시작건수/조회종료건수 값을 확인하십시오! 조회시작건수/조회종료건수 값이 누락되어 있으면 오류를 발생합니다.
에러-301 : 조회건수 값의 타입이 유효하지 않습니다. 조회건수 값을 확인하십시오! 조회건수 값의 타입이 유효하지 않으면 오류를 발생합니다. 정수를 입력하세요.
에러-400 : 검색범위가 적정범위를 초과하여 60초 TIMEOUT이 발생하였습니다. 요청조건 조정하여 다시 요청하시기 바랍니다.
에러-500 : 서버 오류입니다. OpenAPI 호출시 서버에서 오류가 발생하였습니다. 해당 서비스를 찾을 수 없습니다.
에러-600 : DB Connection 오류입니다. OpenAPI 호출시 서버에서 DB접속 오류가 발생했습니다.
에러-601 : SQL 오류입니다. OpenAPI 호출시 서버에서 SQL 오류가 발생했습니다.
에러-602 : 과도한 OpenAPI호출로 이용이 제한되었습니다. 잠시후 이용해주시기 바랍니다.
"""
#--------------------------------------------------------------
"""
샘플코드: https://ecos.bok.or.kr/api/StatisticTableList/sample/xml/kr/1/10/102Y004
StatisticTableList : API서비스명 
sample : 한국은행에서 발급받은 오픈API 인증키
xml : 결과값의 파일 형식 - xml, json
kr : 결과값의 언어 - kr(국문), en(영문)
1 : 전체 결과값 중 시작 번호
10 : 전체 결과값 중 끝 번호
102Y004 : 통계표코드
"""

"""
항목명(국문)	 항목명(영문)	  항목크기	 샘플데이터	    항목설명
상위통계표코드	  P_STAT_CODE	  8     	0000000442	상위통계표코드
통계표코드	     STAT_CODE	     8	        102Y004	    통계표코드
통계명	         STAT_NAME	    200	       1.1.1.1.2.   본원통화 구성내역(평잔, 원계열)	통계명
주기	         CYCLE	        2	       M	        주기(년, 분기, 월)
검색가능여부	  SRCH_YN	     1	        Y	        검색가능여부
출처	         ORG_NAME	    50         한국은행	    출처

"""

"""
100대통계지표 : https://ecos.bok.or.kr/api/KeyStatisticList/T8RDD17FMQU8DIDXS0VA/xml/kr/1/10

날짜 조건별 검색 : https://ecos.bok.or.kr/api/KeyStatisticList/T8RDD17FMQU8DIDXS0VA/xml/kr/1/10/200Y101/A/2020/2023/10101/?/?/?   
"""


import requests
import xml.etree.ElementTree as ET
import pandas as pd
import time
from sqlalchemy import create_engine
from pathlib import Path

# ----------------------------------------
# 설정
# ----------------------------------------
DB_PATH = Path("c:/doit/land/dbTHEH.db")
#INPUT_FILE = Path("c:/doit/land/code.xlsx")
#SHEET_NAME = "list"
SERVICE_KEY = "T8RDD17FMQU8DIDXS0VA"
TABLE_NAME = "tbl_cos_estate_tmp"
NUM_OF_ROWS = 9999
REASON_CODE =""


# DB 연결
engine = create_engine(f"sqlite:///{DB_PATH}")

# ----------------------------------------
# API 호출 함수
# ----------------------------------------
def fetch_trade_data(service_key, reType, lang, page_no, num_of_rows, statCode,period, startY, endY, statItem1):
    global REASON_CODE
    #url = "http://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
    url = f"http://ecos.bok.or.kr/api/StatisticSearch/{service_key}/{reType}/{lang}/{page_no}/{num_of_rows}/{statCode}/{period}/{startY}/{endY}/{statItem1}/?/?/?"   
           #http://ecos.bok.or.kr/api/StatisticSearch/T8RDD17FMQU8DIDXS0VA/xml/kr/1/9999/200Y101/M/2020/2022/10101/?/?/?
    #      "http://ecos.bok.or.kr/api/StatisticSearch/sample/xml/kr/1/10/200Y101/A/2020/2023/10101/?/?/?"
    params = {
    }

    try:
        print("url:",url)
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)

        result_code = root.findtext(".//CODE")  # 오류 출력시 반환되는 코드값

        #print("result_code:", result_code)
        
        #if result_code != "000" :
        #    msg = root.findtext(".//returnAuthMsg") or "API 오류 발생"

        #    REASON_CODE = root.findtext(".//returnReasonCode")  # API 요청이 초과(23:LIMITED_NUMBER_OF_SERVICE_REQUESTS_PER_SECOND_EXCEEDS_ERROR) 아닌 경우는 해당 API 계속 호출
            
        #    raise ValueError(f"[API Error] {msg}")

        if result_code  is not None:
            msg = root.findtext(".//MESSAGE") or "오류 메세지"
            raise ValueError(f"[API Error] {msg}")
        
        
        items = root.findall('.//row')
        data = []
        #print("len(items):", len(items))
        for item in items:
            
            record = {elem.tag: elem.text for elem in item}
            data.append(record)

        # 데이터프레임으로 변환
        df = pd.DataFrame(data)

        return df

    except Exception as e:
        print(f"❌ API 호출 실패(fetch_trade_data) : {e}")
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

#statCode : 101Y004(M2(광의통화)),  statItem1 : BBHA00(M2(평잔, 원계열)), period(A,M,Q)
#           722Y001(한국은행 기준금리 및 여수신금리), 0101000(한국은행 기준금리리)
    
    reType ='xml'
    lang = 'kr'
    page_no = 1
    num_of_rows = NUM_OF_ROWS
    statCode='722Y001'
    period='A'
    startY='1980'
    endY='2022'
    statItem1='0101000'
    try:    
        isCon = True

        while isCon:    
            df = fetch_trade_data(SERVICE_KEY, reType, lang, page_no, num_of_rows,statCode,period,startY, endY,statItem1)

            print("REASON_CODE:",REASON_CODE)
            if REASON_CODE =='23' :  #한도 초과 인 경우
                raise ValueError("[API 호출 한도 초과 Error]")
            else:  
                isCon = False

        print(df.head())
        save_to_sqlite(df, engine, TABLE_NAME)

    except Exception as e:
        print(f"❌ 한도초과로 API 호출 실패 : {e}")
