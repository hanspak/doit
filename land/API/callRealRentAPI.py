# -*- coding: utf-8 -*-
"""
부동산 전월세 실거래가 자동 수집기
- 국토교통부 OpenAPI 호출
- XML → DataFrame 변환
- SQLite 저장
"""

import requests
import xml.etree.ElementTree as ET
import pandas as pd
import time
from sqlalchemy import create_engine
from sqlalchemy import text
from pathlib import Path
from datetime import datetime

# ----------------------------------------
# 설정
# ----------------------------------------
DB_PATH = Path("c:/doit/land/dbTHEH.db")
INPUT_FILE = Path("c:/doit/land/code.xlsx")
SHEET_NAME = "list"
#SERVICE_KEY = "UIRiUedSrUkb68ntUOKaIkSoppX9Pc1aY3jLtDG4OYFF6PJU7P3pPDybDDYgzaPDGG+0O4lkmzMhz7/o5J0oWw=="
SERVICE_KEY = "UIRiUedSrUkb68ntUOKaIkSoppX9Pc1aY3jLtDG4OYFF6PJU7P3pPDybDDYgzaPDGG+0O4lkmzMhz7/o5J0oWw=="



TABLE_NAME = "tbl_rent_real_estate"
TABLE_REAL = "tblRentReal"
NUM_OF_ROWS = 9999
REASON_CODE =""


# DB 연결
engine = create_engine(f"sqlite:///{DB_PATH}")

# ----------------------------------------
# API 호출 함수
# ----------------------------------------
def fetch_trade_data(service_key, lawd_cd, deal_ymd, page_no=1, num_of_rows=NUM_OF_ROWS):
    global REASON_CODE
    #url = "http://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
    url = "http://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"
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
                '지역코드': item.findtext('sggCd'),  #===>key법정동시군구코드 
                '법정동': item.findtext('umdNm'),  #법정동읍면동코드
                '아파트명': item.findtext('aptNm'),   #법정동지번코드
                '지번': item.findtext('jibun'),   #법정동본번코드
                '전용면적': item.findtext('excluUseAr'),    #법정동부번코드
    
                '계약년도': item.findtext('dealYear'),
                '계약월': item.findtext('dealMonth'),
                '계약일': item.findtext('dealDay'),
                '보증금액': item.findtext('monthlyRent'),
                '층': item.findtext('floor'),
    
                '건축년도': item.findtext('buildYear'),
                '계약기간': item.findtext('contractType'),
                '갱신요구권사용': item.findtext('useRRRight'),
                '종전계약보증금': item.findtext('preDeposit'),
                '종전계약월세': item.findtext('preMonthlyRent'),#===>key 
    
                '생성일자' : now1.strftime('%Y-%m-%d %H:%M:%S')
             })

        # 데이터프레임으로 변환
        df = pd.DataFrame(data)

        return df

    except Exception as e:
        print(f"❌ fetch_trade_data():API 호출 실패 ({lawd_cd}, {deal_ymd}): {e}")
        return pd.DataFrame()

# ----------------------------------------
# DB 저장 함수(tbl_real_estate)
# ----------------------------------------
def save_to_sqlite(df: pd.DataFrame, engine, table: str):
    sql = f"delete from {table} where 지역코드 ='{lawd_cd}' and 계약년도 = '{deal_ymd[:4]}' and 계약월 = '{int(deal_ymd[4:])}'"   
    
    print("sql:", sql)    
    #with engine.connect() as conn:
    with engine.connect() as conn:
        result= conn.execute(text(sql))
        conn.commit() 
        print(f"✅ {table} {lawd_cd},{deal_ymd}에서 {result.rowcount}행  데이터 삭제")


    if not df.empty:
        df.to_sql(table, con=engine, if_exists='append', index=False)
        print(f"✅ tbl_rent_real_estate : {lawd_cd},{deal_ymd} 저장 완료 ({len(df)}건)")
    else:
        print(f"⚠️ tbl_rent_real_estate : {lawd_cd},{deal_ymd} 저장할 데이터 없음")


# ----------------------------------------
# DB 저장 함수(tblReal)
# ----------------------------------------
def insert_into_tblReal(engine,lawd_cd, deal_ymd):
    year = deal_ymd[:4]
    month = int(deal_ymd[4:])  # '06' → 6

    from calendar import monthrange
    last_day = monthrange(int(year), month)[1]  # monthrange  첫째날 요일, 마지막 날 반환

    start_date = f"{year}-{str(month).zfill(2)}-01"
    end_date = f"{year}-{str(month).zfill(2)}-{last_day}"    

    sql = f"delete from tblRentReal where sggCd ='{lawd_cd}' and dealDate between '{start_date}' and '{end_date}'"   
    
    #print("sql:", sql)
    #with engine.connect() as conn:
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        conn.commit() 
        print(f"✅ insert_into_tblReal(): tblReal : {lawd_cd},{deal_ymd}에서 {result.rowcount}행  데이터 삭제")


    sql = """
    INSERT INTO tblRentReal(	sggCd 	,aptSeq     ,excluUseAr     ,dealDate 	 	,floor 	
                            ,cdealType 	,no 	,useArea 	,cdealDay 	,umdNm
                            ,jibun 	,aptNm 	,aptDong 	,rgstDate 	,dealAmount
                            ,dealingGbn 	,estateAgentSggNm 	,slerGbn 	,buyerGbn 	,landLeaseholdGbn
                            ,umdCd 	,landCd 	,bonbun 	,bubun 	,roadNm
                            ,roadNmSggCd 	,roadNmCd 	,roadNmSeq 	,roadNmbCd 	,roadNmBonbun
                            ,createdDate
                            )
    SELECT 법정동시군구코드   sggCd, 단지일련번호 aptSeq, 전용면적 excluUseAr,  DATE(printf('%04d-%02d-%02d', 계약년도, 계약월, 계약일))  dealDate,  층 floor  
       ,CASE WHEN 해제여부 = 'O' THEN -1 ELSE 1 END  cdealType
       , ROW_NUMBER() OVER (
           PARTITION BY 법정동시군구코드, 계약년도, 계약월, 계약일, 단지일련번호, 층, 전용면적, 해제여부
           ORDER BY 계약일 -- 또는 다른 정렬 기준
       ) AS no
	   , round( 전용면적 / 3.305785,1 ) useArea  -- 역산한 전용면적(평)
	   , case when 해제사유발생일 is not NULL
             then date(printf('20%02d-%02d-%02d',substr(해제사유발생일,1,2), substr(해제사유발생일,4,2) , substr(해제사유발생일,7,2))) 
			 else NULL
			 end  cdealDay
	   , 법정동 umdNm, 지번 jibun, 단지명 aptNm, 아파트동명 aptDong  
	   , case when 해제사유발생일 is not NULL
             then date(printf('20%02d-%02d-%02d',substr(등기일자,1,2), substr(등기일자,4,2) , substr(등기일자,7,2))) 
			 else NULL
			 end  rgstDate
	   , CAST(replace(거래금액,",","") as integer) dealAmount, 거래유형 dealingGbn, 중개사소재지 estateAgentSggNm
	   , 매도자 slerGbn, 매수자 buyerGbn, 토지임대부아파트여부 landLeaseholdGbn
	   , 법정동읍면동코드 umdCd, 법정동지번코드 landCd, 법정동본번코드 bonbun, 법정동부번코드 bubun
	   , 도로명 roadNm, 도로명시군구코드 roadNmSggCd, 도로명코드 roadNmCd
	   , 도로명일련번호코드 roadNmSeq, 도로명지상지하코드 roadNmbCd, 도로명건물본번호코드 roadNmBonbun
	   , date('now')
    FROM tbl_real_estate
    """
    sql = sql + f" where 법정동시군구코드 ='{lawd_cd}' and 계약년도 = '{deal_ymd[:4]}' and 계약월 = '{int(deal_ymd[4:])}'"   
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        conn.commit() 
        print(f"✅ insert_into_tblReal(): tbl_real_estate → tblReal 데이터 {result.rowcount} 이관 완료")


# ----------------------------------------
# DB 저장 함수(tblRealStatAptMon) : 통계저장
# ----------------------------------------
def insert_into_tblRealStatUnitMon(engine,lawd_cd, deal_ymd):
    year = deal_ymd[:4]
    month = int(deal_ymd[4:])  # '06' → 6
 
    start_date = f"{year}-{str(month).zfill(2)}-01"
    from calendar import monthrange
    last_day = monthrange(int(year), month)[1]  # monthrange  첫째날 요일, 마지막 날 반환
    end_date = f"{year}-{str(month).zfill(2)}-{last_day}"

    #print("start_date:", start_date) 
    #print("end_date:", end_date)

    sql = f"delete from tblRentRealStatUnitMon where sggCd='{lawd_cd}' and dealYear = '{year}' and dealMon =  '{month}' "   
    
    #print("sql:", sql)
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit() 
        print(f"✅ insert_into_tblRealStatUnitMon(): {lawd_cd},{deal_ymd}  데이터 삭제")

    sql = f"""
            insert or replace into tblRentRealStatUnitMon
            select  sggCd, aptSeq, dealYear, dealMon,  dealAvgAmount,  nm2, p1_code, nm1, aptNm,  cnt , rnk, date('now')
            from(
            select RANK() OVER (PARTITION BY sggCd, dealYear, dealMon ORDER BY round(avg(dealAvgAmount),0) DESC) AS rnk
                , p1_code, nm1, sggCd, dealYear, dealMon, nm2, aptSeq, aptNm , round(avg(dealAvgAmount),0) dealAvgAmount, sum(cnt) cnt
            from(
            select c1.p1_code, c2.nm nm1, r.sggCd, strftime('%Y',r.dealDate) dealYear, strftime('%m',dealDate) dealMon
                , c1.nm nm2
                , r.aptSeq, r.aptNm
                , round(sum(cdealType*dealAmount/excluUseAr)/sum(cdealType)*10000,0) dealAvgAmount, count(*) cnt
            from tblReal r left join tblCode c1
                                on r.sggCd = c1.code
                        left join tblCode c2
                                on c1.p1_code = c2.code
            where sggCd = '{lawd_cd}'
            and   r.dealDate between '{start_date}' and '{end_date}'
            group by c1.p1_code, r.sggCd, strftime('%Y',dealDate), strftime('%m',dealDate), r.aptSeq
            having sum(cdealType*dealAmount/excluUseAr)<>0
            )
            group by p1_code, nm1, sggCd, dealYear, dealMon, nm2, aptSeq, aptNm, cnt 
            ) order by sggCd, dealYear,dealmon,aptSeq, rnk

            """
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit() 
        print("✅ insert_into_tblRealStatUnitMon():데이터 생성 완료")


# ----------------------------------------
# 실행 루프
# ----------------------------------------
if __name__ == '__main__':
    try:    
        df_input = pd.read_sql("select code, p1_code from tblCode where cha = 3 and isUsed in (1,3) order by code", con=engine)
        #df_input = pd.read_excel(INPUT_FILE, sheet_name=SHEET_NAME)
        # 2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016,2015,2014,2013,2012,2011,2010,2009,2008,2007
        #             
        # 2016(LAWD_CD:DEAL_YMD 26350 201612), 2019 X . 확인 필요함.  2017,2018, 2020, 2021,2022,2023,2024 완료, 2025년 1~3월(완료)
        
        from datetime import datetime, timedelta   
        curMon = datetime.now()    
        befMon = curMon.replace(day=1)  - timedelta(days=1)

        sCurMon = curMon.strftime('%Y%m')
        sBefMon = befMon.strftime('%Y%m')


        for idx, row in df_input.iterrows():
            lawd_cd = str(row['code'])
            p1_code = str(row.get('p1_code', '')).strip()

            #print("p1_code:", p1_code)
            if not p1_code or p1_code == '0.0' or p1_code == '0' or lawd_cd=='0':
                continue
             
            for y in range(2010,2014):  # 2020~2025, 2018~2019, 2015~2017, 2012~2014
                for m in range(1,13):
            #for deal_ymd in [sCurMon, sBefMon]:  # 현재 월과 이전 월만 처리
            #for deal_ymd in ['202201', '202202','202203','202204','202205','202206','202207','202208','202209','202210','202211','202212']:  # 테스트용
                #deal_ymd = f"{year}{str(month).zfill(2)}"                                          
                    deal_ymd = f"{y}{str(m).zfill(2)}"
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

                    #insert_into_tblReal(engine,lawd_cd, deal_ymd)

                    #insert_into_tblRealStatUnitMon(engine,lawd_cd, deal_ymd)

                    time.sleep(1)  # 요청 간격 조절

    except Exception as e:
        print(f"❌ 오류발생 : ({lawd_cd}, {deal_ymd}): {e}")
