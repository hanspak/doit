# -*- coding: utf-8 -*-
"""
- tbl_real_estate 임시 파일에 있는 데이터를 tblReal 테이블로 값 이전
- tblRealStatAptMon 테이블에 통계 데이터 생성
"""
import os
import pandas as pd
import time
from sqlalchemy import create_engine
from pathlib import Path
from datetime import datetime

# ----------------------------------------
# 설정
# ----------------------------------------
DB_PATH = Path(os.environ["DB_PATH"])  # C:\db\dbTHEH.db
SRC_TABLE_NAME = "tbl_real_estate"
TG_TABLE_NAME = "tblReal"
STAT_TABLE_NAME = "tblRealStatAptMon"


# DB 연결
engine = create_engine(f"sqlite:///{DB_PATH}")


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
