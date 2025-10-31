# -*- coding: utf-8 -*-
"""
부동산 실거래가 자동 수집기 (확장형)
- 국토교통부 OpenAPI 호출
- XML → DataFrame 변환
- SQLite 저장
- 중복 방지
- 로그 저장
- 자동 수집 범위 설정
"""

import requests
import xml.etree.ElementTree as ET
import pandas as pd
import time
import logging
from sqlalchemy import create_engine, text
from pathlib import Path
from datetime import datetime

# ----------------------------------------
# 설정
# ----------------------------------------
DB_PATH = Path("c:/doit/land/dbTHEH.db")
INPUT_FILE = Path("c:/doit/land/code.xlsx")
SHEET_NAME = "list"
SERVICE_KEY = "UIRiUedSrUkb68ntUOKaIkSoppX9Pc1aY3jLtDG4OYFF6PJU7P3pPDybDDYgzaPDGG+0O4lkmzMhz7/o5J0oWw=="
TABLE_NAME = "tbl_real_estate_tmp2"
NUM_OF_ROWS = 9999
LOG_FILE = "collector.log"

# ----------------------------------------
# 로깅 설정
# ----------------------------------------
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(message)s')
log = logging.getLogger()

# DB 연결
engine = create_engine(f"sqlite:///{DB_PATH}")

# ----------------------------------------
# 자동 수집 기준 최근 등록된 DEAL_YMD 확인
# ----------------------------------------
def get_latest_deal_ym(engine, table):
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT MAX(계약년도 || substr('0' || 계약월, -2, 2)) FROM {table}"))
        latest = result.scalar()
        return latest or "202201"  # 기본값 설정

# ----------------------------------------
# API 호출 함수
# ----------------------------------------
def fetch_trade_data(service_key, lawd_cd, deal_ymd, page_no=1, num_of_rows=NUM_OF_ROWS):
    url = "http://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
    params = {
        'serviceKey': service_key,
        'pageNo': page_no,
        'numOfRows': num_of_rows,
        'LAWD_CD': lawd_cd,
        'DEAL_YMD': deal_ymd
    }

    try:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)

        result_code = root.findtext(".//resultCode")
        if result_code != "000":
            msg = root.findtext(".//returnAuthMsg") or "API 오류 발생"
            raise ValueError(f"[API Error] {msg}")

        items = root.findall('.//item')
        records = []
        for item in items:
            record = {elem.tag: elem.text for elem in item}
            record['LAWD_CD'] = lawd_cd
            record['DEAL_YMD'] = deal_ymd
            records.append(record)

        return pd.DataFrame(records)

    except Exception as e:
        log.error(f"API 실패: {lawd_cd}-{deal_ymd} / {e}")
        return pd.DataFrame()

# ----------------------------------------
# DB 저장 함수 (중복 방지 포함)
# ----------------------------------------
def save_to_sqlite(df: pd.DataFrame, engine, table: str):
    if df.empty:
        log.warning("빈 데이터, 저장 생략")
        return

    # 중복 제거 (계약일, 전용면적, 층, 단지명 기준)
    df.drop_duplicates(subset=["dealDay", "excluUseAr", "floor", "aptNm"], inplace=True)

    with engine.begin() as conn:
        df.to_sql(table, con=conn, if_exists='append', index=False)
        log.info(f"저장 완료: {len(df)}건")

# ----------------------------------------
# 실행 루프
# ----------------------------------------
if __name__ == '__main__':
    df_input = pd.read_excel(INPUT_FILE, sheet_name=SHEET_NAME)
    latest_ym = get_latest_deal_ym(engine, TABLE_NAME)
    latest_year = int(latest_ym[:4])
    latest_month = int(latest_ym[4:6])

    for idx, row in df_input.iterrows():
        lawd_cd = str(row['code'])
        p1_code = str(row.get('p1_code', '')).strip()
        if not p1_code or p1_code == '0':
            continue

        for year in range(latest_year, datetime.now().year + 1):
            start_month = latest_month if year == latest_year else 1
            for month in range(start_month, 13):
                deal_ymd = f"{year}{str(month).zfill(2)}"
                print(f"🔍 요청중: LAWD_CD={lawd_cd}, DEAL_YMD={deal_ymd}")

                df = fetch_trade_data(SERVICE_KEY, lawd_cd, deal_ymd)
                save_to_sqlite(df, engine, TABLE_NAME)
                time.sleep(0.5)
