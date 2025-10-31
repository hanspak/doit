#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
월별 평당 거래가와 변동율 시각화 스크립트

사용법:
    python plot_real_estate.py --sggCd 11110 --aptSeq 11680-4394 [--db 경로] [--save 그림파일.png]

옵션:
    --sggCd     : 구/시군구 코드 (예: 11110)
    --aptSeq    : 아파트 단지 시퀀스 키 (예: 11680-4394)
    --db        : SQLite DB 경로 (기본: c:/doit/land/dbTHEH.db)
    --save      : 그래프를 저장할 파일 경로 (지정하지 않으면 화면에 표시)
"""
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import argparse
from datetime import datetime

def main():
    parser = argparse.ArgumentParser(description='월별 평당 거래가 및 변동율 시각화')
    parser.add_argument('--sggCd',   required=True, help='구/시군구 코드')
    parser.add_argument('--aptSeq',  required=True, help='아파트 단지 시퀀스')
    parser.add_argument('--db',      default=r'c:/doit/land/dbTHEH.db', help='SQLite DB 경로')
    parser.add_argument('--save',    help='그래프 저장 파일 경로 (예: output.png)')
    args = parser.parse_args()
    # 데이터 로드
    conn = sqlite3.connect(args.db)
    query = f"""
    SELECT dealYear, dealMon, dealAvgAmount
    FROM tblRealStatAptMon
    WHERE sggCd   = '{args.sggCd}'
      AND aptSeq  = '{args.aptSeq}'
    ORDER BY dealYear, dealMon
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        print('⚠️ 지정한 sggCd 및 aptSeq에 해당하는 데이터가 없습니다.')
        return

    # 날짜 및 변동율 계산
    df['date']       = pd.to_datetime(df['dealYear'] + df['dealMon'], format='%Y%m')
    df['pct_change'] = df['dealAvgAmount'].pct_change() * 100  # %

    # 그래프 그리기
    fig, ax = plt.subplots()
    ax.plot(df['date'], df['dealAvgAmount'], marker='o', label='평당 거래가')
    ax.set_xlabel('Date')
    ax.set_ylabel('평당 거래가 (만원)')
    ax.legend(loc='upper left')

    ax2 = ax.twinx()
    ax2.bar(df['date'], df['pct_change'], alpha=0.3, label='변동율(%)')
    ax2.set_ylabel('변동율 (%)')
    ax2.legend(loc='upper right')

    fig.autofmt_xdate()
    plt.title(f"{args.sggCd} - {args.aptSeq} 월별 평당 거래가 및 변동율")
    plt.tight_layout()

    if args.save:
        plt.savefig(args.save)
        print(f"✅ 그래프를 '{args.save}'에 저장했습니다.")
    else:
        plt.show()

if __name__ == '__main__':
    main()
