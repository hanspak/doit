import sqlite3
import pandas as pd
from datetime import datetime

# 1. SQLite 연결
db_path = r'c:/doit/land/dbTHEH.db'
conn = sqlite3.connect(db_path)

# 2. 전체 데이터 로드
df = pd.read_sql_query("SELECT * FROM tblRealStatAptMon", conn)

# 3. 'YYYYMM' → datetime 변환
df['date'] = pd.to_datetime(df['dealYear'] + df['dealMon'], format='%Y%m')

filled_rows = []
# 4. 단지별 그룹 순회
for (sggCd, aptSeq), grp in df.groupby(['sggCd', 'aptSeq']):
    all_dates = pd.date_range(grp['date'].min(), grp['date'].max(), freq='MS')
    grp = grp.set_index('date').sort_index()
    reidx = grp.reindex(all_dates, method='ffill')
    reidx['sggCd'] = sggCd
    reidx['aptSeq'] = aptSeq

    # 5. 누락된 월만 골라내기
    missing = reidx[~reidx.index.isin(grp.index)].copy()
    if not missing.empty:
        missing['dealYear'] = missing.index.year.astype(str)
        missing['dealMon'] = missing.index.month.astype(str).str.zfill(2)
        missing['cnt'] = 0
        missing['createdDate'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # 전월 데이터를 그대로 복사
        for col in ['p1_code','nm1','nm2','aptNm','dealAvgAmount','rnk']:
            missing[col] = missing[col].ffill()
        filled_rows.append(missing)

# 6. DB에 삽입
if filled_rows:
    to_insert = pd.concat(filled_rows)
    to_insert = to_insert[['sggCd','aptSeq','p1_code','nm1','nm2','aptNm',
                           'dealYear','dealMon','dealAvgAmount','cnt','rnk','createdDate']]
    to_insert.to_sql('tblRealStatAptMon', conn, if_exists='append', index=False)
    print(f"Inserted {len(to_insert)} missing rows.")
else:
    print("No missing months to insert.")

conn.close()
