import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy import text

DB_PATH = Path("c:/doit/land/dbTHEH.db")

# DB 연결
engine = create_engine(f"sqlite:///{DB_PATH}")

sql1 = """
    SELECT substr(a.dealDate,1,7) date, round(avg(a.dealAmount),0) price, count(*) volume
    from tblReal a
    where dealDate between '2023-01-01' and '2025-12-31'
    and sggCd = '11260'
    and aptSeq = '11260-3783'
    and useArea in ( 25.7)
    group by substr(a.dealDate,1,7) 
"""

sql2 = """
    SELECT substr(a.dealDate,1,7) date, a.dealAmount price
    from tblReal a
    where dealDate between '2023-01-01' and '2025-12-31'
    and sggCd = '11260'
    and aptSeq = '11260-3783'
    and useArea in ( 25.7)
"""




data1 = pd.read_sql(sql1, con=engine)

data2 = pd.read_sql(sql2, con=engine)

# 예시 데이터
#data = {
#    'date': pd.date_range('2023-01-01', '2025-10-01', freq='MS'),
#    'price': [20 + i*0.6 + (i%4-2)*0.3 for i in range(34)],   # 임의 상승 데이터
#    'volume': [300 + (i%6)*50 for i in range(34)]
#}
df1 = pd.DataFrame(data1)
df2 = pd.DataFrame(data2)

#print(df)

# 그래프
fig, ax1 = plt.subplots(figsize=(8,4))

# 평균 가격 선
ax1.plot(df1['date'], df1['price'], color='royalblue', linewidth=2.5, label='평균가격(억)')
ax1.set_ylabel('가격(억)', color='royalblue')
ax1.tick_params(axis='y', labelcolor='royalblue')

ax1.plot(df2['date'], df2['price'], 
         marker='o', linestyle='None', color='tab:blue', label='가격')


# 거래량 막대
ax2 = ax1.twinx()
ax2.bar(df1['date'], df1['volume'], alpha=0.2, color='gray', label='거래량')
ax2.set_ylabel('거래량', color='gray')
ax2.tick_params(axis='y', labelcolor='gray')

# 강조 포인트   
max_idx = df2['price'].idxmax()
min_idx = df2['price'].idxmin()

ax1.scatter(df2['date'][max_idx], df2['price'][max_idx], color='red', s=50, label='최고')
ax1.scatter(df2['date'][min_idx], df2['price'][min_idx], color='navy', s=50, label='최저')

plt.title('최근 3년간 평균 거래가격 추이')
plt.legend()
plt.grid(alpha=0.3)
plt.tight_layout()
plt.show()
