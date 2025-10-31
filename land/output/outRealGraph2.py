import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import font_manager, rc, ticker
from matplotlib.ticker import StrMethodFormatter
import matplotlib.dates as mdates
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy import text
from datetime import datetime

DB_PATH = Path("c:/doit/land/dbTHEH.db")

s_date = '2018-01-01'
e_date = '2025-12-31'
c_year = datetime.strptime(e_date,'%Y-%m-%d').year - datetime.strptime(s_date,'%Y-%m-%d').year + 1  

title = str(c_year) + '년 실거래가 그래프'

dangi1 = '11710-6346'
dangi2 = '41290-181'  #래미안 슈르
#dangi2 = '11260-3783'  #e편한세상화랑대

# DB 연결
engine = create_engine(f"sqlite:///{DB_PATH}")


sql11 = f"""
    SELECT max(a.aptNm) aptNm, substr(a.dealDate,1,7) date, round(avg(a.dealAmount),0) price, count(*) volume
    from tblReal a
    where dealDate between '{s_date}' and '{e_date}'
    --and sggCd = '11710'
    and aptSeq in ('{dangi1}')
    and useArea in ( 25.7)
    group by substr(a.dealDate,1,7)
"""

sql12 = f"""
    SELECT a.aptNm, substr(a.dealDate,1,7) date, a.dealAmount price
    from tblReal a
    where dealDate between '{s_date}' and '{e_date}'
    --and sggCd = '11710'
    and aptSeq in ('{dangi1}')
    and useArea in ( 25.7)
"""

sql21 = f"""
    SELECT max(a.aptNm) aptNm, substr(a.dealDate,1,7) date, round(avg(a.dealAmount),0) price, count(*) volume
    from tblReal a
    where dealDate between '{s_date}' and '{e_date}'
    --and sggCd = '41290'
    and aptSeq = '{dangi2}'
    and useArea in ( 25.7)
    group by substr(a.dealDate,1,7)
"""

sql22 = f"""
    SELECT a.aptNm, substr(a.dealDate,1,7) date, a.dealAmount price
    from tblReal a
    where dealDate between '{s_date}' and '{e_date}'
    --and sggCd = '41290'
    and aptSeq = '{dangi2}'
    and useArea in ( 25.7)
"""

data11 = pd.read_sql(sql11, con=engine)
data12 = pd.read_sql(sql12, con=engine)

data21 = pd.read_sql(sql21, con=engine)
data22 = pd.read_sql(sql22, con=engine)


df11 = pd.DataFrame(data11)
df12 = pd.DataFrame(data12)

df11['c_date'] = pd.to_datetime(df11['date'])
df12['c_date'] = pd.to_datetime(df12['date'])

df21 = pd.DataFrame(data21)
df22 = pd.DataFrame(data22)

df21['c_date'] = pd.to_datetime(df21['date'])
df22['c_date'] = pd.to_datetime(df22['date'])


plt.rcParams['font.family'] = 'Malgun Gothic'   # 윈도우용
plt.rcParams['axes.unicode_minus'] = False      # 마이너스 부호 깨짐 방지


# 그래프
fig, ax1 = plt.subplots(figsize=(8,4))

# 평균 가격 선1
ax1.plot(df11['c_date'], df11['price'], color='royalblue', linewidth=1, label='평균가격(만)')
ax1.set_ylabel('가격(만)', color='black')
ax1.tick_params(axis='y', labelcolor='black')

# 평균 가격 선2
ax1.plot(df21['c_date'], df21['price'], color='orange', linewidth=1, label='평균가격(만)')

# 실거래 점1
ax1.plot(df12['c_date'], df12['price'],
         marker='.', linestyle='None', color='tab:brown', label='가격')

# 실거래 점2
ax1.plot(df22['c_date'], df22['price'],
         marker='.', linestyle='None', color='tab:green', label='가격')

# 🟦 막대 관련 설정
bar_width = 10           # 막대 두께
offset_days = 14          # 막대 좌우 이동 폭 (날짜 단위)

# 거래량 막대1
ax2 = ax1.twinx()
ax2.bar(df11['c_date'] + pd.Timedelta(days=2) , df11['volume'], alpha=0.5, color='royalblue', label=df11['aptNm'][1], width=bar_width)
ax2.set_ylabel('거래량1', color='black')
ax2.tick_params(axis='y', labelcolor='black')
max_value = round(df11['volume'].max(),-1)
ax2.set_ylim(0, max_value*2)

# 거래량 막대2
ax2.bar(df21['c_date'] + pd.Timedelta(days=offset_days), df21['volume'], alpha=0.5, color='orange', label=df21['aptNm'][1], width=bar_width)

# 강조 포인트1
max_idx = df12['price'].idxmax()
min_idx = df12['price'].idxmin()

ax1.scatter(df12['c_date'][max_idx], df12['price'][max_idx], color='red', s=100, label='최고')
ax1.scatter(df12['c_date'][min_idx], df12['price'][min_idx], color='yellow', s=50, label='최저')

# 강조 포인트2
max_idx = df22['price'].idxmax()
min_idx = df22['price'].idxmin()

ax1.scatter(df22['c_date'][max_idx], df22['price'][max_idx], color='red', s=100, label='최고')
ax1.scatter(df22['c_date'][min_idx], df22['price'][min_idx], color='yellow', s=50, label='최저')

plt.title(title)
plt.legend(loc="upper left")  # 왼쪽 상단에 배치
plt.grid(alpha=0.3)

# 👉 X축 연도별로 한 번만 표시
ax1.xaxis.set_major_locator(mdates.YearLocator())              # 1년 단위 눈금
#ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))      # 연도만 표시
#ax1.xaxis.set_minor_locator(mdates.MonthLocator())             # (선택) 보조 눈금으로 월 단위
ax1.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))

ax2.yaxis.set_major_locator(ticker.MaxNLocator(integer=True)) # 거래량 y축 눈금을 정수 단위로 표시.
ax2.yaxis.set_major_locator(ticker.MultipleLocator(10))  # 5 단위로 눈금 표시

plt.tight_layout()

plt.setp(ax1.get_xticklabels(), rotation=45, ha='right')

plt.show()