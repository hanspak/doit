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

title = str(c_year) + '년 KB 지수'

sigun1 = '11110'  #종로구
#sigun2 = '11140'  #중구
sigun2 = '11680'  #강남구
#dangi2 = '11260-3783'  #e편한세상화랑대

# DB 연결
engine = create_engine(f"sqlite:///{DB_PATH}")


sql11 = f"""
    select b.nm, a.date, a.price
    from tblKBWeek a left join tblCode b 
                on a.code = b.code
    where a.kind = '21'            
    and date between   '{s_date}' and '{e_date}'
    and a.code = '{sigun1}'
"""

sql12 = f"""
    select b.nm, a.date, a.price
    from tblKBWeek a left join tblCode b 
                on a.code = b.code
    where a.kind = '22'            
    and date between   '{s_date}' and '{e_date}'
    and a.code = '{sigun1}'
    """

sql21 = f"""
    select b.nm, a.date, a.price
    from tblKBWeek a left join tblCode b 
                on a.code = b.code
    where a.kind = '21'            
    and date between   '{s_date}' and '{e_date}'
    and a.code = '{sigun2}'
"""

sql22 = f"""
    select b.nm, a.date, a.price
    from tblKBWeek a left join tblCode b 
                on a.code = b.code
    where a.kind = '22'            
    and date between   '{s_date}' and '{e_date}'
    and a.code = '{sigun2}'
"""

data11 = pd.read_sql(sql11, con=engine)
data12 = pd.read_sql(sql12, con=engine)

data21 = pd.read_sql(sql21, con=engine)
data22 = pd.read_sql(sql22, con=engine)


df11 = pd.DataFrame(data11)
df11['c_date'] = pd.to_datetime(df11['Date'])

df12 = pd.DataFrame(data12)
df12['c_date'] = pd.to_datetime(df12['Date'])


df21 = pd.DataFrame(data21)
df21['c_date'] = pd.to_datetime(df21['Date'])

df22 = pd.DataFrame(data22)
df22['c_date'] = pd.to_datetime(df22['Date'])


plt.rcParams['font.family'] = 'Malgun Gothic'   # 윈도우용
plt.rcParams['axes.unicode_minus'] = False      # 마이너스 부호 깨짐 방지


# 그래프
fig, ax1 = plt.subplots(figsize=(8,4))



# 평균 가격 선1
ax1.plot(df11['c_date'], df11['price'], color='royalblue', linewidth=1, label=df11['nm'][0]+'매매가격지수')
ax1.set_ylabel('지수', color='black')
ax1.tick_params(axis='y', labelcolor='black')

ax1.plot(df12['c_date'], df12['price'], color='red', linewidth=1, label=df11['nm'][0]+'전세가격지수')

# 평균 가격 선2
ax1.plot(df21['c_date'], df21['price'], color='orange', linewidth=1, label=df21['nm'][0]+'매매가격지수')
ax1.plot(df22['c_date'], df22['price'], color='yellow', linewidth=1, label=df21['nm'][0]+'전세가격지수')
#ax1.set_ylabel('가격(억)', color='orange')
#ax1.tick_params(axis='y', labelcolor='orange')



plt.title(title)
plt.legend(loc="upper left")  # 왼쪽 상단에 배치
plt.grid(alpha=0.3)




# 👉 X축 연도별로 한 번만 표시
ax1.xaxis.set_major_locator(mdates.YearLocator())              # 1년 단위 눈금
#ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))      # 연도만 표시
#ax1.xaxis.set_minor_locator(mdates.MonthLocator())             # (선택) 보조 눈금으로 월 단위
#ax1.tick_params(axis='x', which='major', rotation=0)           # 라벨 회전 없음

# 👉 중복 제거 (같은 연도 여러 번 표시 방지)
#ax1.xaxis.set_tick_params(labelrotation=0)
#plt.setp(ax1.get_xticklabels(), ha='center')

# 3개월 단위로 라벨 표시
#ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=36))  
# x축의 날짜 형식 지정
#ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
# x축 눈금 간격(월 단위로 설정)
#ax1.xaxis.set_major_locator(mdates.MonthLocator())

ax1.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))


#ax2.yaxis.set_major_locator(ticker.MaxNLocator(integer=True)) # 거래량 y축 눈금을 정수 단위로 표시.
#ax2.yaxis.set_major_locator(ticker.MultipleLocator(10))  # 5 단위로 눈금 표시


plt.tight_layout()


# x축 라벨이 겹치지 않게 회전
#plt.xticks(rotation=45)
#plt.xticks(rotation=45, ha='right')
# 라벨 회전
plt.setp(ax1.get_xticklabels(), rotation=45, ha='right')


plt.show()
