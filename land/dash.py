# app.py
import streamlit as st
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt

# 데이터 불러오기 함수
@st.cache_data
def load_data():
    db_path = r'c:\doit\land\dbTHEH.db'      # ← SQLite DB 경로
    conn = sqlite3.connect(db_path)
    sql = r"select km.code, c.nm, km.year, km.mon, km.year||'-'||km.mon as dt, km.kind, round(km.price,2) price "
    sql = sql + r"from tblKBMon km left join tblcode c "
    sql = sql + r"	on km.code = c.code "
    sql = sql + r"where km.code = '11000' "
    sql = sql + r"order by km.code, km.year, km.mon, km.kind "

    df = pd.read_sql(sql, conn)
    conn.close()
    df['dt'] = pd.to_datetime(df['dt'])
    return df

# 데이터 로딩
df = load_data()

# 제목
st.title("📊 아파트 매매 지수 대시보드")

# 지역 선택
codes = df['code'].unique()
selected_code = st.selectbox("지역 코드 선택", codes)

# 필터링
filtered = df[df['code'] == selected_code]

# 선 그래프
st.subheader(f"지역 코드 {selected_code}의 시계열 추이")
fig, ax = plt.subplots()
ax.plot(filtered['DATE'], filtered['price'], marker='o')
ax.set_xlabel("날짜")
ax.set_ylabel("매매지수")
ax.grid(True)
st.pyplot(fig)

# 데이터 테이블 보기
with st.expander("📄 원시 데이터 보기"):
    st.dataframe(filtered)