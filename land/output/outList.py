import streamlit as st
import pandas as pd
import sqlite3

# -----------------------------------
# 0️⃣ DB 초기 설정
# -----------------------------------
DB_PATH = "realestate.db"

def create_table():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS tblBest (
            sggCd       TEXT    NOT NULL,
            aptSeq      TEXT    NOT NULL,
            rank        INTEGER NOT NULL,
            excluUseAr  REAL    NOT NULL,
            useArea     REAL,
            aptNm       TEXT,
            PRIMARY KEY (sggCd, aptSeq, rank, excluUseAr)
        );
        """)
create_table()

# -----------------------------------
# 1️⃣ Streamlit 기본 UI
# -----------------------------------
st.set_page_config(page_title="최고가 아파트 순위", page_icon="🏠", layout="centered")
st.title("🏠 최고가 아파트 순위")
st.caption("출처: 국토부 실거래가 데이터")

# -----------------------------------
# 2️⃣ 데이터 예시
# -----------------------------------
data = [
    ["11680", "A001", 1, 80.18, 80.18, "현대6,7차", "서울 강남구 압구정동", 130.95],
    ["11680", "A002", 2, 64.13, 64.13, "현대1,2차", "서울 강남구 압구정동", 127.0],
    ["11680", "A003", 3, 78.1, 78.1, "효성빌라청담101", "서울 강남구 청담동", 113.0],
    ["11680", "A004", 4, 79.82, 79.82, "신현대(현대9,11,12차)", "서울 강남구 압구정동", 112.95],
    ["11680", "A005", 5, 73.5, 73.5, "아이파크삼성", "서울 강남구 삼성동", 98.0],
    ["11680", "A006", 6, 68.2, 68.2, "한양4사", "서울 강남구 압구정동", 88.07],
]
cols = ["sggCd", "aptSeq", "rank", "excluUseAr", "useArea", "aptNm", "주소", "거래금액(억원)"]
df = pd.DataFrame(data, columns=cols)

# -----------------------------------
# 3️⃣ 체크박스로 선택
# -----------------------------------
st.subheader("✅ 관심 아파트 선택")

checked_rows = []
for _, row in df.iterrows():
    checked = st.checkbox(
        f"{row['rank']}위 | {row['aptNm']} ({row['주소']}) - {row['거래금액(억원)']}억",
        key=f"check_{row['aptSeq']}"
    )
    if checked:
        checked_rows.append(row)

# -----------------------------------
# 4️⃣ SQLite에 저장
# -----------------------------------
if st.button("💾 선택한 아파트 DB에 저장"):
    if not checked_rows:
        st.warning("선택된 항목이 없습니다.")
    else:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            inserted = 0
            for row in checked_rows:
                try:
                    cur.execute("""
                        INSERT OR REPLACE INTO tblBest (sggCd, aptSeq, rank, excluUseAr, useArea, aptNm)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (row['sggCd'], row['aptSeq'], row['rank'], row['excluUseAr'], row['useArea'], row['aptNm']))
                    inserted += 1
                except Exception as e:
                    st.error(f"{row['aptNm']} 저장 중 오류: {e}")
            conn.commit()
        st.success(f"✅ {inserted}건이 tblBest 테이블에 저장되었습니다!")

# -----------------------------------
# 5️⃣ DB 조회 (확인용)
# -----------------------------------
if st.checkbox("📋 DB 내용 보기"):
    with sqlite3.connect(DB_PATH) as conn:
        saved_df = pd.read_sql_query("SELECT * FROM tblBest", conn)
    st.dataframe(saved_df)
