# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import sqlite3
from datetime import date

# =============================
# 기본 설정
# =============================
DB_PATH = r"c:\doit\land\dbTHEH.db"

CITY_NAME_TO_P1_CODE = {
    "서울": "11000",
    "부산": "26000",
    "대구": "27000",
}

AREA_RANGES_M2 = [
    ("전체", None),
    ("0 ~ 60㎡", (0, 60)),
    ("60 ~ 85㎡", (60, 85)),
    ("85 ~ 102㎡", (85, 102)),
    ("102 ~ 135㎡", (102, 135)),
    ("135㎡ 이상", (135, None)),
]

# =============================
# 데이터 로드 함수
# =============================
@st.cache_data(show_spinner=False)
def load_codes_df() -> pd.DataFrame:
    with sqlite3.connect(DB_PATH, timeout=5) as conn:
        df = pd.read_sql_query(
            "SELECT code, nm, p1_code, cha, isUsed FROM tblCode", conn
        )
    return df


@st.cache_data(show_spinner=False)
def load_top_apartments(sgg_code: str, year: int, month: int) -> pd.DataFrame:
    with sqlite3.connect(DB_PATH, timeout=5) as conn:
        df = pd.read_sql_query(
            """
            SELECT rnk AS rank, sggCd, aptSeq, aptNm, dealYear, dealMon, dealAvgAmount, cnt
            FROM tblRealStatUnitMon
            WHERE sggCd = ? AND dealYear = ? AND dealMon = ?
            ORDER BY dealAvgAmount DESC, cnt DESC, rank ASC
            LIMIT 50
            """,
            conn,
            params=(sgg_code, year, month),
        )
    return df


@st.cache_data(show_spinner=False)
def load_volume_series(sgg_code: str, start_date: date, end_date: date, area_range: tuple | None) -> pd.DataFrame:
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    area_sql = ""
    params = [sgg_code, start_str, end_str]

    if area_range:
        lo, hi = area_range
        if lo is not None and hi is not None:
            area_sql = " AND excluUseAr >= ? AND excluUseAr < ?"
            params.extend([lo, hi])
        elif lo is not None:
            area_sql = " AND excluUseAr >= ?"
            params.append(lo)
        elif hi is not None:
            area_sql = " AND excluUseAr < ?"
            params.append(hi)

    with sqlite3.connect(DB_PATH, timeout=5) as conn:
        df = pd.read_sql_query(
            f"""
            SELECT substr(dealDate, 1, 7) AS ym, count(*) AS volume
            FROM tblReal
            WHERE sggCd = ?
              AND dealDate BETWEEN ? AND ?
              {area_sql}
            GROUP BY substr(dealDate, 1, 7)
            ORDER BY ym
            """,
            conn,
            params=params,
        )
    return df


@st.cache_data(show_spinner=False)
def load_sgg_list(p1_code: str) -> pd.DataFrame:
    codes = load_codes_df()
    mask = (
        (codes["p1_code"].astype(str) == str(p1_code))
        & (codes["cha"] == 3)
        & (codes["isUsed"].isin([1, 1.0]))
    )
    sgg = codes.loc[mask, ["code", "nm"]].sort_values("nm").reset_index(drop=True)
    return sgg


# =============================
# UI 레이아웃
# =============================
st.set_page_config(page_title="부동산 정보 대시보드", page_icon="🏠", layout="wide")
st.title("🏠 대한민국 부동산 정보")
st.caption("메뉴를 선택해 조회를 시작하세요.")

menu = st.sidebar.radio("메뉴", ["최고가 아파트", "거래량 조회"], index=0)

# 공통 코드 로드
try:
    codes_df = load_codes_df()
except Exception as e:
    st.warning(f"코드 테이블을 불러오는 데 실패했습니다: {e}")
    codes_df = pd.DataFrame(columns=["code", "nm"])

# =============================
# 메뉴 1: 최고가 아파트
# =============================
if menu == "최고가 아파트":
    st.subheader("💎 최고가 아파트")

    col1, col2, col3 = st.columns([1.2, 1, 1])

    with col1:
        city_name = st.selectbox("광역시/도 선택", list(CITY_NAME_TO_P1_CODE.keys()), index=0)
        p1_code = CITY_NAME_TO_P1_CODE[city_name]
        sgg_df = load_sgg_list(p1_code) if not codes_df.empty else pd.DataFrame(columns=["code", "nm"])

    with col2:
        if not sgg_df.empty:
            selected_row = st.selectbox(
                "시/군/구 선택",
                [tuple(x) for x in sgg_df[["code", "nm"]].to_records(index=False)],
                format_func=lambda x: f"{x[1]} ({x[0]})",
            )
            selected_sgg, selected_sgg_nm = selected_row
        else:
            selected_sgg, selected_sgg_nm = "", ""

    with col3:
        default_dt = date.today().replace(day=1)
        dt = st.date_input("기준 월", value=default_dt)
        year, month = dt.year, dt.month

    if selected_sgg:
        df_top = load_top_apartments(selected_sgg, year, month)
        if df_top.empty:
            st.info("해당 조건에 대한 통계 데이터가 없습니다.")
        else:
            st.markdown(f"**{selected_sgg_nm} {year}-{str(month).zfill(2)} 최고가 상위 아파트**")
            st.dataframe(df_top, use_container_width=True)

            chart_df = df_top.head(20).copy()
            chart_df["label"] = chart_df["rank"].astype(str) + ". " + chart_df["aptNm"].astype(str)
            st.bar_chart(chart_df.set_index("label")[["dealAvgAmount"]])
    else:
        st.info("좌측에서 시/군/구를 선택하세요.")

# =============================
# 메뉴 2: 거래량 조회
# =============================
if menu == "거래량 조회":
    st.subheader("📈 거래량 조회")

    col1, col2, col3 = st.columns([1.2, 1, 1])
    today = date.today()

    with col1:
        city_name2 = st.selectbox("광역시/도 선택", list(CITY_NAME_TO_P1_CODE.keys()), index=0, key="city2")
        p1_code2 = CITY_NAME_TO_P1_CODE[city_name2]
        sgg_df2 = load_sgg_list(p1_code2) if not codes_df.empty else pd.DataFrame(columns=["code", "nm"])

    with col2:
        if not sgg_df2.empty:
            selected_row2 = st.selectbox(
                "시/군/구 선택",
                [tuple(x) for x in sgg_df2[["code", "nm"]].to_records(index=False)],
                format_func=lambda x: f"{x[1]} ({x[0]})",
                key="sgg2",
            )
            selected_sgg2, selected_sgg_nm2 = selected_row2
        else:
            selected_sgg2, selected_sgg_nm2 = "", ""

    with col3:
        area_label = st.selectbox("전용면적(㎡)", [name for name, _ in AREA_RANGES_M2], index=0)
        area_range = next((rng for name, rng in AREA_RANGES_M2 if name == area_label), None)

    col4, col5 = st.columns(2)
    with col4:
        start_dt = st.date_input("시작 월", value=date(today.year, 1, 1), key="start")
    with col5:
        end_dt = st.date_input("종료 월", value=today, key="end")

    if selected_sgg2:
        if start_dt > end_dt:
            st.warning("시작 월이 종료 월보다 이후입니다. 기간을 확인해주세요.")
        else:
            df_vol = load_volume_series(selected_sgg2, start_dt, end_dt, area_range)
            if df_vol.empty:
                st.info("해당 조건에 대한 거래가 없습니다.")
            else:
                st.markdown(f"**{selected_sgg_nm2} {start_dt.strftime('%Y-%m')} ~ {end_dt.strftime('%Y-%m')} 거래량**")
                st.line_chart(df_vol.set_index("ym")[["volume"]], use_container_width=True)
                with st.expander("원본 데이터"):
                    st.dataframe(df_vol, use_container_width=True)
    else:
        st.info("좌측에서 시/군/구를 선택하고 기간을 지정하세요.")
