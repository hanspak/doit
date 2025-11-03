# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import sqlite3
from datetime import date, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# =============================
# 기본 설정
# =============================
DB_PATH = r"c:\db\dbTHEH.db"

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
def load_top_apartments(sgg_code: str | None, start_date: date, end_date: date, area_range: tuple | None = None) -> pd.DataFrame:
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    area_sql = ""
    sgg_sql = ""
    params = []
    
    if sgg_code:
        sgg_sql = "r.sggCd = ?"
        params.append(sgg_code)
    
    params.extend([start_str, end_str])
    
    if area_range:
        lo, hi = area_range
        if lo is not None and hi is not None:
            area_conditions = ["r.excluUseAr >= ?", "r.excluUseAr < ?"]
            params.extend([lo, hi])
        elif lo is not None:
            area_conditions = ["r.excluUseAr >= ?"]
            params.append(lo)
        elif hi is not None:
            area_conditions = ["r.excluUseAr < ?"]
            params.append(hi)
        else:
            area_conditions = []
    else:
        area_conditions = []
    
    where_conditions = []
    if sgg_sql:
        where_conditions.append(sgg_sql)
    where_conditions.append("r.dealDate >= ?")
    where_conditions.append("r.dealDate <= ?")
    where_conditions.append("r.cdealType = 1")
    where_conditions.extend(area_conditions)
    
    where_clause = " AND ".join(where_conditions)
    
    query_template = f"""
            WITH ranked_apartments AS (
                SELECT 
                    r.sggCd,
                    r.aptSeq,
                    r.aptNm,
                    MAX(r.dealAmount) AS max_deal_amount
                FROM tblReal r
                WHERE {where_clause}
                GROUP BY r.sggCd, r.aptSeq, r.aptNm
                ORDER BY max_deal_amount DESC
                LIMIT 50
            ),
            top_deals AS (
                SELECT 
                    r.rowid,
                    ra.sggCd,
                    ra.aptSeq,
                    ra.aptNm,
                    r.excluUseAr,
                    r.dealAmount,
                    r.dealDate,
                    ROW_NUMBER() OVER (
                        PARTITION BY ra.sggCd, ra.aptSeq, ra.aptNm 
                        ORDER BY r.dealAmount DESC, r.dealDate DESC
                    ) AS rn
                FROM ranked_apartments ra
                INNER JOIN tblReal r 
                    ON ra.sggCd = r.sggCd 
                    AND ra.aptSeq = r.aptSeq 
                    AND ra.aptNm = r.aptNm
                    AND r.dealAmount = ra.max_deal_amount
                    AND r.dealDate >= ?
                    AND r.dealDate <= ?
                    AND r.cdealType = 1
            )
            SELECT 
                ROW_NUMBER() OVER (ORDER BY td.dealAmount DESC, td.dealDate DESC) AS rank,
                c2.nm AS 시도,
                c1.nm AS 시군구,
                td.sggCd AS sggCd,
                td.aptSeq AS aptSeq,
                td.aptNm AS 아파트명,
                td.excluUseAr AS 면적,
                td.dealAmount AS 거래금액,
                td.dealDate AS 거래일자
            FROM top_deals td
            LEFT JOIN tblCode c1 ON td.sggCd = c1.code
            LEFT JOIN tblCode c2 ON c1.p1_code = c2.code
            WHERE td.rn = 1
            ORDER BY td.dealAmount DESC, td.dealDate DESC
            """
    
    # 파라미터를 실제 값으로 치환한 쿼리 생성 (표시용)
    all_params = params + [start_str, end_str]
    # 파라미터 위치 찾기
    param_positions = []
    for i, char in enumerate(query_template):
        if char == '?':
            param_positions.append(i)
    
    # 각 ? 위치에 파라미터 값을 삽입
    result_parts = []
    last_idx = 0
    for i, pos in enumerate(param_positions):
        if i < len(all_params):
            result_parts.append(query_template[last_idx:pos])
            param = all_params[i]
            if isinstance(param, str):
                result_parts.append(f"'{param}'")
            else:
                result_parts.append(str(param))
            last_idx = pos + 1
    result_parts.append(query_template[last_idx:])
    display_query = ''.join(result_parts)
    
    with sqlite3.connect(DB_PATH, timeout=5) as conn:
        df = pd.read_sql_query(
            query_template,
            conn,
            params=all_params,
        )
    
    # 쿼리를 전역 변수나 st.session_state에 저장하여 나중에 표시
    if 'last_query' not in st.session_state:
        st.session_state.last_query = {}
    st.session_state.last_query['load_top_apartments'] = display_query.strip()
    
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
def load_p1_list() -> pd.DataFrame:
    """광역시/도 목록을 반환한다. cha=0(전국)과 cha=1(광역시/도)인 모든 데이터를 반환한다."""
    codes = load_codes_df()
    mask = (codes["cha"] == 0) | (codes["cha"] == 1)
    p1_list = codes.loc[mask, ["code", "nm"]].sort_values("code").reset_index(drop=True)
    return p1_list


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
# 추가 데이터 로드 함수: 단지 목록, 시세 추이
# =============================
@st.cache_data(show_spinner=False)
def load_apartment_yearly_stats(sgg_code: str, apt_seq: str, apt_nm: str) -> pd.DataFrame:
    """해당 아파트의 년도별 평균가격과 거래량을 반환한다."""
    with sqlite3.connect(DB_PATH, timeout=5) as conn:
        df = pd.read_sql_query(
            """
            SELECT 
                strftime('%Y-%m', dealDate) AS dealYear,
                ROUND(AVG(dealAmount), 0) AS avgPrice,
                COUNT(*) AS volume
            FROM tblReal
            WHERE sggCd = ?
              AND aptSeq = ?
              AND aptNm = ?
              AND cdealType = 1
            GROUP BY strftime('%Y-%m', dealDate)
            ORDER BY dealYear
            """,
            conn,
            params=(sgg_code, apt_seq, apt_nm),
        )
    return df


@st.cache_data(show_spinner=False)
def load_apartments_in_sgg(sgg_code: str) -> pd.DataFrame:
    """해당 시/군/구의 아파트 목록을 반환한다."""
    with sqlite3.connect(DB_PATH, timeout=5) as conn:
        df = pd.read_sql_query(
            """
            SELECT DISTINCT aptSeq, aptNm
            FROM tblReal
            WHERE sggCd = ?
            ORDER BY aptNm
            """,
            conn,
            params=(sgg_code,),
        )
    return df


@st.cache_data(show_spinner=False)
def load_price_trend(sgg_code: str, apt_seq: str, start_date: date | None, end_date: date | None) -> pd.DataFrame:
    """해당 아파트의 연월별 평균 거래금액 추이를 반환한다."""
    date_filter_sql = ""
    params: list = [sgg_code, apt_seq]

    if start_date is not None:
        date_filter_sql += " AND dealDate >= ?"
        params.append(start_date.strftime("%Y-%m-01"))

    if end_date is not None:
        # end_date의 월 마지막 날까지 포함
        if end_date.month == 12:
            last_day = date(end_date.year, 12, 31)
        else:
            next_month_first = date(end_date.year, end_date.month + 1, 1)
            last_day = next_month_first - timedelta(days=1)
        date_filter_sql += " AND dealDate <= ?"
        params.append(last_day.strftime("%Y-%m-%d"))

    with sqlite3.connect(DB_PATH, timeout=5) as conn:
        df = pd.read_sql_query(
            f"""
            SELECT 
                strftime('%Y', dealDate) AS dealYear,
                strftime('%m', dealDate) AS dealMon,
                ROUND(AVG(dealAmount), 0) AS dealAvgAmount
            FROM tblReal
            WHERE sggCd = ?
              AND aptSeq = ?
              {date_filter_sql}
            GROUP BY strftime('%Y', dealDate), strftime('%m', dealDate)
            ORDER BY dealYear, dealMon
            """,
            conn,
            params=params,
        )
    if not df.empty:
        df["ym"] = (
            df["dealYear"].astype(str)
            + "-"
            + df["dealMon"].astype(str).str.zfill(2)
        )
    return df


# =============================
# UI 레이아웃
# =============================
st.set_page_config(page_title="부동산 정보 대시보드", page_icon="🏠", layout="wide")
st.title("🏠 대한민국 부동산 정보")
st.caption("메뉴를 선택해 조회를 시작하세요.")

menu = st.sidebar.radio("메뉴", ["최고가 아파트", "거래량 조회", "시세 추이"], index=0)

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

    # 광역시/도 목록 로드
    p1_df = load_p1_list() if not codes_df.empty else pd.DataFrame(columns=["code", "nm"])

    col1, col2, col3, col4 = st.columns([1.2, 1, 1.2, 1.2])

    with col1:
        if not p1_df.empty:
            selected_p1_row = st.selectbox(
                "광역시/도 선택",
                [tuple(x) for x in p1_df[["code", "nm"]].to_records(index=False)],
                format_func=lambda x: f"{x[1]} ({x[0]})",
            )
            p1_code, p1_nm = selected_p1_row
            # 전국인지 확인 (cha=0)
            is_nationwide = codes_df[codes_df["code"] == p1_code]["cha"].values[0] == 0 if not codes_df.empty and p1_code in codes_df["code"].values else False
            if not is_nationwide:
                sgg_df = load_sgg_list(p1_code)
            else:
                sgg_df = pd.DataFrame(columns=["code", "nm"])
        else:
            p1_code, p1_nm = "", ""
            is_nationwide = False
            sgg_df = pd.DataFrame(columns=["code", "nm"])

    with col2:
        if not is_nationwide:
            if not sgg_df.empty:
                selected_row = st.selectbox(
                    "시/군/구 선택",
                    [tuple(x) for x in sgg_df[["code", "nm"]].to_records(index=False)],
                    format_func=lambda x: f"{x[1]} ({x[0]})",
                )
                selected_sgg, selected_sgg_nm = selected_row
            else:
                selected_sgg, selected_sgg_nm = "", ""
        else:
            st.info("전국 선택 시 시/군/구 선택이 필요하지 않습니다.")
            selected_sgg, selected_sgg_nm = None, "전국"

    with col3:
        today = date.today()
        start_dt = st.date_input("시작일자", value=date(today.year, today.month, 1))
    
    with col4:
        today = date.today()
        end_dt = st.date_input("마지막일자", value=today)
    
    area_label = st.selectbox("전용면적(㎡)", [name for name, _ in AREA_RANGES_M2], index=0)
    area_range = next((rng for name, rng in AREA_RANGES_M2 if name == area_label), None)

    if selected_sgg or is_nationwide:
        if start_dt > end_dt:
            st.warning("시작일자가 마지막일자보다 이후입니다. 기간을 확인해주세요.")
        else:
            df_top = load_top_apartments(selected_sgg, start_dt, end_dt, area_range)
            if df_top.empty:
                st.info("해당 조건에 대한 통계 데이터가 없습니다.")
            else:
                area_info = f" ({area_label})" if area_label != "전체" else ""
                region_info = selected_sgg_nm if selected_sgg else "전국"
                st.markdown(f"**{region_info} {start_dt.strftime('%Y-%m-%d')} ~ {end_dt.strftime('%Y-%m-%d')}{area_info} 최고가 상위 아파트**")
                
                # 단지 선택을 위한 selectbox
                apt_options = [f"{row['rank']}. {row['아파트명']} ({row['시도']} {row['시군구']})" for _, row in df_top.iterrows()]
                selected_apt_idx = st.selectbox(
                    "📌 단지 선택 (년도별 통계 그래프를 보려면 선택하세요)",
                    options=range(len(apt_options)),
                    format_func=lambda x: apt_options[x],
                    index=None,
                    key="selected_apt"
                )
                
                st.dataframe(df_top[["rank", "시도", "시군구", "아파트명", "면적", "거래금액", "거래일자"]], use_container_width=True)

                chart_df = df_top.head(20).copy()
                chart_df["label"] = chart_df["rank"].astype(str) + ". " + chart_df["아파트명"].astype(str)
                st.bar_chart(chart_df.set_index("label")[["거래금액"]])
                
                # 선택된 단지의 년도별 통계 그래프 표시
                if selected_apt_idx is not None:
                    selected_apt = df_top.iloc[selected_apt_idx]
                    sgg_code = selected_apt["sggCd"]
                    apt_seq = selected_apt["aptSeq"]
                    apt_nm = selected_apt["아파트명"]
                    
                    st.markdown(f"### 📊 {apt_nm} 년도별 가격 및 거래량")
                    
                    yearly_stats = load_apartment_yearly_stats(sgg_code, apt_seq, apt_nm)
                    
                    if not yearly_stats.empty:
                        # 이중 Y축 차트 생성
                        fig = make_subplots(specs=[[{"secondary_y": True}]])
                        
                        # 평균가격 라인 차트 (왼쪽 Y축)
                        fig.add_trace(
                            go.Scatter(
                                x=yearly_stats["dealYear"],
                                y=yearly_stats["avgPrice"] * 10000,  # 억 단위로 변환 / 100000000
                                name="평균가격",
                                mode="lines+markers",
                                line=dict(color="blue", width=2),
                                marker=dict(size=8)
                            ),
                            secondary_y=False,
                        )
                        
                        # 거래량 막대 차트 (오른쪽 Y축)
                        fig.add_trace(
                            go.Bar(
                                x=yearly_stats["dealYear"],
                                y=yearly_stats["volume"],
                                name="거래량",
                                marker=dict(color="rgba(255, 165, 0, 0.6)"),
                                yaxis="y2"
                            ),
                            secondary_y=True,
                        )
                        
                        # 레이아웃 설정
                        fig.update_xaxes(title_text="연도")
                        fig.update_yaxes(title_text="평균가격 (억원)", secondary_y=False)
                        fig.update_yaxes(title_text="거래량 (건)", secondary_y=True)
                        fig.update_layout(
                            title=f"{apt_nm} 년도별 가격 및 거래량",
                            height=500,
                            hovermode="x unified",
                            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("해당 단지의 년도별 통계 데이터가 없습니다.")
                
                # 쿼리 표시
                if 'last_query' in st.session_state and 'load_top_apartments' in st.session_state.last_query:
                    with st.expander("실행된 SQL 쿼리 보기"):
                        st.code(st.session_state.last_query['load_top_apartments'], language='sql')
    else:
        st.info("좌측에서 시/군/구를 선택하세요.")

# =============================
# 메뉴 2: 거래량 조회
# =============================
if menu == "거래량 조회":
    st.subheader("📈 거래량 조회")

    # 광역시/도 목록 로드
    p1_df2 = load_p1_list() if not codes_df.empty else pd.DataFrame(columns=["code", "nm"])

    col1, col2, col3 = st.columns([1.2, 1, 1])
    today = date.today()

    with col1:
        if not p1_df2.empty:
            selected_p1_row2 = st.selectbox(
                "광역시/도 선택",
                [tuple(x) for x in p1_df2[["code", "nm"]].to_records(index=False)],
                format_func=lambda x: f"{x[1]} ({x[0]})",
                key="p1_2",
            )
            p1_code2, p1_nm2 = selected_p1_row2
            sgg_df2 = load_sgg_list(p1_code2)
        else:
            p1_code2, p1_nm2 = "", ""
            sgg_df2 = pd.DataFrame(columns=["code", "nm"])

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

# =============================
# 메뉴 3: 시세 추이
# =============================
if menu == "시세 추이":
    st.subheader("📊 시세 추이")

    # 광역시/도 목록 로드
    p1_df3 = load_p1_list() if not codes_df.empty else pd.DataFrame(columns=["code", "nm"])

    col1, col2, col3 = st.columns([1.2, 1, 1])
    today = date.today()

    with col1:
        if not p1_df3.empty:
            selected_p1_row3 = st.selectbox(
                "광역시/도 선택",
                [tuple(x) for x in p1_df3[["code", "nm"]].to_records(index=False)],
                format_func=lambda x: f"{x[1]} ({x[0]})",
                key="p1_3",
            )
            p1_code3, p1_nm3 = selected_p1_row3
            sgg_df3 = load_sgg_list(p1_code3)
        else:
            p1_code3, p1_nm3 = "", ""
            sgg_df3 = pd.DataFrame(columns=["code", "nm"])

    with col2:
        if not sgg_df3.empty:
            selected_row3 = st.selectbox(
                "시/군/구 선택",
                [tuple(x) for x in sgg_df3[["code", "nm"]].to_records(index=False)],
                format_func=lambda x: f"{x[1]} ({x[0]})",
                key="sgg3",
            )
            selected_sgg3, selected_sgg_nm3 = selected_row3
        else:
            selected_sgg3, selected_sgg_nm3 = "", ""

    with col3:
        start_dt3 = st.date_input("시작 월", value=date(today.year - 1, today.month, 1), key="start3")
        end_dt3 = st.date_input("종료 월", value=today, key="end3")

    if selected_sgg3:
        apt_df = load_apartments_in_sgg(selected_sgg3)
        if apt_df.empty:
            st.info("선택한 시/군/구에 아파트 목록이 없습니다.")
        else:
            apt_row = st.selectbox(
                "아파트 선택",
                [tuple(x) for x in apt_df[["aptSeq", "aptNm"]].to_records(index=False)],
                format_func=lambda x: f"{x[1]} ({x[0]})",
                key="apt3",
            )
            apt_seq3, apt_nm3 = apt_row

            if start_dt3 > end_dt3:
                st.warning("시작 월이 종료 월보다 이후입니다. 기간을 확인해주세요.")
            else:
                trend_df = load_price_trend(selected_sgg3, str(apt_seq3), start_dt3, end_dt3)
                if trend_df.empty:
                    st.info("해당 조건에 대한 시세 추이 데이터가 없습니다.")
                else:
                    st.markdown(f"**{selected_sgg_nm3} · {apt_nm3} 시세 추이**")
                    st.line_chart(trend_df.set_index("ym")[ ["dealAvgAmount"] ], use_container_width=True)
                    with st.expander("원본 데이터"):
                        st.dataframe(trend_df, use_container_width=True)
    else:
        st.info("좌측에서 시/군/구를 선택하고 기간을 지정하세요.")
