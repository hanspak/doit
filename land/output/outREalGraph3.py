# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import ticker
from matplotlib.ticker import StrMethodFormatter
import matplotlib.dates as mdates
from pathlib import Path
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta    

# =========================
# 1) 사용자 설정
# =========================
DB_PATH = Path("c:/doit/land/dbTHEH.db")

s_date = '2018-01-01'
e_date = '2025-12-31'
c_year = datetime.strptime(e_date, '%Y-%m-%d').year - datetime.strptime(s_date, '%Y-%m-%d').year + 1
title = f"{c_year}년 실거래가 그래프"

dangi1 = '11710-6346'
dangi2 = '41290-181'  # 래미안 슈르
use_area = 25.7        # 실사용 면적(전용) 필터

# =========================
# 2) 폰트/그래프 전역 설정
# =========================
# 한글 폰트 (OS별 fallback)
plt.rcParams['axes.unicode_minus'] = False
preferred_fonts = ["Malgun Gothic", "AppleGothic", "NanumGothic", "DejaVu Sans"]
for f in preferred_fonts:
    try:
        plt.rcParams['font.family'] = f
        break
    except Exception:
        continue

# 선/눈금/범례 기본값
plt.rcParams.update({
    "figure.figsize": (10, 4.8),
    "lines.linewidth": 1.5,
    "axes.grid": True,
    "grid.alpha": 0.3,
    "legend.frameon": False,
})

# =========================
# 3) DB & 쿼리 유틸
# =========================
engine = create_engine(f"sqlite:///{DB_PATH}")

SQL_MONTHLY = text("""
    SELECT
        MAX(a.aptNm)            AS aptNm,
        SUBSTR(a.dealDate,1,7)  AS ym,
        ROUND(AVG(a.dealAmount), 0) AS price,
        COUNT(*)                 AS volume
    FROM tblReal a
    WHERE a.dealDate BETWEEN :s AND :e
      AND a.aptSeq = :seq
      AND a.useArea IN (:area)
    GROUP BY SUBSTR(a.dealDate,1,7)
    ORDER BY ym
""")

SQL_DAILY = text("""
    SELECT
        a.aptNm,
        SUBSTR(a.dealDate,1,7) AS ym,
        a.dealDate             AS dt,
        a.dealAmount           AS price
    FROM tblReal a
    WHERE a.dealDate BETWEEN :s AND :e
      AND a.aptSeq = :seq
      AND a.useArea IN (:area)
    ORDER BY a.dealDate
""")

def load_monthly(engine, seq, s, e, area):
    with engine.connect() as conn:
        df = pd.read_sql(SQL_MONTHLY, conn, params={"s": s, "e": e, "seq": seq, "area": area})
    if df.empty:
        return df
    df = df.rename(columns={"ym": "date"})
    df["c_date"] = pd.to_datetime(df["date"], format="%Y-%m")
    return df[["aptNm", "date", "c_date", "price", "volume"]]

def load_daily(engine, seq, s, e, area):
    with engine.connect() as conn:
        df = pd.read_sql(SQL_DAILY, conn, params={"s": s, "e": e, "seq": seq, "area": area})
    if df.empty:
        return df
    df["c_date"] = pd.to_datetime(df["dt"])
    return df[["aptNm", "ym", "dt", "c_date", "price"]]

# =========================
# 4) 데이터 로드
# =========================
df11 = load_monthly(engine, dangi1, s_date, e_date, use_area)   # 단지1 월평균+거래량
df12 = load_daily  (engine, dangi1, s_date, e_date, use_area)   # 단지1 일별 실거래
df21 = load_monthly(engine, dangi2, s_date, e_date, use_area)   # 단지2 월평균+거래량
df22 = load_daily  (engine, dangi2, s_date, e_date, use_area)   # 단지2 일별 실거래

# 방어적 처리: 빈 데이터 안내
if df11.empty and df21.empty:
    raise ValueError("조회 구간/조건에 해당하는 데이터가 없습니다. (두 단지 모두) 조건을 확인하세요.")

# =========================
# 5) 보조 유틸 (최대/최소 포인트)
# =========================
def get_extrema_points(daily_df):
    """일별 실거래 df에서 최대/최소 위치/값 반환 (없으면 None)"""
    if daily_df is None or daily_df.empty:
        return None, None
    max_idx = daily_df["price"].idxmax()
    min_idx = daily_df["price"].idxmin()
    pmax = daily_df.loc[max_idx, ["c_date", "price"]]
    pmin = daily_df.loc[min_idx, ["c_date", "price"]]
    return pmax, pmin

# =========================
# 6) 시각화
# =========================
def plot_compare(df11, df12, df21, df22, title):
    fig, ax1 = plt.subplots(figsize=(10, 4.8))

    # 색상 팔레트 (명확한 구분)
    color_line_1 = "royalblue"
    color_line_2 = "orange"
    color_scatter_1 = "tab:brown"
    color_scatter_2 = "tab:green"

    # --- 평균선 (단지1/단지2)
    if not df11.empty:
        name1 = str(df11["aptNm"].iloc[0])
        ax1.plot(df11["c_date"], df11["price"], color=color_line_1, linewidth=1.4,
                 label=f"{name1} 월평균(만)")
    if not df21.empty:
        name2 = str(df21["aptNm"].iloc[0])
        ax1.plot(df21["c_date"], df21["price"], color=color_line_2, linewidth=1.4,
                 label=f"{name2} 월평균(만)")

    ax1.set_ylabel("가격(만)")

    # --- 실거래 점 (단지1/단지2)
    if df12 is not None and not df12.empty:
        ax1.plot(df12["c_date"], df12["price"], linestyle="None", marker=".", markersize=6,
                 color=color_scatter_1, label="실거래(단지1)")
    if df22 is not None and not df22.empty:
        ax1.plot(df22["c_date"], df22["price"], linestyle="None", marker=".", markersize=6,
                 color=color_scatter_2, label="실거래(단지2)")

    # --- 거래량 막대 (보조축)
    ax2 = ax1.twinx()
    bar_width_days = 18  # 월 데이터 기준: 약 3주 폭
    half_shift = 8       # 두 막대를 좌우로 약간 벌리기

    if not df11.empty:
        ax2.bar(df11["c_date"] - pd.Timedelta(days=half_shift),
                df11["volume"], width=bar_width_days, alpha=0.45, color=color_line_1,
                label=f"{name1} 거래량")
    if not df21.empty:
        ax2.bar(df21["c_date"] + pd.Timedelta(days=half_shift),
                df21["volume"], width=bar_width_days, alpha=0.45, color=color_line_2,
                label=f"{name2} 거래량")

    ax2.set_ylabel("거래량(건)")
    if not (df11.empty and df21.empty):
        vmax = 0
        if not df11.empty:
            vmax = max(vmax, df11["volume"].max())
        if not df21.empty:
            vmax = max(vmax, df21["volume"].max())
        ax2.set_ylim(0, np.ceil(vmax * 1.6))

    # --- 최대/최소 강조 (각 단지 실거래 기준)
    def annotate_extrema(daily_df, color_point, lab_prefix):
        pmax, pmin = get_extrema_points(daily_df)
        if pmax is not None:
            ax1.scatter(pmax["c_date"], pmax["price"], s=80, color="red", zorder=5)
            ax1.annotate(f"{lab_prefix} 최고 {int(pmax['price']):,}",
                         xy=(pmax["c_date"], pmax["price"]),
                         xytext=(0, 18), textcoords="offset points",
                         ha="center", fontsize=9)
        if pmin is not None:
            ax1.scatter(pmin["c_date"], pmin["price"], s=60, color="gold", edgecolor="k", zorder=5)
            ax1.annotate(f"{lab_prefix} 최저 {int(pmin['price']):,}",
                         xy=(pmin["c_date"], pmin["price"]),
                         xytext=(0, -22), textcoords="offset points",
                         ha="center", fontsize=9)

    if df12 is not None and not df12.empty:
        annotate_extrema(df12, color_scatter_1, "단지1")
    if df22 is not None and not df22.empty:
        annotate_extrema(df22, color_scatter_2, "단지2")

    # --- 축/눈금/포맷
    ax1.set_title(title)
    ax1.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))       # 가격 천단위
    ax2.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))        # 거래량 정수 눈금
    ax2.yaxis.set_major_locator(ticker.MultipleLocator(10))              # 10단위 눈금

    # X축: 연도 눈금(필요 시 월 보조 눈금 활성화)
    ax1.xaxis.set_major_locator(mdates.YearLocator())
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    # ax1.xaxis.set_minor_locator(mdates.MonthLocator())

    plt.setp(ax1.get_xticklabels(), rotation=45, ha="right")
    fig.autofmt_xdate()

    # --- 범례: 축별로 구분해서 깔끔하게
    # 1) 기본(가격/실거래) 범례 - 좌상
    handles1, labels1 = ax1.get_legend_handles_labels()
    if handles1:
        leg1 = ax1.legend(handles=handles1, loc="upper left", ncols=1)
        leg1.set_zorder(10)

    # 2) 거래량 범례 - 우상
    handles2, labels2 = ax2.get_legend_handles_labels()
    if handles2:
        leg2 = ax2.legend(handles=handles2, loc="upper right", ncols=1)
        leg2.set_zorder(10)

    plt.tight_layout()
    plt.show()

# =========================
# 7) 실행
# =========================
plot_compare(df11, df12, df21, df22, title)
