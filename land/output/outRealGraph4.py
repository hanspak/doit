# -*- coding: utf-8 -*-
"""Command line helper for plotting apartment transaction trends."""

import os
from __future__ import annotations

import argparse
from datetime import datetime
from itertools import cycle
from pathlib import Path
from typing import Sequence

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib import font_manager, ticker
from matplotlib.ticker import StrMethodFormatter
from sqlalchemy import create_engine, text

DEFAULT_DB_PATH = Path("C:\db\dbTHEH.db")
DEFAULT_DANGI_LIST = [
    "11710-6346",
    "41290-181",
    "11260-3783",
    "11440-5410",
]
DEFAULT_USE_AREA = 25.7
DATE_FMT = "%Y-%m-%d"

SQL_MONTHLY = text(
    """
    -- Monthly aggregate price/volume per apartment
    SELECT
        MAX(a.aptNm)               AS aptNm,
        SUBSTR(a.dealDate, 1, 7)   AS ym,
        ROUND(AVG(a.dealAmount), 0) AS price,
        COUNT(*)                   AS volume
    FROM tblReal a
    WHERE a.dealDate BETWEEN :s AND :e
      AND a.aptSeq = :seq
      AND a.useArea = :area
    GROUP BY SUBSTR(a.dealDate, 1, 7)
    ORDER BY ym
"""
)

SQL_DAILY = text(
    """
    SELECT
        a.aptNm,
        SUBSTR(a.dealDate, 1, 7) AS ym,
        a.dealDate               AS dt,
        a.dealAmount             AS price
    FROM tblReal a
    WHERE a.dealDate BETWEEN :s AND :e
      AND a.aptSeq = :seq
      AND a.useArea = :area
    ORDER BY a.dealDate
"""
)


def parse_args() -> argparse.Namespace:
    """Build command-line interface so the script runs without editing code."""
    parser = argparse.ArgumentParser(
        description="Plot monthly averages and daily deals for the selected apartment sequences."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="Path to the SQLite database (default: %(default)s).",
    )
    parser.add_argument(
        "--start-date",
        default="2018-01-01",
        help=f"First deal date to include (format: {DATE_FMT}).",
    )
    parser.add_argument(
        "--end-date",
        default="2025-12-31",
        help=f"Last deal date to include (format: {DATE_FMT}).",
    )
    parser.add_argument(
        "--use-area",
        type=float,
        default=DEFAULT_USE_AREA,
        help="Exclusive area to filter on in square meters.",
    )
    parser.add_argument(
        "--seq",
        dest="dangi_list",
        action="append",
        help="Apartment sequence code to include. Repeat the flag to add more codes.",
    )
    parser.add_argument(
        "--annotate-extrema",
        dest="annotate_extrema",
        action="store_true",
        help="Annotate the highest and lowest deal for each apartment (default).",
    )
    parser.add_argument(
        "--no-annotate",
        dest="annotate_extrema",
        action="store_false",
        help="Disable annotations for min/max deals.",
    )
    parser.set_defaults(annotate_extrema=True)
    parser.add_argument(
        "--save-path",
        type=Path,
        help="Optional path to save the resulting figure (PNG/other matplotlib-supported formats).",
    )
    parser.add_argument(
        "--no-show",
        action="store_true",
        help="Do not open an interactive window (useful for batch runs).",
    )
    parser.add_argument(
        "--title",
        help="Custom plot title. Defaults to an auto-generated range summary.",
    )

    args = parser.parse_args()
    args.dangi_list = args.dangi_list or list(DEFAULT_DANGI_LIST)

    try:
        start_dt = datetime.strptime(args.start_date, DATE_FMT)
        end_dt = datetime.strptime(args.end_date, DATE_FMT)
    except ValueError:
        parser.error(f"Dates must follow {DATE_FMT}. Received start={args.start_date!r}, end={args.end_date!r}.")

    if start_dt > end_dt:
        parser.error("--start-date must not be later than --end-date.")

    setattr(args, "start_dt", start_dt)
    setattr(args, "end_dt", end_dt)

    return args


def configure_plot_style(font_candidates: Sequence[str] | None = None) -> None:
    """Set fonts/grid so plots are legible even when running headless."""
    plt.rcParams["axes.unicode_minus"] = False
    font_candidates = font_candidates or ("Malgun Gothic", "AppleGothic", "NanumGothic", "DejaVu Sans")
    for font in font_candidates:
        try:
            font_manager.findfont(font, fallback_to_default=False)
        except ValueError:
            continue
        plt.rcParams["font.family"] = font
        break

    plt.rcParams.update(
        {
            "figure.figsize": (11.5, 5.2),
            "lines.linewidth": 1.6,
            "axes.grid": True,
            "grid.alpha": 0.3,
            "legend.frameon": False,
        }
    )


def load_monthly(engine, seq: str, start_date: str, end_date: str, use_area: float) -> pd.DataFrame:
    with engine.connect() as conn:
        df = pd.read_sql(
            SQL_MONTHLY,
            conn,
            params={"s": start_date, "e": end_date, "seq": seq, "area": use_area},
        )
    if df.empty:
        return df
    df = df.rename(columns={"ym": "date"})
    df["c_date"] = pd.to_datetime(df["date"], format="%Y-%m")
    return df[["aptNm", "date", "c_date", "price", "volume"]]


def load_daily(engine, seq: str, start_date: str, end_date: str, use_area: float) -> pd.DataFrame:
    with engine.connect() as conn:
        df = pd.read_sql(
            SQL_DAILY,
            conn,
            params={"s": start_date, "e": end_date, "seq": seq, "area": use_area},
        )
    if df.empty:
        return df
    df["c_date"] = pd.to_datetime(df["dt"])
    return df[["aptNm", "ym", "dt", "c_date", "price"]]


def color_cycle():
    palettes: list[Sequence[float]] = []
    for cmap_name in ("tab10", "tab20"):
        try:
            cmap = plt.get_cmap(cmap_name)
        except ValueError:
            continue
        palettes.extend(cmap.colors)
    if not palettes:
        palettes = [f"C{i}" for i in range(10)]
    return cycle(palettes)


def build_title(start_dt: datetime, end_dt: datetime, use_area: float) -> str:
    year_span = end_dt.year - start_dt.year + 1
    return f"{year_span} year transaction trend (use area {use_area} m^2)"


def plot_real_estate(
    monthly_list: Sequence[pd.DataFrame],
    daily_list: Sequence[pd.DataFrame],
    annotate_extrema: bool,
    title: str,
    save_path: Path | None,
    show_plot: bool,
) -> None:
    fig, ax_price = plt.subplots(figsize=(11.5, 5.2))
    ax_volume = ax_price.twinx()

    ax_price.set_ylabel("Price (KRW)")
    ax_price.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))

    ax_volume.set_ylabel("Transaction volume")
    ax_volume.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))

    ax_price.xaxis.set_major_locator(mdates.YearLocator())
    ax_price.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    plt.setp(ax_price.get_xticklabels(), rotation=45, ha="right")

    bar_width_days = 18
    gap_days = 10
    valid_monthly = [df for df in monthly_list if not df.empty]
    monthly_offsets = [
        (idx - (len(valid_monthly) - 1) / 2) * gap_days if valid_monthly else 0
        for idx in range(len(valid_monthly))
    ]

    color_iter = color_cycle()
    bar_color_iter = color_cycle()

    handles_price, labels_price = [], []
    handles_volume, labels_volume = [], []
    max_volume = 0.0
    monthly_index = 0

    for monthly, daily in zip(monthly_list, daily_list):
        if monthly.empty and daily.empty:
            continue

        apt_name = ""
        if not monthly.empty:
            apt_name = monthly["aptNm"].iloc[0]
        elif not daily.empty:
            apt_name = daily["aptNm"].iloc[0]

        line_color = next(color_iter)
        bar_color = next(bar_color_iter)

        if not monthly.empty:
            (line_handle,) = ax_price.plot(
                monthly["c_date"],
                monthly["price"],
                color=line_color,
                label=f"{apt_name} monthly avg",
            )
            handles_price.append(line_handle)
            labels_price.append(f"{apt_name} monthly avg")

        if not daily.empty:
            scatter_handle = ax_price.scatter(
                daily["c_date"],
                daily["price"],
                s=30,
                color=line_color,
                marker="o",
                label=f"{apt_name} deals",
                zorder=4,
            )
            handles_price.append(scatter_handle)
            labels_price.append(f"{apt_name} deals")

        if not monthly.empty:
            offset_days = monthly_offsets[monthly_index] if monthly_offsets else 0
            monthly_index += 1
            shift = pd.to_timedelta(offset_days, unit="D")
            bar_container = ax_volume.bar(
                monthly["c_date"] + shift,
                monthly["volume"],
                width=bar_width_days,
                alpha=0.45,
                color=bar_color,
                label=f"{apt_name} volume",
            )
            handles_volume.append(bar_container)
            labels_volume.append(f"{apt_name} volume")
            max_volume = max(max_volume, float(monthly["volume"].max()))

        if annotate_extrema and not daily.empty:
            pmax = daily.loc[daily["price"].idxmax(), ["c_date", "price"]]
            pmin = daily.loc[daily["price"].idxmin(), ["c_date", "price"]]
            ax_price.scatter(pmax["c_date"], pmax["price"], s=80, color="red", zorder=5)
            ax_price.annotate(
                f"{apt_name} max {int(pmax['price']):,}",
                xy=(pmax["c_date"], pmax["price"]),
                xytext=(0, 18),
                textcoords="offset points",
                ha="center",
                fontsize=9,
            )
            ax_price.scatter(
                pmin["c_date"],
                pmin["price"],
                s=60,
                color="gold",
                edgecolor="black",
                zorder=5,
            )
            ax_price.annotate(
                f"{apt_name} min {int(pmin['price']):,}",
                xy=(pmin["c_date"], pmin["price"]),
                xytext=(0, -22),
                textcoords="offset points",
                ha="center",
                fontsize=9,
            )

    if max_volume:
        ax_volume.set_ylim(0, np.ceil(max_volume * 1.6))

    ax_price.set_title(title)
    if handles_price:
        leg_price = ax_price.legend(handles=handles_price, labels=labels_price, loc="upper left")
        leg_price.set_zorder(10)
    if handles_volume:
        leg_volume = ax_volume.legend(handles=handles_volume, labels=labels_volume, loc="upper right")
        leg_volume.set_zorder(10)

    plt.tight_layout()

    if save_path:
        save_path = Path(save_path)
        save_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=200, bbox_inches="tight")
        print(f"Saved figure to {save_path}")

    if show_plot:
        plt.show()
    else:
        plt.close(fig)


def main() -> None:
    """Entry point used by both CLI and imports."""
    args = parse_args()
    configure_plot_style()
    engine = create_engine(f"sqlite:///{args.db_path}")

    monthly_list = [
        load_monthly(engine, seq, args.start_date, args.end_date, args.use_area) for seq in args.dangi_list
    ]
    daily_list = [
        load_daily(engine, seq, args.start_date, args.end_date, args.use_area) for seq in args.dangi_list
    ]
    engine.dispose()

    if not any(not df.empty for df in monthly_list + daily_list):
        raise SystemExit("No records found for the provided filters.")

    title = args.title or build_title(args.start_dt, args.end_dt, args.use_area)
    plot_real_estate(
        monthly_list=monthly_list,
        daily_list=daily_list,
        annotate_extrema=args.annotate_extrema,
        title=title,
        save_path=args.save_path,
        show_plot=not args.no_show,
    )


if __name__ == "__main__":
    main()
