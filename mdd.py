import yfinance as yf
import matplotlib.pyplot as plt

# KOSPI 종목 코드 (Yahoo Finance 기준: ^KS11)
kospi = yf.Ticker("^KS11").history(start="2024-01-01", end="2024-05-01")

# 20일 이동 최고가 계산
window = 20
peak = kospi['Adj Close'].rolling(window=window, min_periods=1).max()

# 시각화
plt.figure(figsize=(12, 6))
plt.plot(kospi.index, kospi['Adj Close'], label='KOSPI Adj Close')
plt.plot(kospi.index, peak, label=f'{window}-Day Rolling Max', linestyle='--', color='red')
plt.title(f'KOSPI and {window}-Day Peak')
plt.xlabel("Date")
plt.ylabel("Index Level")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()