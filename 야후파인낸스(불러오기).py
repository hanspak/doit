import yfinance as yf
import matplotlib.pyplot as plt

# 삼성전자 (005930.KS)의 주가 데이터 가져오기
sec1 = yf.Ticker("005930.KS").history(start="2024-01-01", end="2024-05-01")

# 종가 시계열 그래프 그리기
plt.figure(figsize=(12,6))
plt.plot(sec1.index, sec1['Close'], 'b', label='Samsung Electronics')
plt.title('Samsung Electronics Closing Price (2024)')
plt.xlabel('Date')
plt.ylabel('Price (KRW)')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()