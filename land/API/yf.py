import pandas as pd
import yfinance as yf
import numpy as np
import matplotlib.pyplot as plt

# ETF 티커 설정 (Yahoo Finance 기준)
etf_dict = {
    'S&P500': '314250.KQ',  # KODEX 미국S&P500
    '배당다우존스': '360750.KQ',  # TIGER 미국배당다우존스
    '나스닥100': '379780.KQ',   # KODEX 미국나스닥100
    '코리아밸류업': '495850.KQ' # KODEX 코리아밸류업
}

weights = {
    'S&P500': 0.25,
    '배당다우존스': 0.25,
    '나스닥100': 0.20,
    '코리아밸류업': 0.30
}

start_date = '2024-01-01'
end_date = '2025-06-30'

# 데이터 다운로드
prices = pd.DataFrame()
for name, ticker in etf_dict.items():
    prices[name] = yf.download(ticker, start=start_date, end=end_date)['Adj Close']
    print(prices[name].head())

# 수익률 계산
returns = prices.pct_change().dropna()
portfolio_returns = (returns * pd.Series(weights)).sum(axis=1)

# 누적 수익률
cumulative_returns = (1 + portfolio_returns).cumprod()

# 성과 지표 계산
annual_return = portfolio_returns.mean() * 252
volatility = portfolio_returns.std() * np.sqrt(252)
sharpe_ratio = annual_return / volatility
mdd = (cumulative_returns / cumulative_returns.cummax() - 1).min()

# 결과 출력
results = pd.DataFrame({
    '항목': ['연환산 수익률', '연환산 변동성', '샤프지수', '최대 낙폭'],
    '값': [f"{annual_return:.2%}", f"{volatility:.2%}", f"{sharpe_ratio:.2f}", f"{mdd:.2%}"]
})

# Excel 저장
results.to_excel('B안_포트폴리오_백테스트결과.xlsx', index=False)

# 그래프
cumulative_returns.plot(title='B안 포트폴리오 누적 수익률')
plt.ylabel('Cumulative Return')
plt.grid()
plt.show()