import pandas as pd
import plotly.express as px
from ipywidgets import interact

# 가상 예시 데이터
data = {
    "aptSeq": [101, 101, 101, 102, 102, 103, 103],
    "aptNm": ["힐스테이트A", "힐스테이트A", "힐스테이트A",
              "래미안B", "래미안B", "자이C", "자이C"],
    "dealDate": ["2024-01-10", "2024-02-10", "2024-03-10",
                 "2024-01-12", "2024-02-12", "2024-01-15", "2024-02-15"],
    "price": [8.5, 8.7, 8.9, 7.2, 7.5, 6.8, 7.0]
}
df = pd.DataFrame(data)
df['dealDate'] = pd.to_datetime(df['dealDate'])

# 인터랙티브 함수 정의
def plot_apt(apt_seq):
    sub = df[df['aptSeq'] == apt_seq]
    if sub.empty:
        print("데이터 없음")
        return
    fig = px.line(sub, x='dealDate', y='price',
                  title=f"{sub['aptNm'].iloc[0]} 실거래가 추이",
                  markers=True,
                  labels={'dealDate': '거래일자', 'price': '가격(억)'})
    fig.update_traces(line=dict(width=3, color='royalblue'))
    fig.update_layout(template='plotly_white')
    fig.show()

# 단지 선택용 위젯
interact(plot_apt, apt_seq=sorted(df['aptSeq'].unique()))
