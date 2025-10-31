import streamlit as st

col1, col2, col3 = st.columns([1.2, 1, 1])

with col1:
    st.header("왼쪽 영역 (넓음)")
    st.metric("매출액1", "₩1.2억", "+5.3%")

with col2:
    st.header("중앙 영역")
    st.metric("영업이익", "₩3,400만", "-1.2%")

with col3:
    st.header("오른쪽 영역")
    st.metric("순이익", "₩2,800만", "+0.8%")
