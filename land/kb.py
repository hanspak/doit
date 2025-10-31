import pandas as pd
import sqlite3

db_path = r'c:\doit\land\dbTHEH.db'      # ← SQLite DB 경로
table_names = ['tblKBMon','tblKBWeek']
excel_path = r'c:\doit\land\upload.xlsx'  # ← 엑셀 파일 경로
sheet_names = ['U매매월간APT지수','U전세월간APT지수','U매매주간APT지수','U전세주간APT지수','U매매주간APT증감','U전세주간APT증감']
#sheet_names = ['U전세주간APT지수']


# 5. SQLite DB에 저장
conn = sqlite3.connect(db_path)  # 새 DB 파일 생성 또는 연결

for sheet_name in sheet_names:
    #print("*sheet_name:", sheet_name)
    if sheet_name =='U매매월간APT지수' or sheet_name =='U전세월간APT지수' :
        #print("sheet_name:",sheet_name)
        # 1. 엑셀 파일에서 매매APT 시트 로드
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
        # 2. Long Format 변환
        df_long = df.melt(
        id_vars=['Year', 'Mon'],
        var_name='code',       # 지역 코드
        value_name='price'     # 매매지수
        )
        if sheet_name =='U매매월간APT지수' :
            df_long['kind'] = 11
        elif sheet_name =='U전세월간APT지수' :    
            df_long['kind'] = 12
        else : 
            break

    
        df_long = df_long[['code', 'Year', 'Mon', 'kind', 'price']]
        table_name = table_names[0]
        df_long['price'] = df_long['price'].fillna(0)
        # 4. 자료형 정리
        df_long['code'] = df_long['code'].astype(str)
        df_long['Year'] = df_long['Year'].astype(str)
        df_long['Mon'] = df_long['Mon'].astype(str)
        df_long['kind'] = df_long['kind'].astype(int)
        df_long['price'] = df_long['price'].astype(float)

    elif sheet_name =='U매매주간APT지수' or sheet_name =='U전세주간APT지수' or sheet_name =='U매매주간APT증감' or sheet_name =='U전세주간APT증감':
        print("sheet_name:",sheet_name)
        # 1. 엑셀 파일에서 매매APT 시트 로드
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
        # 2. Long Format 변환
        df_long = df.melt(
        id_vars=['Date'],
        var_name='code',       # 지역 코드
        value_name='price'     # 매매지수
        )

        if sheet_name =='U매매주간APT지수' :
            df_long['kind'] = 21
        elif sheet_name =='U전세주간APT지수' :    
            df_long['kind'] = 22
        elif sheet_name =='U매매주간APT증감' :    
            df_long['kind'] = 23
        elif sheet_name =='U전세주간APT증감' :    
            df_long['kind'] = 24    
        else : 
            break
        
        df_long = df_long[['code', 'Date', 'kind', 'price']]
        table_name = table_names[1]
        df_long['price'] = df_long['price'].fillna(0)
        # 4. 자료형 정리
        df_long['code'] = df_long['code'].astype(str)
        df_long['Date'] = df_long['Date'].astype(str)
        df_long['kind'] = df_long['kind'].astype(int) 
        df_long['price'] = df_long['price'].astype(float)

    df_long.to_sql(table_name, conn, if_exists='append', index=False)

# 3. 결측치 제거 (선택)
#df_long = df_lo
# df_long = df_long[['code', 'Year', 'Mon', 'price']]

# ng.dropna(subset=['price'])
conn.commit()
conn.close()

print("✅ 아파트 매매 데이터가 DB에 저장되었습니다.")