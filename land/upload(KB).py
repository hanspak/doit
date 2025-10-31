import pandas as pd
import sqlite3
import os


excel_path = r'c:\doit\land\upload.xlsx'  # ← 엑셀 파일 경로
db_path = r'c:\doit\land\dbTHEH.db'      # ← SQLite DB 경로
table_name = 'tblKBMon'                     # 테이블 이름
sheet_name = '2.매매APT'                       # ← 원하는 시트명으로 변경

df = pd.read_excel(excel_path, sheet_name=sheet_name)

#print(df.head)

# 🛠 SQLite 연결
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# 📌 테이블 자동 생성 (열 타입은 TEXT로 통일)
columns = df.columns
#col_defs = ', '.join([f'"{col}" TEXT' for col in columns])
#create_table_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({col_defs})'
#cursor.execute(create_table_sql)

#print(columns[0])

# insert_sql 정의

insert_sql = f"insert into {table_name}(code, year, mon, gubun, value) values(?,?,?,?,?)"

#print("insert_sql:",insert_sql )
#s_year = columns['year']
#s_mon = columns['mon']

# 2. Long Format으로 변환 (melt 사용)
# 2. Long Format 변환
df_long = df.melt(
    id_vars=['Year', 'Mon'],
    var_name='code',       # 지역 코드
    value_name='price'     # 매매지수
)


# 3. 결측치 제거 (선택)
df_long = df_long.dropna(subset=['price'])

# 4. 자료형 정리
df_long['code'] = df_long['code'].astype(str)
df_long['Year'] = df_long['Year'].astype(int)
df_long['Mon'] = df_long['Mon'].astype(int)
df_long['price'] = df_long['price'].astype(float)

df_long.to_sql('apt_price_index', conn, if_exists='replace', index=False)
conn.close()






for col in columns:
    print(col)


for index, row in df.iterrows():
   values = []
   val = row["code"]
   if pd.notna(val):
        val = str(int(val))  # 50110.0 → '50110'
        values.append(val)  
   else:
       break        
   
   val = row["p1_code"]
   if pd.notna(val):
        val = str(int(val))  # 50110.0 → '50110'
        values.append(val)  
   else:
        values.append(None)

   val = row["p2_code"]
   if pd.notna(val):
        val = str(int(val))  # 50110.0 → '50110'
        values.append(val)  
   else:
        values.append(None)

   val = row["kind1"]
   if pd.notna(val):
        val = val  # 50110.0 → '50110'
        values.append(val)  
   else:
        values.append(None)

   val = row["kind2"]
   if pd.notna(val):
        val = val  # 50110.0 → '50110'
        values.append(val)  
   else:
        values.append(None)    

   val = row["ename"]
   if pd.notna(val):
        val = val  # 50110.0 → '50110'
        values.append(val)  
   else:
        values.append(None)    

   val = row["name"]
   if pd.notna(val):
        val = val  # 50110.0 → '50110'
        values.append(val)  
   else:
        values.append(None)    

   val = row["nm"]
   if pd.notna(val):
        val = val  # 50110.0 → '50110'
        values.append(val)  
   else:
        values.append(None)    

   val = row["etc"]
   if pd.notna(val):
        val = val  # 50110.0 → '50110'
        values.append(val)  
   else:
        values.append(None)    

   print(index)
   print(values)
   cursor.execute(insert_sql, values)
#   conn.commit()


    # ✅ 저장 및 종료
conn.commit()
conn.close()


print(f"엑셀 데이터를 행 단위로 SQLite DB '{table_name}' 테이블에 저장 완료!")
print(f"DB 위치: {os.path.abspath(db_path)}")