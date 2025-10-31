import pandas as pd
import sqlite3
import os


excel_path = r'c:\doit\land\upload.xlsx'  # ← 엑셀 파일 경로
db_path = r'c:\doit\land\dbTHEH.db'      # ← SQLite DB 경로
table_name = 'tblCode'                     # 테이블 이름
sheet_name = 'code'                       # ← 원하는 시트명으로 변경

df = pd.read_excel(excel_path, sheet_name=sheet_name)

print(df.head)

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

insert_sql = f"insert into {table_name}({columns[0]},{columns[1]},{columns[2]},{columns[3]},{columns[4]},{columns[5]},{columns[6]},{columns[7]},{columns[8]}) values(?,?,?,?,?,?,?,?,?)"

print("insert_sql:",insert_sql )



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