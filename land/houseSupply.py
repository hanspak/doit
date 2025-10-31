# -*- coding: utf-8 -*-
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import time
from sqlalchemy import create_engine

#---------------------------------------------------------------------------------------------------------
# 2. DB에 저장하는 함수
def save_to_db(df, engine, table_name='tbl_supply' ):
    if isinstance(df, pd.DataFrame):
        print("3.이 객체는 DataFrame입니다.")
    else:
        print("3.이 객체는 DataFrame이 아닙니다.")


    if not df.empty:
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"✅ DB 저장 완료: {len(df)}건")
    else:
        print("⚠️ 저장할 데이터 없음")

#---------------------------------------------------------------------------------------------------------
#def callRealAPI(page, per_page, service_key):
def get_housing_supply(year_month: str, page: int = 1, per_page: int = 1000, service_key: str = ""):

    # 요청 URL
    url = 'http://api.odcloud.kr/api/15111714/v1/uddi:0b257760-ac19-4841-adb4-b38b4d153397'

    global isError

    # 요청 파라미터
    params = {
        "page": page,
        "perPage": per_page,
        "serviceKey": service_key
    }

    try:
        print("**callRealAPI call=>1") 
        response = requests.get(url, params=params)
        response.raise_for_status()  # HTTP 에러 발생 시 예외
        print("**callRealAPI call=>2") 

        data = response.json()

        # 필터: 연월 기준 정리
        records = data.get("data", [])

        #filtered = [item for item in records if item.get("연월") == year_month]
        filtered = []

        for item in records:
            #if item.get("연월") == year_month:
            filtered.append(item)

        df = pd.DataFrame(filtered)
        
        return df


        #print(response.url)

        #print(response.text)

        # XML 파싱
        root = ET.fromstring(response.content)

        #resultCode = root.find(".//resultCode").text
        resultCode =""
      
        result_elem = root.find(".//resultCode")

        if result_elem is not None:
            resultCode = result_elem.text
            print("resultCode =", resultCode)
        else:
            print("resultCode not found")

        if resultCode !="000" :
           # returnReasonCode 요소 찾기
            returnAuthMsg = root.find(".//returnAuthMsg").text
            #print("returnAuthMsg =", returnAuthMsg)

            if returnAuthMsg != "" :
                print("Error : ", returnAuthMsg)
                isError =  True
                return 

        items = root.findall('.//item')

        # 데이터 추출
        i = 0
        data = []
        df = pd.DataFrame()
        for item in items:
        #print('%d'%i)
            i =i + 1
            data.append({
                '법정동시군구코드': item.findtext('sggCd'),  #===>key법정동시군구코드 
                '법정동읍면동코드': item.findtext('umdCd'),  #법정동읍면동코드
                '법정동지번코드': item.findtext('landCd'),   #법정동지번코드
                '법정동본번코드': item.findtext('bonbun'),   #법정동본번코드
                '법정동부번코드': item.findtext('bubun'),    #법정동부번코드
    
                '도로명': item.findtext('roadNm'),
                '도로명시군구코드': item.findtext('roadNmSggCd'),
                '도로명코드': item.findtext('roadNmCd'),
                '도로명일련번호코드': item.findtext('roadNmSeq'),
                '도로명지상지하코드': item.findtext('roadNmbCd'),
    
                '도로명건물본번호코드': item.findtext('roadNmBonbun'),
                '법정동': item.findtext('umdNm'),
                '단지명': item.findtext('aptNm'),
                '지번': item.findtext('jibun'),
                '전용면적': item.findtext('excluUseAr'),#===>key 
    
                '계약년도': item.findtext('dealYear'), #===>key
                '계약월': item.findtext('dealMonth'),  #===>key
                '계약일': item.findtext('dealDay'),    #===>key
                '거래금액': item.findtext('dealAmount'),
                '층': item.findtext('floor'),         #===>key
    
                '단지일련번호': item.findtext('aptSeq'), #===>key
                '해제여부': item.findtext('cdealType'), #===>key
                '해제사유발생일': item.findtext('cdealDay'),
                '거래유형': item.findtext('dealingGbn'),
                '중개사소재지': item.findtext('estateAgentSggNm'),
    
                '등기일자': item.findtext('rgstDate'),
                '아파트동명': item.findtext('aptDong'),
                '매도자': item.findtext('slerGbn'),
                '매수자': item.findtext('buyerGbn'),
                '토지임대부아파트여부': item.findtext('landLeaseholdGbn')
             })

        # 데이터프레임으로 변환
        df = pd.DataFrame(data)
 
        if isinstance(df, pd.DataFrame):
            print("1.이 객체는 DataFrame입니다.")
        else:
            print("1.이 객체는 DataFrame이 아닙니다.")
        
        return df
        #file_name = "c:/doit/land/output/output_"+str(LAWD_CD)+"_"+str(DEAL_YMD)+".xlsx"
        #df.to_excel(file_name, index=False)

    except requests.exceptions.RequestException as e:
        isError = True
        return {
            'param1': LAWD_CD,
            'param2': DEAL_YMD,
            'result_value': f"ERROR: {e}"
        }

#---------------------------------------------------------------------------------------------------------
# 1. 요청할 데이터가 담긴 엑셀 파일 로드
db_path = r'c:\doit\land\dbTHEH.db'      # ← SQLite DB 경로

isError = False

# SQLite DB 연결 (파일 경로로 저장)
engine = create_engine('sqlite:///' + db_path)

# 본인의 인증키
SERVICE_KEY = 'cFLQwzNPvbNw8ptBjHs1TAf0ctJ5TFSFV9oRO1egG06NuwFKgIhpdPY8yBFUgkzpmcDeiuR6JkLfN3YNieLBJw=='    #입주물량 service_key     
isError = False  
while isError==False:
    #df = callRealAPI(service_key, pageN,numOfRows,LAWD_CD,DEAL_YMD)
    df = get_housing_supply("2026-12", service_key=SERVICE_KEY)

    print(df.head())
    if isinstance(df, pd.DataFrame):
        print("2.이 객체는 DataFrame입니다.")
    else:
        print("2.이 객체는 DataFrame이 아닙니다.")
        print("isError:", isError)
    if isError == False :
        save_to_db(df, engine)    
        isError = True
    else:
        isError = False 

            #exit()               # 테스트 데이터
            #results.append(df)
        #time.sleep(0.5)  # rate limit 대응
        #if idx > 5 :
        #    break

    #final_df = pd.concat(results, ignore_index=True)        
    #save_to_db(final_df, engine)

#file_name = "c:/doit/land/output/output_"+str(DEAL_YMD)+".xlsx"

#final_df = pd.concat(results, ignore_index=True)

#final_df.to_excel(file_name, index=False)
#save_to_db(final_df, engine)

#print(df.shape)