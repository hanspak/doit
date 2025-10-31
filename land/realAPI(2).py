# -*- coding: utf-8 -*-
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import time
from sqlalchemy import create_engine

#---------------------------------------------------------------------------------------------------------
# 2. DB에 저장하는 함수
def save_to_db(df, engine, table_name='tbl_real_estate' ):
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
def callRealAPI(service_key, pageN,numOfRows,LAWD_CD,DEAL_YMD):
    # 요청 URL
    # #url = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade'
    url = 'http://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev'

    global isError

    #url = 'http://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev?serviceKey=UIRiUedSrUkb68ntUOKaIkSoppX9Pc1aY3jLtDG4OYFF6PJU7P3pPDybDDYgzaPDGG%2B0O4lkmzMhz7%2Fo5J0oWw%3D%3D&LAWD_CD=11110&DEAL_YMD=202405&pageNo=1&numOfRows=1'
    #       http://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev?serviceKey=UIRiUedSrUkb68ntUOKaIkSoppX9Pc1aY3jLtDG4OYFF6PJU7P3pPDybDDYgzaPDGG%252B0O4lkmzMhz7%252Fo5J0oWw%253D%253D&pageN=1&numOfRows=10&LAWD_CD=11110&DEAL_YMD=202404

    # 요청 파라미터
    params = {
        'serviceKey': service_key,
        'pageN':pageN,
        'numOfRows':numOfRows, 
        'LAWD_CD': LAWD_CD,         # 서울 종로구 (법정동 코드:11110)
        'DEAL_YMD': DEAL_YMD,       # 2024년 4월
    }
    try:
        print("**callRealAPI call=>1") 
        response = requests.get(url, params=params)
        response.raise_for_status()  # HTTP 에러 발생 시 예외
        print("**callRealAPI call=>2") 

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
input_file = r'c:\doit\land\code.xlsx'  # ← 엑셀 파일 경로
sheet_name = "list"
df_input = pd.read_excel(input_file,sheet_name)

isError = False


# SQLite DB 연결 (파일 경로로 저장)
engine = create_engine('sqlite:///' + db_path)

# 본인의 인증키
service_key = 'UIRiUedSrUkb68ntUOKaIkSoppX9Pc1aY3jLtDG4OYFF6PJU7P3pPDybDDYgzaPDGG+0O4lkmzMhz7/o5J0oWw=='                   

# 2. 각 행마다 API 호출
for idx, row in df_input.iterrows():
    # 요청 파라미터 구성 (예시)
    LAWD_CD = row['code']
    #param2 = row['p1_code'] 
    param2 = row['p1_code'] if pd.notna(row['p1_code']) else 0
    param3 = row['p2_code'] if pd.notna(row['p2_code']) else 0
    param4 = row['kind1'] if pd.notna(row['kind1']) else 0
    param5 = row['kind2'] if pd.notna(row['kind2']) else 0
    ename  = row['ename'] if pd.notna(row['ename']) else 0

    #print("LAWD_CD:",LAWD_CD)
    #print("param2:",param2)
    #print("param3:",param3)
    #print("param4:",param4)
    #print("param5:",param5)
    #print("ename:",ename)

    pageN = '1'
    numOfRows = '9999'
    year = '2019'       # 2024, 2023, 2022, 2021, 2020, 2019            
                        # 2016(LAWD_CD:DEAL_YMD 26350 201612), 2019 X . 확인 필요함.  2017,2018, 2020, 2021,2022,2023,2024 완료, 2025년 1~3월(완료)
    if   param2== 0 :
       continue
       
    
    #if idx > 2 :
    #    break 
    
    # 결과를 누적할 리스트
    results = []

    for y in range(2025,2026):
        for i in range(1, 5):  # 0부터 12까지 포함   
            #DEAL_YMD = year+ str(i).zfill(2)
            DEAL_YMD = str(y)+ str(i).zfill(2)

            #LAWD_CD= "11350"     # 테스트 데이터
            #DEAL_YMD = "201909"  # 테스트 데이터
            print("LAWD_CD:DEAL_YMD",LAWD_CD,DEAL_YMD)

            isError = False  
            cntCall = 0
            while isError==False:
                print("cntCall=", cntCall)
                df = callRealAPI(service_key, pageN,numOfRows,LAWD_CD,DEAL_YMD)

                if isinstance(df, pd.DataFrame):
                    print("2.이 객체는 DataFrame입니다.")
                else:
                    print("2.이 객체는 DataFrame이 아닙니다.")
    
                print("isError:", isError)
                if isError == False :
                    #save_to_db(df, engine)    
                    isError = True
                else:
                    isError = False 
                    cntCall= cntCall+ 1
                #exit()

            #exit()               # 테스트 데이터
            #results.append(df)
            time.sleep(0.5)  # rate limit 대응
        #if idx > 5 :
        #    break

    #final_df = pd.concat(results, ignore_index=True)        
    #save_to_db(final_df, engine)

#file_name = "c:/doit/land/output/output_"+str(DEAL_YMD)+".xlsx"

#final_df = pd.concat(results, ignore_index=True)

#final_df.to_excel(file_name, index=False)
#save_to_db(final_df, engine)

#print(df.shape)