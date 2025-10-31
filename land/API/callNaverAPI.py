import requests
import xml.etree.ElementTree as ET

complexNo = "7058"
#url = f"https://new.land.naver.com/api/complexes/{complexNo}?sameAddressGroup=false"

url = "https://new.land.naver.com/complexes/7058?ms=35.854291,128.629904,17&a=APT:PRE:ABYG:JGC&e=RETAIL"


headers = {
    'Referer': f'https://new.land.naver.com/complexes/{complexNo}',
    'User-Agent': 'Mozilla/5.0'
}

#url = "http://ecos.bok.or.kr/api/StatisticSearch/sample/xml/kr/1/10/200Y101/A/2020/2023/10101/?/?/?"
print("url:", url)
response = requests.get(url, headers=headers, verify=False)
print("data:",  response.text)
#root = ET.fromstring(response.content)
data = response.json()  # JSON 파싱

print("단지명:", data.get("complexName"))