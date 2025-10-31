import sys
import requests
import uuid
import urllib3

#urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

if len(sys.argv) < 2:
    print("Usage: python", sys.argv[0], "{url}")
    sys.exit()

url = sys.argv[1]
print("url:", url)

resp = requests.get(url, verify=False)  # SSL 인증서 검증 생략

fileName = str(uuid.uuid4()) + ".html"
with open(fileName, mode="wt", encoding="utf-8") as f:
    f.write(resp.text)

print(url, "saved to", fileName)