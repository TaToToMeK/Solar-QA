import requests
import json
import brotli

def is_zip(content):
    # XLSX (zip) zaczyna się od PK\x03\x04
    return content[:2] == b"PK"

def parse_headers_file(filename):
    headers = {}
    with open(filename, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith(("GET", "POST", "PUT", "DELETE", "PATCH", "HEAD")) or line.startswith("HTTP"):
                continue
            if ": " in line:
                key, value = line.split(": ", 1)
                headers[key.strip()] = value.strip()
    # Usuń nagłówki zbędne dla requests
    for skip in ["Content-Length", "Origin", "Sec-Fetch-", "DNT", "Pragma", "Cache-Control", "Host", "Connection"]:
        headers = {k: v for k, v in headers.items() if not k.startswith(skip)}
    return headers

headers = parse_headers_file("../tmp/headers.txt")

with open("../tmp/payload.json", "r", encoding="utf-8") as f:
    payload = json.load(f)

payload["deviceId"] = "230731624"
payload["deviceSn"] = "SS1ES120P4U121"
payload["parentSn"] = "2768723817"
payload["startDay"] = "2025-06-04"
payload["endDay"]   = "2025-06-04"
xls_filename = "/media/ramdisk/exported_data_"+payload["deviceId"]+"-"+payload["deviceSn"]+".xlsx"

url = "https://globalhome.solarmanpv.com/device-s/report/export"

response = requests.post(url, headers=headers, json=payload)
encoding = response.headers.get("Content-Encoding")
print("Content-Encoding:", encoding)
print("Content-Type:", response.headers.get("Content-Type"))
print("Pierwsze bajty:", response.content[:4])

if response.status_code == 200:
    content = response.content
    if encoding == "br" and not is_zip(content):
        print("Dekompresja Brotli...")
        content = brotli.decompress(content)
    else:
        print("Plik już rozpakowany lub nie wymaga dekompresji.")
    with open(xls_filename, "wb") as f:
        f.write(content)
    print(f"Plik został zapisany jako {xls_filename}")
else:
    print("Błąd:", response.status_code, response.text[:500])


