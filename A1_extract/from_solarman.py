from config import config
import calendar
import requests
import json
import sys
import brotli
from config.db import connect_db
import logging
logger = logging.getLogger(__name__)



def is_zip(content):
    # XLSX (zip) zaczyna się od PK\x03\x04
    #         Pierwsze bajty: b'PK\x03\x04'
    return content[:2] == b"PK"
    #return content[:4] == b'PK\x03\x04'

def parse_headers_file(filename):
    headers = {}
    with open(filename, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith(("GET", "POST", "PUT", "DELETE", "PATCH", "HEAD")) or line.startswith(
                    "HTTP"):
                continue
            if ": " in line:
                key, value = line.split(": ", 1)
                headers[key.strip()] = value.strip()
    # Usuń nagłówki zbędne dla requests
    for skip in ["Content-Length", "Origin", "Sec-Fetch-", "DNT", "Pragma", "Cache-Control", "Host", "Connection"]:
        headers = {k: v for k, v in headers.items() if not k.startswith(skip)}
    return headers

def download_solarman_report(device_id, device_sn, parent_sn, start_day, end_day):
    headers = parse_headers_file(config.SOLARMAN_HEADERS_FILE)
    with open(config.SOLARMAN_PAYLOAD_FILE, "r", encoding="utf-8") as f:
        payload = json.load(f)
    payload["deviceId"] = device_id
    payload["deviceSn"] = device_sn
    payload["parentSn"] = parent_sn
    payload["startDay"] = start_day
    payload["endDay"] = end_day
    xls_filename = f"{config.EXTRACTED_TEMP_DIR}solarmanpv_{device_id}_{device_sn}_od_{start_day}_do_{end_day}.xlsx"
    print(f"Pobieranie raportu Solarman: {xls_filename}")

    try:
        response = requests.post(config.SOLARMAN_URL, headers=headers, json=payload)
        content = response.content
        encoding = response.headers.get("Content-Encoding")
        print("Content-Encoding:", encoding)
        print("Content-Type:", response.headers.get("Content-Type"))
        print("Pierwsze bajty:", response.content[:4])
        if response.status_code == 200:
            if encoding == "br" and not is_zip(content):
                print("Dekompresja Brotli...")
                content = brotli.decompress(content)
            else:
                print("Plik już rozpakowany lub nie wymaga dekompresji.")
            with open(xls_filename, "wb") as f:
                f.write(content)
            print(f"Plik został zapisany jako {xls_filename}")
        else:
            print("Błąd:", response.status_code, response.text[:500], file=sys.stderr)
    except Exception as e:
        print("Wystąpił błąd podczas pobierania raportu:", str(e), file=sys.stderr)


def download_all_solarman_reports(startDay, endDay):
    # pobiera raporty z zadanego przedziału czasowego dla wszystkich urządzeń z DEVICES_LIST_FILE i.e. devices_list.txt
    with open(config.DEVICES_LIST_FILE, "r") as file:
        for i, line in enumerate(file, start=1):
            device_param = line.strip().split("\t")[:6]
            if len(device_param) != 6:
                print(f"Wiersz {i} ma niewłaściwą liczbę elementów: {device_param}")
                continue
            deviceName,deviceId,deviceSn,parentSn,system,admin = device_param
            print(f"deviceName: {deviceName}, deviceId: {deviceId}, deviceSn: {deviceSn}, parentSn: {parentSn}, system: {system}, admin: {admin}")
            # Pobierz raport dla każdego urządzenia
            download_solarman_report(deviceId,deviceSn,parentSn,startDay,endDay)

def pull_all_solarman():
    # Pobieranie wszystkich raportów Solarman dla wszystkich urządzeń w 2025 roku
    Y = 2025
    for M in [8]:  # Miesiące od 1 do 12
        first_day = f"{Y}-{M:02d}-01"
        last_day_num = calendar.monthrange(Y, M)[1]
        last_day = f"{Y}-{M:02d}-{last_day_num:02d}"
        print(f"{first_day}  →  {last_day}")
        download_all_solarman_reports(first_day, last_day)
    logger.info("pull_all_solarman finished")

def pull_last_solarman():
    # Pobieranie ostatnich raportów Solarman dla wszystkich urządzeń od ostaniej aktualizacji
    # sprawdzenie ostatniego dnia w bazie danych dla każdej instalacji
    engine = connect_db()
    with engine.connect() as connection:
        result = connection.execute("SELECT device_id,sn,parent_sn, MAX(date) FROM solarman_reports GROUP BY sn")
        for row in result:
            device_id,sn,parent_sn, last_date = row
            print(f"Urządzenie {device_id} ma ostatni raport z dnia {last_date}")
            today= calendar.datetime.date.today()
            # Pobierz raport od ostatniego dnia w bazie do dzisiaj
            download_solarman_report(device_id, device_id, device_id, last_date, last_date)



if __name__  == "__main__":
    print("Pobieranie raportu Solarman dla urządzenia SS3ES125P38069 ===========")
    print ("download_solarman_report('229763641','SS3ES125P38069','2754356247', '2025-07-15', '2025-07-15')")
    download_solarman_report('229763641','SS3ES125P38069','2754356247', '2025-07-15', '2025-07-15')
    print("Koniec ===========")
