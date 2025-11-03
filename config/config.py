from pathlib import Path
from datetime import date
from dotenv import load_dotenv
import os

# Bazowy katalog projektu (główna lokalizacja repo)
BASE_DIR = Path(__file__).resolve().parent.parent

# Katalogi
CONFIG_DIR   = BASE_DIR / "config"
TMP_DIR      = BASE_DIR / "tmp"
LOGS_DIR     = BASE_DIR / "logs"
OUTPUT_DIR   = BASE_DIR / "output"
REPORTS_DIR  = OUTPUT_DIR / "reports"
PLOTS_DIR    = OUTPUT_DIR / "plots"

# Pliki metadanych i konfiguracyjne
ENV_FILE               = BASE_DIR / ".env"
DEVICES_LIST_FILE      = CONFIG_DIR / "devices_list.txt"
DEVICES_SECRETS_FILE   = CONFIG_DIR / "devices_secrets.json"
LAST_ANALYSIS_DATE_FILE= CONFIG_DIR / "last_analysis_date.txt"


#SOLARMAN specific
SOLARMAN_URL = "https://globalhome.solarmanpv.com/device-s/report/export"
SOLARMAN_HEADERS_FILE = TMP_DIR / "headers.txt"
SOLARMAN_PAYLOAD_FILE = TMP_DIR / "payload.json"

# Załaduj zmienne środowiskowe z pliku .env (jeśli istnieje)
load_dotenv(dotenv_path=ENV_FILE)

# Nazwy plików wyjściowych (dynamiczne lub statyczne)
TODAY = date.today().strftime("%Y-%m-%d")
EXTRACTED_TEMP_DIR= TMP_DIR
EXTRACTED_ARCHIVE_DIR=TMP_DIR

ANOMALY_REPORT_FILE    = REPORTS_DIR / f"anomalies_{TODAY}.pdf"
DAILY_PLOT_FILE        = PLOTS_DIR / f"production_{TODAY}.png"

# Ustawienia analizy (opcjonalne – stałe konfiguracyjne)
THRESHOLD_ANOMALY_KW   = 0.5
PLOT_RESOLUTION_DPI    = 150

def _load_devices():
    devices = {}
    with open(DEVICES_LIST_FILE, "r", encoding="utf-8") as file:
        devices_file_header_line = file.readline().strip()
        devices_file_header = devices_file_header_line.split("\t")
        devices_file_header_length= len(devices_file_header)
        for i, line in enumerate(file, start=1):
            device_param = line.strip().split("\t")[:devices_file_header_length]
            if len(device_param) != devices_file_header_length:
                logger.warning(f"W {DEVICES_LIST_FILE} w wierszu {i} oczekiwano {devices_file_header_length} parametrów) : {device_param}")
                continue

            deviceName, deviceId, deviceSn, parentSn, system, admin,is_pull = device_param
            devices[deviceSn] = {
                "sn" : deviceSn,
                "name": deviceName,
                "id": deviceId,
                "parent_sn": parentSn,
                "system": system,
                "admin": admin,
                "is_pull": is_pull
            }
    return devices

# Wczytanie słownika przy imporcie modułu
DEVICES_LIST = _load_devices()

