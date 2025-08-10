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
DEVICES_LIST_FILE      = CONFIG_DIR / "devices_list.txt"
DEVICES_SECRETS_FILE   = CONFIG_DIR / "devices_secrets.json"
ENV_FILE               = BASE_DIR / ".env"

#SOLARMAN specific
SOLARMAN_URL = "https://globalhome.solarmanpv.com/device-s/report/export"
SOLARMAN_HEADERS_FILE = TMP_DIR / "headers.txt"
SOLARMAN_PAYLOAD_FILE = TMP_DIR / "payload.json"

# Załaduj zmienne środowiskowe z pliku .env (jeśli istnieje)
load_dotenv(dotenv_path=ENV_FILE)

# Nazwy plików wyjściowych (dynamiczne lub statyczne)
TODAY = date.today().strftime("%Y-%m-%d")
EXTRACTED_TEMP_DIR= '/media/ramdisk/'
EXTRACTED_ARCHIVE_DIR='~/PV-MONITOR/'

ANOMALY_REPORT_FILE    = REPORTS_DIR / f"anomalies_{TODAY}.pdf"
DAILY_PLOT_FILE        = PLOTS_DIR / f"production_{TODAY}.png"

# Ustawienia analizy (opcjonalne – stałe konfiguracyjne)
THRESHOLD_ANOMALY_KW   = 0.5
PLOT_RESOLUTION_DPI    = 150

def _load_devices():
    devices = {}
    with open(DEVICES_LIST_FILE, "r", encoding="utf-8") as file:
        next(file)  # pomija nagłówek
        for i, line in enumerate(file, start=1):
            device_param = line.strip().split("\t")[:6]
            if len(device_param) != 6:
                logger.warning(f"Wiersz {i} ma niewłaściwą liczbę elementów: {device_param}")
                continue

            deviceName, deviceId, deviceSn, parentSn, system, admin = device_param
            devices[deviceSn] = {
                "sn" : deviceSn,
                "name": deviceName,
                "id": deviceId,
                "parent_sn": parentSn,
                "system": system,
                "admin": admin
            }
    return devices

# Wczytanie słownika przy imporcie modułu
DEVICES_LIST = _load_devices()
