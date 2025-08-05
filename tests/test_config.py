import pytest
from config import config

def test_base_directories_exist():
    assert config.BASE_DIR.exists(), "BASE_DIR nie istnieje"
    assert config.CONFIG_DIR.exists(), "CONFIG_DIR nie istnieje"
    assert config.TMP_DIR.exists(), "TMP_DIR nie istnieje"
    assert config.OUTPUT_DIR.exists(), "OUTPUT_DIR nie istnieje"

def test_metadata_files_exist():
    assert config.DEVICES_LIST_FILE.exists(), "Brak devices_list.txt"
    assert config.DEVICES_SECRETS_FILE.exists(), "Brak devices_secrets.json (jeśli potrzebny lokalnie)"

def test_reports_and_plots_paths():
    # Nie muszą istnieć, ale sprawdzamy poprawność ścieżek
    assert config.ANOMALY_REPORT_FILE.parent.exists(), "Katalog raportów nie istnieje"
    assert config.DAILY_PLOT_FILE.parent.exists(), "Katalog wykresów nie istnieje"

