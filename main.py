import logging
from logging import Logger
import argparse
import utils.logging_config

from A1_extract.from_solarman import pull_all_solarman, \
    download_solarman_report, download_all_solarman_reports
from A3_load import to_mysql

def _parse_level(name: str) -> int:
    lvl = logging.getLevelName((name or "INFO").upper())
    return lvl if isinstance(lvl, int) else logging.INFO

parser = argparse.ArgumentParser()
parser.add_argument(
    "--log-level",
    default="DEBUG",
    # dodajemy customowe poziomy albo usuń choices, jeśli chcesz pełną dowolność
    #choices=["TRACE", "VERBOSE", "DEBUG", "INFO", "NOTICE", "WARNING", "ERROR", "ALERT", "CRITICAL"],
    help="Poziom logowania (domyślnie: INFO)"
)
args = parser.parse_args()

logging.basicConfig(
    level=_parse_level(args.log_level),  # <-- użyj parsera poziomów
    format="%(asctime)s | %(levelname)-7s |%(lineno)4d:%(filename)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# (opcjonalnie) wycisz głośne biblioteki:
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy").setLevel(logging.WARNING)
logger: Logger = logging.getLogger(__name__)

def main():
    logger.info(f'Dostępne poziomy logowania: "TRACE", "VERBOSE", "DEBUG", "INFO", "NOTICE", "WARNING", "ERROR", "ALERT", "CRITICAL"')
    logger.notice("Główna funkcja do pobierania plików raportów Solarman")
    xls_list=download_all_solarman_reports(None,None)
    #pull_all_solarman(2025)
    logger.notice("Główna funkcja ładowania raportów Solarman do bazy")
    to_mysql.main(xls_list)
    logger.notice("main.py zakończony")

if __name__ == "__main__":
    main()
