from logging import Logger

from A1_extract.from_solarman import pull_all_solarman  # Import the function from solarmanpv module
from A3_load import to_mysql
import logging
logging.basicConfig(
    level=logging.DEBUG,  # lub DEBUG, INFO, WARNING, ERROR, CRITICAL
    format='%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger: Logger = logging.getLogger(__name__)

def main():
    logger.info("Główna funkcja do pobierania plików raportów Solarman")
    pull_all_solarman(2025)
    logger.info("Główna funkcja ładowania raportów Solarman do bazy")
    to_mysql.main()

if __name__ == "__main__":
    main()
