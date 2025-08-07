from A1_extract.from_solarman import pull_all_solarman  # Import the function from solarmanpv module
from A3_load import to_mysql
import logging
logging.basicConfig(
    level=logging.INFO,  # lub DEBUG, WARNING, ERROR, CRITICAL
    format='%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


def main():
    # Główna funkcja do pobierania raportów Solarman
    print("Pobieranie raportów Solarman dla wszystkich urządzeń w 2025 roku...")
    pull_all_solarman()
    to_mysql.main()

if __name__ == "__main__":
    main()
