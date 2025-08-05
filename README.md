## Kolejność etapów w projekcie PV-monitor:

0. `config/` – konfiguracja projektu, lista urządzeń, pliki `.env`
1. `extract/` – pobieranie danych z Solarman i innych źródeł
2. `transform/` – czyszczenie, interpolacja, ujednolicanie
3. `load/` – zapis danych do bazy lub eksport do plików
4. `report/` – generowanie raportów i wykresów dziennych
5. `analyze/` – analiza anomalii, podsumowania, wykresy porównawcze