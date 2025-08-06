# 🔎 solar-qa

**solar-qa** to projekt służący do analizy efektywności działania instalacji fotowoltaicznych (PV) oraz wykrywania anomalii w danych z raportów produkcji energii. Celem projektu jest ułatwienie diagnostyki, automatyczne wykrywanie anomalii oraz identyfikacja i klasyfikacja problemów, takich jak spadki wydajności czy przerwy w pracy oraz szacowaniu z tego powodu strat.

---

## 🎯 Główne funkcjonalności

- Pobieranie danych z wielu źródeł (np. Solarman)
- Przetwarzanie i czyszczenie danych pomiarowych
- Automatyczne wykrywanie anomalii (braki, spadki, nieciągłości)
- Generowanie dziennych raportów i wizualizacji
- Ładowanie danych do bazy SQL
- Kategoryzacja problemów i porównania między instalacjami

---

## 🧱 Struktura katalogów
```text
solar-qa/
├── A1_extract/ # Pobieranie danych z instalacji PV
├── A2_transform/ # Czyszczenie, interpolacja, standaryzacja
├── A3_load/ # Zapis danych do bazy danych
├── A4_report/ # Tworzenie zestawień i statystyk, 
├── A5_analyze/ # Analizy, wykrywanie anomalii, 
├── config/ # Konfiguracja projektu i połączenia z bazą
│ ├── config.py
│ ├── db.py
│ ├── devices_list.txt
│ ├── devices_secrets.json
│ └── sql/
├── tmp/ # Katalog tymczasowy (nie wersjonowany)
├── logs/ # Logi działania aplikacji (.gitignore)
├── scratch/ # Próbne skrypty, prototypy, testy lokalne (.gitignore)
├── tests/ # Testy jednostkowe 
├── main.py # Główne wejście do aplikacji 
├── .env # Zmienne środowiskowe (.gitignore)
├── .gitignore
└── README.md
```

