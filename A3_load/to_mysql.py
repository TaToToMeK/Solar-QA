from sqlalchemy import text, inspect, exc
import os
import re
import warnings
import pandas as pd
from pathlib import Path
from config.db import connect_db
from config import config
import logging
logger = logging.getLogger(__name__)


warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

def normalize_timestamp(series: pd.Series) -> pd.Series:
    """
    Normalizuje kolumnę z datą/czasem do formatu 'YYYY-MM-DD HH:MM:SS'.
    Jeśli nie da się sparsować, ustawia stałą wartość '1999-12-31 00:00:00'.
    """
    default_str = "1999-12-31 00:00:00"
    parsed = pd.to_datetime(series, errors="coerce", dayfirst=False)

    # Zastąp NaT wartością domyślną, a następnie sformatuj
    filled = parsed.fillna(pd.Timestamp(default_str))
    return filled.dt.strftime('%Y-%m-%d %H:%M:%S')
def normalize_column_name(name):
    """
    Zamienia nazwę kolumny na wersję do bazy danych:
    - usuwa znaki specjalne
    - spacje zmienia na '_'
    - konwertuje na lowercase
    """
    # Usuń wszystko poza literami, cyframi i spacjami
    name = re.sub(r"[^A-Za-z0-9]+", "_", name)
    # Zamień spacje na podkreślenia
    # name = name.replace(" ", "_")
    # Na lowercase
    name = name.lower()
    return name
def correct_updated_time_values(df):
    if "updated_time" in df.columns:
        df["updated_time"] = normalize_timestamp(df["updated_time"])
        print("✅ Przetworzono kolumnę 'updated_time'")
    if "system_time" in df.columns:
        df["system_time"] = normalize_timestamp(df["system_time"])
        print("✅ Przetworzono kolumnę 'system_time'")
def clean_dataframe_for_insert(df, column_type_map):
    """
    Czyści dataframe zgodnie z typami kolumn SQL (decimal, datetime, varchar)
    """
    df_clean = df.copy()
    for col in df_clean.columns:
        if col not in column_type_map:
            continue
        sql_type, max_length = column_type_map[col]
        if sql_type in ["DECIMAL","INT"]:
            df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce")
        elif sql_type == "DATETIME":
            df_clean[col] = pd.to_datetime(df_clean[col], errors="coerce")
        elif sql_type in ("VARCHAR", "CHAR") and max_length:
            df_clean[col] = df_clean[col].astype(str).apply(
                lambda x: x[:max_length] if isinstance(x, str) else x
            )
    return df_clean
def clean_duplicated(df_clean):
    df_clean = df_clean.sort_values('updated_time', ascending=False)
    df_clean = df_clean.drop_duplicates(subset=['sn', 'system_time'], keep='first')
    df_clean = df_clean.sort_values(['sn','updated_time'])
    return df_clean
def summarize_dataframe_old2(df):
    summary = []

    for col in df.columns:
        series = df[col]
        dtype = str(series.dtype)

        try:
            min_val = series.min()
            max_val = series.max()
        except Exception as e:
            min_val = max_val = f"❌ {e}"

        nulls = series.isna().sum()

        summary.append({
            "column": col,
            "dtype": dtype,
            "min": min_val,
            "max": max_val,
            "nulls": nulls
        })
    return pd.DataFrame(summary)


def create_temp_table(engine, table_name, columns):
    """
    Tworzy tymczasową tabelę MySQL o nazwie table_name na podstawie listy kolumn.
    Kolumna 'updated_time' jest typu DATETIME, reszta TEXT.
    """
    columns_sql = []
    for col in columns:
        if col == "updated_time":
            col_type = "DATETIME"
        else:
            col_type = "TEXT"  # możesz zmienić na VARCHAR(255) dla krótkich pól
        columns_sql.append(f"`{col}` {col_type}")

    drop_table_sql = f""" DROP TABLE IF EXISTS `{table_name}`;  """
    with engine.connect() as conn:
        conn.execute(text(drop_table_sql))
        print(f"✅ Tymczasowa tabela '{table_name}' została usunięta: ")
    create_table_sql = f"""    CREATE TABLE `{table_name}` ({', '.join(columns_sql)});"""
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        print(f"✅ Tymczasowa tabela '{table_name}' została utworzona z kolumnami: {columns}")
def sync_columns_with_target(engine, temp_table, target_table, columns):
    """
    Porównuje kolumny z My_Columns z tabelą docelową i dodaje brakujące kolumny.
    Typ kolumny wywnioskowany z danych tymczasowej tabeli.
    """
    inspector = inspect(engine)

    # Istniejące kolumny w solarman_data
    existing_cols = [col["name"] for col in inspector.get_columns(target_table)]
    print(f"📝 Istniejące kolumny w {target_table}: {existing_cols}")

    # Brakujące kolumny
    missing_cols = [col for col in columns if col not in existing_cols]

    if missing_cols:
        print(f"➕ Dodajemy brakujące kolumny do {target_table}: {missing_cols}")
        with engine.connect() as conn:
            for col in missing_cols:
                # Wywnioskuj typ kolumny z danych w temp_table
                result = conn.execute(text(f"""
                    SELECT `{col}` FROM `{temp_table}` WHERE `{col}` IS NOT NULL LIMIT 100
                """))
                sample_values = [row[0] for row in result]

                # Domyślnie VARCHAR
                col_type = "VARCHAR(255)"
                for val in sample_values:
                    if isinstance(val, (int, float)):
                        col_type = "DECIMAL(20, 4)"
                        break
                    elif isinstance(val, str) and "-" in val and ":" in val:
                        # Prosta heurystyka dla daty
                        col_type = "DATETIME"
                        break

                print(f"🔹 Dodajemy kolumnę: {col} ({col_type})")
                conn.execute(text(f"""
                    ALTER TABLE `{target_table}` ADD COLUMN `{col}` {col_type} NULL
                """))
    else:
        print("✅ Wszystkie kolumny z My_Columns już istnieją w tabeli docelowej.")
def get_column_names(engine, table_name):
    """
    Pobiera listę nazw kolumn z podanej tabeli w aktualnie podłączonej bazie.
    """
    query = text("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'PVMONITOR'
          AND TABLE_NAME = :table_name
        ORDER BY ORDINAL_POSITION;
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"table_name": table_name}).fetchall()

    # Zamień wynik na listę nazw kolumn
    column_names = [row[0] for row in result]
    return column_names
def get_column_type_map(engine, table_name):
    """
    Zwraca słownik: {kolumna: (typ_SQL, max_długość)}
    """
    query = text("""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = :table_name
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"table_name": table_name}).fetchall()
    return {row[0]: (row[1].upper(), row[2]) for row in result}
def create_import_table(engine, import_table_name):
    """
        Usuwa tymczasową tabelę (jeśli istnieje) i tworzy ją na podstawie struktury tabeli docelowej.
    """
    main_table_name = os.getenv("DB_MAINTABLE")
    with engine.begin() as conn:
        print(f"🗑️ DROP TABLE IF EXISTS `{import_table_name}`")
        conn.execute(text(f"DROP TABLE IF EXISTS `{import_table_name}`;"))

        print(f"🛠️ CREATE TABLE `{import_table_name}` LIKE `{main_table_name}`")
        conn.execute(text(f"CREATE TABLE `{import_table_name}` LIKE `{main_table_name}`;"))

    print(f"✅ Utworzono importową tabelę `{import_table_name}` na podstawie `{main_table_name}`.")
def safe_insert_dataframe_to_sql(engine, df, import_table_name):
    #Wstawia dane z df_db_data do tabeli import_table_name, tylko dla kolumn istniejących w tabeli SQL.
    #Czyści dane zgodnie z typami SQL.
    try:
        # 1. Pobierz kolumny z tabeli
        column_type_map = get_column_type_map(engine, import_table_name)
        db_columns = list(column_type_map.keys())

        # 2. Wybierz tylko wspólne kolumny
        common_columns = [col for col in df.columns if col in db_columns]
        if not common_columns:
            print("❌ Brak wspólnych kolumn do importu.")
            return

        df_subset = df[common_columns]
        # 3a Raport z czystości danych przed wstawieniem
        print("🔍 Wiersze z błędnymi datami w system_time :", df[pd.to_datetime(df["system_time"], errors="coerce").isna()].shape[0])

        for col in common_columns:
            sql_type = column_type_map[col][0]
            if sql_type in ["DECIMAL", "INT"]:
                # Sprawdź, czy kolumna zawiera nieparsowalne liczby
                failed = pd.to_numeric(df[col], errors="coerce").isna().sum()
                if failed > 0:
                    print("🔍 Wiersze z nieparsowalnymi liczbami w", col, ":", failed)
            elif sql_type == "DATETIME":
                # Sprawdź, czy kolumna zawiera nieparsowalne daty
                failed = pd.to_datetime(df[col], errors="coerce").isna().sum()
                if failed > 0:
                    print("🔍 Wiersze z nieparsowalnymi datami w", col, ":", failed)



        # 3. Oczyść dane do typów SQL
        df_clean = clean_dataframe_for_insert(df_subset, column_type_map)
        df_clean = clean_duplicated(df_clean)

        # 4. Insert do SQL
        df_clean.to_sql(import_table_name, con=engine, if_exists='append', index=False, method='multi', chunksize=1000)
        print(f"✅ Wstawiono {len(df_clean)} rekordów do {import_table_name} (kolumny: {common_columns})")

    except exc.SQLAlchemyError as e:
        print(f"❌ Błąd przy zapisie do bazy: {e}")
def summarize_dataframe(df, DB_Columns):
    summary = []

    for col in df.columns:
        series = df[col]
        dtype = str(series.dtype)

        try:
            min_val = series.min()
            max_val = series.max()
        except Exception as e:
            min_val = max_val = f"❌ {e}"

        nulls = series.isna().sum()

        if dtype.startswith("int"):
            sql_type = "INT"
        elif dtype == "float64":
            sql_type = "DECIMAL(10,2)"
        else:
            sql_type = "VARCHAR(40)"

        sql_def = f"ADD COLUMN `{col}` {sql_type} DEFAULT NULL,"
        if col in DB_Columns:
            exists_flag = 1
            sql_def=''
        else:
            exists_flag = 0

        summary.append({
            "column": col,
            "dtype": dtype,
            "min": min_val,
            "max": max_val,
            "nulls": nulls,
            "exists_in_db": exists_flag,
            "ALTER TABLE SOLARMAN_DATA": sql_def
        })

    return pd.DataFrame(summary)
def merge_import_to_main(engine, import_table="IMPORT_DATA"):
    main_table = os.getenv("DB_MAINTABLE")
    """
    Wstawia dane z tabeli importowej do głównej:
    - nie nadpisuje istniejących rekordów (wg klucza sn, system_time, updated_time)
    - liczy ile rekordów się powtarza, a ile różni
    - zwraca podsumowanie
    """
    with engine.begin() as conn:
        # 1. Powtórzenia wg klucza
        Q1=f"""
            SELECT COUNT(*) FROM {import_table} i
            JOIN {main_table} s
              ON i.sn = s.sn AND
                 i.system_time = s.system_time 
#                AND i.updated_time = s.updated_time
        """
        print (f"Q1: {Q1}")
        duplicates = conn.execute(text(Q1)).scalar()

        # 2. Różniące się rekordy
        Q2=f"""
            SELECT COUNT(*) FROM {import_table} i
            JOIN {main_table} s
              ON i.sn = s.sn AND
                 i.system_time = s.system_time 
#                AND i.updated_time = s.updated_time
            WHERE CONCAT_WS('|', {','.join(f'i.`{col}`' for col in ['sn','system_time'])}, '') <> 
                  CONCAT_WS('|', {','.join(f's.`{col}`' for col in ['sn','system_time'])}, '')
        """
        print (f"Q2: {Q2}")
        differing = conn.execute(text(Q2)).scalar()

        # 3. Wstaw tylko nowe
        Q3=f"""
            INSERT INTO {main_table}
            SELECT * FROM {import_table} i
            WHERE NOT EXISTS (
                SELECT 1 FROM {main_table} s
                WHERE s.sn = i.sn
                  AND s.system_time = i.system_time
#                  AND s.updated_time = i.updated_time
            )
        """
        print(f"Q3: {Q3}")
        inserted_result = conn.execute(text(Q3))

        inserted_rows = inserted_result.rowcount
    print(f"🔁 Powtórzenia na kluczu (`sn`, `system_time`, `updated_time`): {duplicates}")
    print(f"⚠️  Rekordy różniące się zawartością (kolizje danych): {differing}")
    print(f"✅ Wstawiono nowych wierszy do {main_table}: {inserted_rows}")
    return {
        "duplicates": duplicates,
        "differing": differing,
        "inserted": inserted_rows
    }
def merge_import_to_main_previousversion(engine, import_table="IMPORT_DATA"):
    main_table = os.getenv("DB_MAINTABLE")
    """
    Wstawia dane z tabeli importowej do głównej:
    - nie nadpisuje istniejących rekordów (wg klucza sn, system_time, updated_time)
    - liczy ile rekordów się powtarza, a ile różni
    - zwraca podsumowanie
    """
    with engine.begin() as conn:
        # 1. Powtórzenia wg klucza
        duplicates = conn.execute(text(f"""
            SELECT COUNT(*) FROM {import_table} i
            JOIN {main_table} s
              ON i.sn = s.sn AND
                 i.system_time = s.system_time AND
                 i.updated_time = s.updated_time
        """)).scalar()

        # 2. Różniące się rekordy
        differing = conn.execute(text(f"""
            SELECT COUNT(*) FROM {import_table} i
            JOIN {main_table} s
              ON i.sn = s.sn AND
                 i.system_time = s.system_time AND
                 i.updated_time = s.updated_time
            WHERE CONCAT_WS('|', {','.join(f'i.`{col}`' for col in ['sn','system_time','updated_time'])}, '') <> 
                  CONCAT_WS('|', {','.join(f's.`{col}`' for col in ['sn','system_time','updated_time'])}, '')
        """)).scalar()

        # 3. Wstaw tylko nowe
        inserted_result = conn.execute(text(f"""
            INSERT INTO {main_table}
            SELECT * FROM {import_table} i
            WHERE NOT EXISTS (
                SELECT 1 FROM {main_table} s
                WHERE s.sn = i.sn
                  AND s.system_time = i.system_time
                  AND s.updated_time = i.updated_time
            )
        """))

        inserted_rows = inserted_result.rowcount
    print(f"🔁 Powtórzenia na kluczu (`sn`, `system_time`, `updated_time`): {duplicates}")
    print(f"⚠️  Rekordy różniące się zawartością (kolizje danych): {differing}")
    print(f"✅ Wstawiono nowych wierszy do {main_table}: {inserted_rows}")
    return {
        "duplicates": duplicates,
        "differing": differing,
        "inserted": inserted_rows
    }
def list_all_file_paths(start_path, pattern):
    #Zwraca listę ścieżek do plików pasujących do wzorca (np. 'sol*.xlsx') w katalogu start_path (rekurencyjnie).
    start = Path(start_path)
    return [str(p) for p in start.rglob(pattern) if p.is_file()]
def process_excel_file(file_path, import_table_name='IMPORT_DATA', debug_table_name=''):
    df = pd.read_excel(file_path)
    file_columns = df.columns.tolist()
    norm_columns = [normalize_column_name(col) for col in file_columns]
    columns=list(zip(file_columns, norm_columns))
    #print("🔍 Kolumny w pliku:")
    #for i, l in enumerate (columns):
    #    print(f"{i+1:3} \t{l[0]:40} \t{l[1]:40}")
    #print(f"kolumny w pliku: {file_columns}")
    #print(f"kolumny  w pliku znormalizowe: {norm_columns}")
    df.columns = norm_columns
    correct_updated_time_values(df)

    # Połączenie do bazy
    engine = connect_db()
    create_import_table(engine, import_table_name)
    # weźmy kolumny z tabeli docelowej
    db_columns=get_column_names(engine, import_table_name)
    print(f"🔍 kolumny w tabeli {import_table_name} : {db_columns}")

    summary_df = summarize_dataframe(df,db_columns)
    summary_df.to_csv("/media/ramdisk/summary.csv", index=False, encoding="utf-8")
    print(f"summary zapisano do pliku /media/ramdisk/summary.csv")

    df.to_csv("/media/ramdisk/analiza.csv", index=False, encoding="utf-8")
    print("📊 Zapisano dane do pliku /media/ramdisk/analiza.csv")
    if debug_table_name!='':
        df.to_sql(debug_table_name, con=engine, if_exists='append', index=False)
        print(f"📤 Zaimportowano {len(df)} rekordów do tabeli {debug_table_name}")
        sync_columns_with_target(engine, debug_table_name, 'SOLARMAN_DATA', db_columns)

    safe_insert_dataframe_to_sql(engine, df, import_table_name)
    merge_import_to_main(engine,import_table_name)
    return ("process_excel_file - ended")

def main():
    # Główna funkcja do przetwarzania plików Excel
    all_files = list_all_file_paths(config.EXTRACTED_TEMP_DIR, "solarmanpv*.xlsx")
    # all_files=list_all_file_paths("/home/astek/Dokumenty/2025.Energetyka/PV-MONITOR/PUK-Bielany/", "sol*.xlsx")
    print(f"Znaleziono {len(all_files)}  plików  w {config.EXTRACTED_TEMP_DIR}sol*.xlsx :")
    print("\n".join(all_files))
    for file in all_files:
        print(f"Przetwarzanie pliku: {file}")
        process_excel_file(file, import_table_name='IMPORT_DATA', debug_table_name='')
        print("--------------------------------------------------")
    logger.info("process_excel_file completed for all files.")

if __name__  == "__main__":
    main()

'''
process_excel_file("/home/astek/Dokumenty/2025.Energetyka/PV-MONITOR/PUK-Bielany/solarmanpv_229763641_SS3ES125P38069_od_2025-07-01_do_2025-07-31.xlsx")
process_excel_file("/home/astek/Dokumenty/2025.Energetyka/PV-MONITOR/PUK-Bielany/solarmanpv_229763641_SS3ES125P38069_od_2025-01-01_do_2025-01-31.xlsx")
process_excel_file("/home/astek/Dokumenty/2025.Energetyka/PV-MONITOR/SolarmanSampleSize5.xlsx")
process_excel_file("/home/astek/Dokumenty/2025.Energetyka/PV-MONITOR/solarmanpv_230709814_SS3ES150NAT230_short.xlsx")
process_excel_file("/home/astek/Dokumenty/2025.Energetyka/PV-MONITOR/SolarmanSample.xlsx")
process_excel_file("/home/astek/Dokumenty/2025.Energetyka/PV-MONITOR/PUK-Bielany/solarmanpv_230709814_SS3ES150NAT230_od_2025-02-01_do_2025-02-28.xlsx")
process_excel_file("/home/astek/Dokumenty/2025.Energetyka/PV-MONITOR/PUK-Bielany/solarmanpv_229763641_SS3ES125P38069_od_2025-01-01_do_2025-01-31.xlsx")
process_excel_file("/home/astek/Dokumenty/2025.Energetyka/PV-MONITOR/PUK-Bielany/solarmanpv_229763641_SS3ES125P38069_od_2025-01-01_do_2025-01-31.xlsx")
process_excel_file("/home/astek/Dokumenty/2025.Energetyka/PV-MONITOR/PUK-Bielany/solarmanpv_229763641_SS3ES125P38069_od_2025-02-01_do_2025-02-28.xlsx")
process_excel_file("/home/astek/Dokumenty/2025.Energetyka/PV-MONITOR/PUK-Bielany/solarmanpv_230731855_SS1ES122M5G764_od_2025-02-01_do_2025-02-28.xlsx")
process_excel_file("/home/astek/Dokumenty/2025.Energetyka/PV-MONITOR/PUK-Bielany/solarmanpv_230731624_SS1ES120P4U121_od_2025-02-01_do_2025-02-28.xlsx")
process_excel_file("/home/astek/Dokumenty/2025.Energetyka/PV-MONITOR/PUK-Bielany/solarmanpv_230731624_SS1ES120P4U121_od_2025-01-01_do_2025-01-31.xlsx")
process_excel_file("/home/astek/Dokumenty/2025.Energetyka/PV-MONITOR/PUK-Bielany/solarmanpv_230731855_SS1ES122M5G764_od_2025-01-01_do_2025-01-31.xlsx")
process_excel_file("/home/astek/Dokumenty/2025.Energetyka/PV-MONITOR/PUK-Bielany/solarmanpv_229763641_SS3ES125P38069_od_2025-01-01_do_2025-01-31.xlsx")
'''


