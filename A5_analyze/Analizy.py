from config.db import connect_db
from config import config
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import logging
logging.basicConfig(
    level=logging.INFO,  # lub DEBUG, WARNING, ERROR, CRITICAL
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def analyze_instalations(engine):
    print("Pobranie statystyk STATS_N_DAYS  z bazy danych")
    df_coeff = pd.read_sql("SELECT sn, coefficient FROM V_STATS_N_DAYS", engine)
    return df_coeff
def load_db_data(engine, date_str: str) -> pd.DataFrame:
    print (f"\nPobieranie danych z bazy danych dla dnia: {date_str}")
    """
    Pobiera dane z tabeli PVMONITOR.SOLARMAN_DATA tylko z wybranego dnia.
    Args:
        engine: obiekt SQLAlchemy (np. z connect_db())
        date_str: data jako string w formacie 'YYYY-MM-DD'
    Returns:
        pd.DataFrame z kolumnami [sn, system_time, daily_production_active_kwh_]
    """
    date_start = datetime.strptime(date_str, "%Y-%m-%d")  # początek dnia
    date_end = date_start + timedelta(days=1)

    query = """
        SELECT sn, system_time, daily_production_active_kwh_  
        FROM PVMONITOR.SOLARMAN_DATA
        WHERE system_time >= %s AND system_time < %s
          AND daily_production_active_kwh_ IS NOT NULL
          AND daily_production_active_kwh_ > 0 -- # obsługa case typu sn#069 2025-07-05 21:10
          ;
    """

    # params jako lista lub tuple
    df = pd.read_sql(query, engine, params=(date_start, date_end))

    df['system_time'] = pd.to_datetime(df['system_time'])
    df = df.sort_values(['sn', 'system_time']).reset_index(drop=True)

    return df
def x(group, idx):
    if idx < len(group):
        return group.iloc[idx]['system_time']
    else:
        print (f"x index idx poza zakresem")
    return False
def y(group, idx):
    return group.iloc[idx]['daily_production_active_kwh_'] if idx < len(group) else 0
def interpolate_value(tick, x1, y1, x2, y2):
    # Interpoluje wartość na podstawie dwóch punktów (x1, y1) i (x2, y2).
    if x1 == x2:
        return y1  # Unikamy dzielenia przez zero

    slope = (y2 - y1) / (x2 - x1).total_seconds()
    return y1 + slope * (tick - x1).total_seconds()
def resample_sn(group, full_index, start_raw, end_raw):
    #add at the begining of the group add value 0 for time_stamp=start_raw
    row = {'system_time': start_raw,'daily_production_active_kwh_': 0}
    first_row = pd.DataFrame([row])
    group = pd.concat([first_row, group], ignore_index=True)
    ll=len(group)
    print (f"resample_sn; group length: {ll}")
    idxleft=0
    idxright=1
    value = 0
    result = []
    for tick in full_index:
        if tick< x(group,idxleft ): # nie mamy jeszcze danych w group; w resamplingu dostawiamy value=0
            result.append(0)
            continue
        while idxright<ll and x(group, idxright) < tick:
            idxright += 1
        if idxright>=ll or idxleft>=ll: # nie mamy już danch w group; w resamplingu dostawiamy last value
            result.append(value) # last value
            continue
        idxleft = idxright - 1
        x1 = x(group, idxleft)
        x2= x(group, idxright)
        y1= y(group, idxleft)
        y2= y(group, idxright)
        value=round(interpolate_value(tick, x1, y1, x2, y2),3)
        #print (f"x1:{x1}\t tick:{tick}\t x2:{x2}\t [x1<tick<x2]: {x1<tick<x2} \t idxleft: {idxleft}\t idxleft: {idxleft} \t idxright: {idxright}\t value: {value}")
        result.append(value)

    print(f"ending;   resampled lenght: {len(result)}")

    return result

def timeprobe_to_hours(timeprobe: str) -> float:
    # Konwertuje string np. '15min', '1H', '90s' na liczbę godzin (float).
    return pd.to_timedelta(timeprobe).total_seconds() / 3600

def normalize_kW(resampled,timeprobe:str,coeff)-> pd.DataFrame:    #    Oblicza różnicę między kolejnymi wartościami w resamplowanym DataFrame.
    if len(resampled) < 2:
        return resampled  # Nie ma różnicy do obliczenia
    kW_values = [0]  # Pierwsza wartość różnicy jest zerowa
    for i in range(1, len(resampled)):
        dY = max(0,resampled[i] - resampled[i - 1])  # obsługa case typu sn#069 2025-07-05 21:10
        kW = dY / timeprobe_to_hours(timeprobe)  # Przekształcamy różnicę na kW
        norm_kW=kW/coeff
        kW_values.append(norm_kW)
    return kW_values

def get_coeff_for_sn(df_coeff, sn):
    return df_coeff[df_coeff['sn'] == sn]['coefficient'].values[0] if not df_coeff[df_coeff['sn'] == sn].empty else 1
def interpolate_at(df, t):
    #Zwraca liniowo interpolowaną wartość w chwili `t`.
    #Zakłada df z DateTimeIndex i jedną kolumną.
    if t in df.index:
        return df.loc[t].values[0]
    if t < df.index[0] or t > df.index[-1]:
        raise ValueError("Czas poza zakresem indeksu")
    left = df[df.index <= t].iloc[-1]
    right = df[df.index >= t].iloc[0]
    t_val = np.interp(
        t.value,  # ns timestamp
        [left.name.value, right.name.value],
        [left.values[0], right.values[0]]
    )
    #print (f"Interpolacja dla {t} z {left.name}={left.values[0]} i {right.name}={right.values[0]} daje {t_val}")
    return t_val

def trapezoidal_integral(df, t1, t2):
    """
    Liczy całkę z danych (kW) pomiędzy t1 a t2 metodą trapezów.
    Automatycznie interpoluje t1 i t2, jeśli nie są w indeksie.
    Zwraca wartość w kWh.
    """
    assert df.index.is_monotonic_increasing and df.index.is_unique, "Index musi być rosnący i unikalny"
    assert t1 < t2
    df_sel = df[(df.index >= t1) & (df.index <= t2)].copy()
    # Interpoluj t1/t2 jeśli nie istnieją w indeksie
    if t1 not in df_sel.index:
        df_sel.loc[t1] = interpolate_at(df, t1)
    if t2 not in df_sel.index:
        df_sel.loc[t2] = interpolate_at(df, t2)
    df_sel = df_sel.sort_index()
    #debug
    target = pd.Timestamp("2025-07-02 12:54:00")
    #if abs((t1 - target).total_seconds()) < 30:
    #    save_df_sel_debug(df_sel) # 4 debug

    values = df_sel.iloc[:, 0].values # zakładamy że dane są w pierwszej kolumnie df_sel
    times = df_sel.index.astype(np.int64) / 1e9  # sekundy
    trapz_integral=np.trapz(values, x=times) / 3600  # kWh
    average_power = trapz_integral / ((t2 - t1).total_seconds() / 3600)  # średnia moc w kW

    return average_power

def save_df_sel_debug(df_sel: pd.DataFrame, filename="debug_integral.xlsx"):

    # Dodaj kolumny pomocnicze: timestamp i timestamp_sec
    df_debug = df_sel.copy()
    df_debug["timestamp"] = df_debug.index
    df_debug["timestamp_sec"] = df_debug.index.astype(np.int64) / 1e9  # sekundy

    # Zapisz do pliku xlsx
    filename= f"{config.TMP_DIR}/{filename}"
    df_debug.to_excel(filename, index=False)
    print(f"Zapisano dane do pliku: {filename}")

def interpolate_energy_linear_grid(df_db_data: pd.DataFrame, df_coeff, date_str, timeprobe: str):
    # df_db_data musi zawierać kolumny 'sn', 'system_time', 'daily_production_active_kwh_'
    print("\n=================\nInterpolacja energii na siatce czasowej:", timeprobe)
    df_db_data = df_db_data.copy()
    df_db_data['system_time'] = pd.to_datetime(df_db_data['system_time'])
    df_db_data = df_db_data.sort_values(['sn', 'system_time'])
    start_raw = df_db_data['system_time'].min()
    end_raw = df_db_data['system_time'].max()
    start = start_raw.floor('H')  # w dół do pełnej godziny
    end = end_raw.ceil('10min')  # w górę
    # Tworzenie równomiernej siatki co timeprobe
    full_index = pd.date_range(start=start, end=end, freq=timeprobe)
    #full_index = full_index[full_index >= start_raw]
    print(f"start: {start}, end: {end}, len: {len(full_index)}")
    #print (full_index)
    df_all_sn_kW=pd.DataFrame(index=full_index)

    # Iteracja po grupach sn
    for sn, group in df_db_data.groupby('sn'):
        # utwórz unormowany całościowy df_db_data dla wszystkich instalacji
        logger.info(f"\nPrzetwarzanie sn: {sn}, liczba punktów: {len(group)}")
        # if sn != 'SS3ES125P38069':      continue #  odkomentuj do testów
        # print (group)
        sn_resampled=resample_sn (group, full_index, start_raw, end_raw)
        coeff= get_coeff_for_sn(df_coeff, sn)
        logger.info(f"SN: {sn}, Coefficient: {coeff}")
        sn_kW = normalize_kW(sn_resampled,timeprobe,coeff)
        #sn_kW=sn_resampled #odkomentuj do testów
        df_resampled = pd.DataFrame({'tick': full_index, 'iterpolated_values': sn_kW})
        df_all_sn_kW[sn]= sn_kW # dodajemy kolumnę z mocą kW dla danego sn
        #plot_interpolation_vs_original(df_resampled, group)

    for sn, group in df_db_data.groupby('sn'):

        logger.info(f"Analiza instalacji SN: {sn}")
        group_sn = df_db_data[df_db_data['sn'] == sn].reset_index(drop=True)

        try:
            analyze_sn(sn, get_coeff_for_sn(df_coeff, sn), group_sn, df_all_sn_kW, full_index,timeprobe, date_str)
        except Exception as e:
            logger.error(f"Błąd podczas analizy instalacji {sn}: {e}")



    df_all_sn_kW['median_kW'] = df_all_sn_kW.median(axis=1, skipna=True)
    plot_all_with_median(df_all_sn_kW, date_str, save_path=f"{config.PLOTS_DIR}/median_{date_str}.png")
    #plot_all_with_median(df_all_sn_kW, date_str)
    return df_all_sn_kW #all

def analyze_sn(sn, coeff, group, df_all_sn_kW,full_index,timeprobe:str, date_str: str) -> None:
    # sn - identyfikator instalacji
    # coeff - współczynnik dla instalacji
    # group - DataFrame z danymi źródłowymi dla tej instalacji    
    # df_all_sn_kW - DataFrame z mocą unormowaną kW dla wszystkich instalacji
    # timeprobe - siatka czasowa, np. '1min', '2min'
    # policz medianę kW z ominięciem aktualnego sn:
    df_bez_sn_kW = df_all_sn_kW.drop(columns=sn)
    df_mediana_bez_sn = df_bez_sn_kW.median(axis=1, skipna=True)
    # przelicz unormowaną medianę na expected_profile w kW mnożąc przez coeff
    df_expected_profile = pd.DataFrame({
#        'first_normalized': df_mediana_bez_sn.values,
        'expected': df_mediana_bez_sn.values * coeff
    }, index=full_index)
    #plot_all_power_series(df_expected_profile, sn, date_str) # sam expected power profile

    rows = []
    for i in range(1, len(group)):
        prev_row = group.loc[i - 1]
        curr_row = group.loc[i]

        t_prev = prev_row['system_time']
        t_curr = curr_row['system_time']
        delta_t_s = (t_curr - t_prev).total_seconds()

        e_prev = prev_row['daily_production_active_kwh_']
        e_curr = curr_row['daily_production_active_kwh_']
        delta_kWh = e_curr - e_prev

        # Pomijamy błędne przypadki
        if delta_t_s <= 0 or delta_kWh < 0:
            continue

        power_kW = delta_kWh / (delta_t_s / 3600)

        # Porównanie z profilem oczekiwanym
        expected_power_kW=trapezoidal_integral(df_expected_profile, t_prev, t_curr)
        #print (f"t_curr={t_curr}   expected_power_kW= {expected_power_kW:.3f}")
        # Zapis do listy słowników
        rows.append({
#            'sn': curr_row['sn'],
            'system_time': t_curr,
#            'delta_t_s': delta_t_s,
#            'delta_kWh': delta_kWh,
            'power_kW': power_kW,
            'expected_power_kW': expected_power_kW,
            'difference': power_kW-expected_power_kW
        })

    # Utwórz nowy DataFrame z wyników
    df_power = pd.DataFrame(rows).set_index('system_time')
    #print (df_power)
    plot_all_power_series(df_power, sn, date_str)



    # zapisz w excel
    #df_expected_profile.to_excel(f"{config.TMP_DIR}/expected_profile_{sn}_{date_str}.xlsx", index=True)
    #plot_all_power_series(df_expected_profile,"wszystkich", date_str)

def plot_interpolation_vs_original(result: pd.DataFrame, group: pd.DataFrame) -> None:
    """
    Rysuje porównanie interpolowanych danych z oryginalnymi pomiarami.

    Parametry:
        result (pd.DataFrame): musi zawierać kolumny 'tick' oraz 'iterpolated_values'
        group (pd.DataFrame): musi zawierać kolumny 'system_time' oraz 'daily_production_active_kwh_'
    """

    sn = group['sn'].iloc[0] if 'sn' in group.columns else 'brak SN'

    plt.figure(figsize=(12, 5))

    # Interpolacja – linia
    plt.plot(result['tick'], result['iterpolated_values'],
             label='Interpolacja (resamplowana)', color='blue',  marker='o',markersize=2, linewidth=1)

    # Oryginalne dane – punkty
    plt.scatter(group['system_time'], group['daily_production_active_kwh_'],
                label='Dane oryginalne', color='orange', marker='o', s=30)

    plt.xlabel("Czas")
    plt.ylabel("Energia [kWh]")
    plt.title(f"Interpolacja vs dane oryginalne – instalacja: {sn}")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

def plot_all_power_series(df_all, sn, date_str):

    df_all = df_all.select_dtypes(include=[np.number])
    df_all = df_all.dropna(how='all')
    if df_all.empty:
        print("⚠️ Brak danych do wykresu.")
        return

    # Wykres
    plt.figure(figsize=(12, 5))
    for col in df_all.columns:
        plt.plot(df_all.index, df_all[col], label=col, marker = 'o', markersize = 1,linewidth=1)

    plt.xlabel("Czas")
    plt.ylabel("Moc [kW]")
    plt.title("Porównanie instalacji PV "+sn+" w dniu " + date_str)
    plt.legend(title="SN")
    plt.grid(True)
    ax = plt.gca()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    try:
        plt.tight_layout()
    except Exception as e:
        print("⚠️ tight_layout() error:", e)

    plt.show()
def plot_all_with_median(df_all,date_str,save_path=None):
    plt.figure(figsize=(12, 5))
    # Rysuj wszystkie kolumny oprócz mediany
    for col in df_all.columns:
        if col != 'median_kW':
            plt.plot(df_all.index, df_all[col], label=col, linewidth=1, alpha=0.7)

    # Mediana — na końcu, grubszą linią
    plt.plot(df_all.index, df_all['median_kW'],
             label='Mediana', color='black', linewidth=2.5, linestyle='--')

    plt.xlabel("Czas")
    plt.ylabel("Moc [kW]")
    plt.title("Mediana instalacji PV w dniu "+date_str)
    plt.legend()
    plt.grid(True)
    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))       # 🕒 znacznik co 1h
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))      # 🕒 format HH:MM
    ax.grid(True, axis='x', which='major', linestyle='--', alpha=0.5)
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=300)
        plt.close()
        print(f"Wykres zapisano do {save_path}")
    else:
        plt.show()
def analyze_day(engine,df_coeff,date_str):
    global db_data, df_interpolated
    db_data = load_db_data(engine, date_str)
    print("Dane z bazy danych dla dnia:", date_str)
    print("Liczba wierszy:", len(db_data))
    # print (db_data.head(10))
    df_interpolated = interpolate_energy_linear_grid(db_data, df_coeff, date_str,timeprobe="3min")
    print("Interpolacja energii dla dnia:", date_str)
    print("Liczba wierszy po interpolacji:", len(df_interpolated))
    # print (df_interpolated)
    db_data.to_excel("/media/ramdisk/db_data.xlsx", index=False)
    print("Dane z bazy danych zostały zapisane do pliku db_data.xlsx")
    df_interpolated.to_excel("/media/ramdisk/interpolated_energy.xlsx", index=False)
    print("Dane zostały zapisane do pliku interpolated_energy.xlsx")

def main():
    # ==========================
    # db_data = pd.read_excel("/media/ramdisk/db_data_manual_sample.xlsx")
    # df_interpolated=interpolate_energy_linear_grid(db_data, timeprobe="1min")
    # exit(1)
    # ==========================
    engine = connect_db()
    # print (df_coeff.head(10))

    df_coeff = analyze_instalations(engine)
    today = datetime.now().strftime("%Y-%m-%d")
    start_time = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
    days = pd.date_range(start_time, today)
    reference_cases = ["2025-04-25", "2025-05-14", "2025-05-22", "2025-05-25", "2025-06-05", "2025-07-02", "2025-07-05",
                       "2025-07-06", "2025-07-10", "2025-07-11", "2025-07-14", "2025-07-11"]
    #reference_cases = ["2025-07-06", "2025-07-02"]

    #days = [datetime.strptime(d, "%Y-%m-%d") for d in reference_cases] # odkomentuj dla testów wybranych dni
    for day in reversed(days):  # od końca
        date_str = day.strftime("%Y-%m-%d")
        analyze_day(engine,df_coeff,date_str)

if __name__== "__main__":
    main()




