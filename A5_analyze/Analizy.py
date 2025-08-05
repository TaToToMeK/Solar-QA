from config.db import connect_db
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

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
    date_start = datetime.strptime(date_str, "%Y-%m-%d")
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
        return df_resampled  # Nie ma różnicy do obliczenia
    kW_values = [0]  # Pierwsza wartość różnicy jest zerowa
    for i in range(1, len(resampled)):
        dY = max(0,resampled[i] - resampled[i - 1])  # obsługa case typu sn#069 2025-07-05 21:10
        kW = dY / timeprobe_to_hours(timeprobe)  # Przekształcamy różnicę na kW
        norm_kW=kW/coeff
        kW_values.append(norm_kW)
    return kW_values

def interpolate_energy_linear_grid(df: pd.DataFrame,df_coeff,date_str, timeprobe: str ):
    print("\n=================\nInterpolacja energii na siatce czasowej:", timeprobe)
    df = df.copy()
    df['system_time'] = pd.to_datetime(df['system_time'])
    df = df.sort_values(['sn', 'system_time'])
    result = []
    start_raw = df['system_time'].min()
    end_raw = df['system_time'].max()
    start = start_raw.floor('H')  # w dół do pełnej godziny
    end = end_raw.ceil('10min')  # w górę
    # Tworzenie równomiernej siatki co timeprobe
    full_index = pd.date_range(start=start, end=end, freq=timeprobe)
    #full_index = full_index[full_index >= start_raw]
    print(f"start: {start}, end: {end}, len: {len(full_index)}")
    #print (full_index)
    df_all_sn_kW=pd.DataFrame(index=full_index)

    # Iteracja po grupach sn
    for sn, group in df.groupby('sn'):
        print(f"\nPrzetwarzanie sn: {sn}, liczba punktów: {len(group)}")
        # if sn != 'SS3ES125P38069':      continue #  odkomentuj do testów
        #print (group)
        sn_resampled=resample_sn (group, full_index, start_raw, end_raw)
        coeff= df_coeff[df_coeff['sn'] == sn]['coefficient'].values[0] if not df_coeff[df_coeff['sn'] == sn].empty else 1
        print(f"SN: {sn}, Coefficient: {coeff}")
        sn_kW = normalize_kW(sn_resampled,timeprobe,coeff)
        #sn_kW=sn_resampled #odkomentuj do testów
        df_resampled = pd.DataFrame({'tick': full_index, 'iterpolated_values': sn_kW})
        df_all_sn_kW[sn]= sn_kW
        #plot_interpolation_vs_original(df_resampled, group)
    df_all_sn_kW['median_kW'] = df_all_sn_kW.median(axis=1, skipna=True)
    #plot_all_power_series(df_all_sn_kW,date_str)
    plot_all_with_median(df_all_sn_kW, date_str, save_path=f"/media/ramdisk/median_{date_str}.png")
    plot_all_with_median(df_all_sn_kW, date_str)

    return df_all_sn_kW #all

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
def plot_all_power_series(df_all,date_str):
    # df_all: DataFrame z indeksem czasowym i kolumnami sn1, sn2, ...
    plt.figure(figsize=(12, 5))
    df_all.plot(ax=plt.gca(), linewidth=2)
    plt.xlabel("Czas")
    plt.ylabel("Moc [kW]")
    plt.title("Porównanie wszystkich instalacji PV w dniu "+date_str)
    plt.grid(True)
    plt.legend(title="SN")
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.tight_layout()
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
def analyze_day(date_str):
    global db_data, df_interpolated
    db_data = load_db_data(engine, date_str)
    print("Dane z bazy danych dla dnia:", date_str)
    print("Liczba wierszy:", len(db_data))
    # print (db_data.head(10))
    df_interpolated = interpolate_energy_linear_grid(db_data, df_coeff, date_str,timeprobe="4min")
    print("Interpolacja energii dla dnia:", date_str)
    print("Liczba wierszy po interpolacji:", len(df_interpolated))
    # print (df_interpolated)
    db_data.to_excel("/media/ramdisk/db_data.xlsx", index=False)
    print("Dane z bazy danych zostały zapisane do pliku db_data.xlsx")
    df_interpolated.to_excel("/media/ramdisk/interpolated_energy.xlsx", index=False)
    print("Dane zostały zapisane do pliku interpolated_energy.xlsx")

#==========================
#db_data = pd.read_excel("/media/ramdisk/db_data_manual_sample.xlsx")
#df_interpolated=interpolate_energy_linear_grid(db_data, timeprobe="1min")
#exit(1)
#==========================
engine = connect_db()
#print (df_coeff.head(10))

df_coeff= analyze_instalations(engine)
today= datetime.now().strftime("%Y-%m-%d")
start_time= (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
days=pd.date_range(start_time, today)
reference_cases = ["2025-04-25","2025-05-14","2025-05-22","2025-05-25","2025-06-05","2025-07-02", "2025-07-05", "2025-07-06", "2025-07-10", "2025-07-11", "2025-07-14", "2025-07-11"]
#days = [datetime.strptime(d, "%Y-%m-%d") for d in reference_cases] # odkomentuj dla testów wybranych dni
for day in reversed(days):  # od końca
    date_str = day.strftime("%Y-%m-%d")
    analyze_day(date_str)



