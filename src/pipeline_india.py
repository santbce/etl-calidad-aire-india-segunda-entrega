#aqui definimos las funciones para extraer, transformar, validar y cargar los datos
from pathlib import Path
from datetime import datetime, timezone
import os
#aqui usamos un backend que guarda los graficos sin depender del kernel de colab
#aqui guardamos los graficos como imagenes para mostrarlos en el notebook
os.environ['MPLBACKEND'] = 'Agg'
import json
import sys
import hashlib
import sqlite3
import time
from contextlib import closing
#aqui importamos las librerias para manejar tablas y consultar las fuentes
import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

ROOT = Path(os.environ.get('INDIA_PROJECT_ROOT', Path(__file__).resolve().parents[1]))
FIRST_DB = ROOT / 'primera_entrega/database/air_quality.db'
CITY_DB = ROOT / 'data/cities.sqlite'
START, END = '2015-01-01', '2024-12-31'
CITIES = ['Bangalore', 'Chennai', 'Delhi', 'Kolkata', 'Mumbai']
MEASURES = ['pm25', 'pm10', 'no_value', 'no2', 'nox', 'nh3', 'co', 'so2', 'o3', 'aqi']
WEATHER = ['temperature_2m_mean', 'precipitation_sum', 'wind_speed_10m_max']
API = 'https://archive-api.open-meteo.com/v1/archive'
NROWS = 18265

def utcnow():
    return datetime.now(timezone.utc).isoformat()

def save_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str), encoding='utf-8')

def workspace(run_id):
    #aqui limpiamos el nombre de la ejecucion antes de crear su carpeta
    safe = ''.join(c if c.isalnum() or c in '-_' else '_' for c in run_id)
    path = ROOT / 'data/runs' / safe
    path.mkdir(parents=True, exist_ok=True)
    return path

def readonly_sqlite(path):
    #aqui abrimos la base original solo para leerla
    return sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True)

def session():
    s = requests.Session()
    s.headers.update({'User-Agent': 'IndiaAirQualityAcademicProject/1.0', 'Accept-Language': 'en'})
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=['GET'], respect_retry_after_header=True)
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s
def build_city_dataset():
    #aqui sacamos las coordenadas de wikipedia y las guardamos en sqlite
    records = []
    raw = ROOT / 'data/raw/cities'
    raw.mkdir(parents=True, exist_ok=True)
    with closing(readonly_sqlite(FIRST_DB)) as con:
        ids = dict(con.execute('SELECT city_name, city_id FROM dim_city').fetchall())
    with session() as s:
        for city in CITIES:
            url = 'https://en.wikipedia.org/wiki/' + city
            response = s.get(url, timeout=(10, 60))
            response.raise_for_status()
            (raw / (city + '.html')).write_text(response.text, encoding='utf-8')
            soup = BeautifulSoup(response.text, 'html.parser')
            #aqui tomamos la coordenada principal de la ciudad
            geo = soup.select_one('#coordinates .geo') or soup.select_one('table.infobox .geo')
            if geo is None:
                raise ValueError(f'{city}: no aparece la coordenada principal; revisar HTML')
            values = geo.get_text(' ', strip=True).split(';')
            if len(values) != 2:
                raise ValueError(f'{city}: formato inesperado de coordenadas')
            latitude, longitude = (float(v.strip()) for v in values)
            if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
                raise ValueError(f'{city}: coordenadas inválidas')
            records.append({'city_id': ids[city], 'city_name': city, 'latitude': latitude,
                            'longitude': longitude, 'source_url': url, 'extracted_at_utc': utcnow()})
            time.sleep(1)
    df = pd.DataFrame(records)
    CITY_DB.parent.mkdir(parents=True, exist_ok=True)
    #aqui reemplazamos el archivo solo cuando termina la extraccion
    tmp = CITY_DB.with_suffix('.pending.sqlite')
    with closing(sqlite3.connect(tmp)) as con, con:
        con.execute('DROP TABLE IF EXISTS city_coordinates')
        con.execute('''CREATE TABLE city_coordinates (
            city_id INTEGER PRIMARY KEY, city_name TEXT NOT NULL UNIQUE,
            latitude REAL NOT NULL CHECK(latitude BETWEEN -90 AND 90),
            longitude REAL NOT NULL CHECK(longitude BETWEEN -180 AND 180),
            source_url TEXT NOT NULL, extracted_at_utc TEXT NOT NULL)''')
        df.to_sql('city_coordinates', con, if_exists='append', index=False)
    tmp.replace(CITY_DB)
    df.to_csv(ROOT / 'data/raw/city_coordinates.csv', index=False)
    return df
def prepare_run(run_id):
    path = workspace(run_id)
    if not FIRST_DB.exists():
        raise FileNotFoundError('Primero carga el ZIP original de India')
    save_json(path / 'run.json', {'run_id': run_id, 'started_at_utc': utcnow(),
              'period': [START, END], 'timezone_assumption': 'Asia/Kolkata',
              'source_db_sha256': hashlib.sha256(FIRST_DB.read_bytes()).hexdigest()})
    print('Ejecución preparada:', run_id)

def extract_air(run_id):
    path = workspace(run_id)
    sql = '''SELECT f.measurement_id, c.city_name, f.city_id,
        date(d.full_datetime) AS date, a.aqi_bucket,
        f.pm25, f.pm10, f.no_value, f.no2, f.nox, f.nh3, f.co, f.so2, f.o3, f.aqi
        FROM fact_air_quality f JOIN dim_city c ON f.city_id=c.city_id
        JOIN dim_date d ON f.date_id=d.date_id JOIN dim_aqi a ON f.aqi_id=a.aqi_id
        WHERE date(d.full_datetime) BETWEEN ? AND ? ORDER BY f.measurement_id'''
    with closing(readonly_sqlite(FIRST_DB)) as con:
        df = pd.read_sql_query(sql, con, params=(START, END))
    check_keys(df, ['city_id', 'date'], NROWS)
    df.to_csv(path / 'air.csv', index=False)
    save_json(path / 'air_provenance.json', {'sql': sql, 'rows': len(df), 'extracted_at_utc': utcnow()})
    print('Fuente 1 desde SQLite:', len(df))

def extract_cities(run_id):
    #aqui actualizamos las coordenadas y luego las extraemos desde la base de datos
    build_city_dataset()
    path = workspace(run_id)
    with closing(readonly_sqlite(CITY_DB)) as con:
        df = pd.read_sql_query('SELECT city_id, city_name, latitude, longitude, source_url, extracted_at_utc FROM city_coordinates', con)
    check_keys(df, ['city_id'], 5)
    if set(df.city_name) != set(CITIES):
        raise ValueError('El dataset de coordenadas no contiene las cinco ciudades')
    df.to_csv(path / 'cities.csv', index=False)
    print('Fuente 2 desde SQLite, procedencia Wikipedia:', len(df))
def parse_weather(payload, city_id, params):
    daily, units = payload.get('daily', {}), payload.get('daily_units', {})
    expected_units = {'time': 'iso8601', 'temperature_2m_mean': '°C',
                      'precipitation_sum': 'mm', 'wind_speed_10m_max': 'm/s'}
    for name, unit in expected_units.items():
        if units.get(name) != unit:
            raise ValueError(f'Unidad API inesperada: {name}: {units.get(name)}')
    days = pd.date_range(params['start_date'], params['end_date']).strftime('%Y-%m-%d').tolist()
    if daily.get('time') != days or any(len(daily.get(c, [])) != len(days) for c in WEATHER):
        raise ValueError('Respuesta API incompleta o estructura diaria diferente')
    df = pd.DataFrame({k: daily[k] for k in ['time'] + WEATHER}).rename(columns={'time': 'date'})
    df['city_id'] = city_id
    df['grid_latitude'] = payload['latitude']
    df['grid_longitude'] = payload['longitude']
    return df

def extract_weather(run_id, offline=False):
    path = workspace(run_id)
    cities = pd.read_csv(path / 'cities.csv')
    cache = ROOT / 'data/raw/weather'
    cache.mkdir(parents=True, exist_ok=True)
    frames, provenance = [], []
    with session() as s:
        for city in cities.to_dict('records'):
            for year in range(2015, 2025):
                params = {'latitude': city['latitude'], 'longitude': city['longitude'],
                          'start_date': f'{year}-01-01', 'end_date': f'{year}-12-31',
                          'daily': ','.join(WEATHER), 'models': 'era5', 'timezone': 'Asia/Kolkata',
                          'temperature_unit': 'celsius', 'precipitation_unit': 'mm', 'wind_speed_unit': 'ms'}
                key = hashlib.sha256(json.dumps(params, sort_keys=True).encode()).hexdigest()[:16]
                filename = cache / f"{city['city_name']}_{year}_{key}.json"
                #aqui reutilizamos las respuestas por 24 horas para no repetir las consultas
                if filename.exists() and (offline or time.time()-filename.stat().st_mtime < 86400):
                    stored = json.loads(filename.read_text(encoding='utf-8'))
                    mode = 'cache'
                elif offline:
                    raise FileNotFoundError('No hay caché para ' + str(filename))
                else:
                    r = s.get(API, params=params, timeout=(10, 60))
                    r.raise_for_status()
                    stored = {'params': params, 'url': r.url, 'extracted_at_utc': utcnow(), 'payload': r.json()}
                    parse_weather(stored['payload'], city['city_id'], params)
                    save_json(filename, stored)
                    mode = 'http'
                    time.sleep(1)
                frames.append(parse_weather(stored['payload'], city['city_id'], params))
                provenance.append({'file': filename.name, 'mode': mode, 'params': params,
                                   'extracted_at_utc': stored['extracted_at_utc'], 'url': stored['url']})
                print(city['city_name'], year, mode, flush=True)
    df = pd.concat(frames, ignore_index=True)
    check_keys(df, ['city_id', 'date'], NROWS)
    df.to_csv(path / 'weather.csv', index=False)
    save_json(path / 'weather_provenance.json', provenance)
def check_keys(df, keys, count):
    if len(df) != count or df[keys].isna().any().any() or df.duplicated(keys).any():
        raise ValueError(f'Conteo, nulos o duplicados inválidos en {keys}; filas={len(df)}')

def profile(df, keys):
    numeric = df.select_dtypes(include='number')
    q1, q3 = numeric.quantile(.25), numeric.quantile(.75)
    outliers = ((numeric < q1-1.5*(q3-q1)) | (numeric > q3+1.5*(q3-q1))).sum()
    return {'rows': len(df), 'columns': list(df), 'types': df.dtypes.astype(str).to_dict(),
            'missing': df.isna().sum().to_dict(), 'exact_duplicates': int(df.duplicated().sum()),
            'key_duplicates': int(df.duplicated(keys).sum()), 'summary': numeric.describe().to_dict(),
            'iqr_outliers': outliers.to_dict(), 'categories': {
                c: df[c].value_counts().to_dict() for c in ['city_name', 'aqi_bucket'] if c in df}}

def eda_sources(run_id):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    path = workspace(run_id)
    result = {}
    for name, keys in [('air', ['city_id', 'date']), ('cities', ['city_id']), ('weather', ['city_id', 'date'])]:
        df = pd.read_csv(path / f'{name}.csv')
        result[name] = profile(df, keys)
        columns = MEASURES if name == 'air' else WEATHER if name == 'weather' else ['latitude','longitude']
        df[columns].hist(bins=30, figsize=(12,9))
        plt.suptitle('EDA de fuente: ' + name)
        plt.tight_layout()
        plt.savefig(path / f'eda_{name}.png', dpi=130)
        plt.close('all')
    save_json(path / 'eda_sources.json', result)
    print('EDA de las tres fuentes guardado; no elimina outliers ni imputa clima')

def transform_merge(run_id):
    path = workspace(run_id)
    #aqui leemos las tres fuentes de esta ejecucion
    air, cities, weather = (pd.read_csv(path / f'{n}.csv') for n in ['air', 'cities', 'weather'])
    for df in [air, weather]:
        df['date'] = pd.to_datetime(df['date'], errors='raise').dt.strftime('%Y-%m-%d')
        df['city_id'] = pd.to_numeric(df['city_id'], errors='raise').astype('int64')
    for df, columns in [(air, MEASURES), (weather, WEATHER)]:
        for c in columns:
            df[c] = pd.to_numeric(df[c], errors='raise')
    #aqui usamos los identificadores de ciudad que tenia la primera entrega
    enriched = air.merge(cities.drop(columns='city_name'), on='city_id', how='left', validate='many_to_one')
    enriched = enriched.merge(weather, on=['city_id', 'date'], how='left', validate='one_to_one', indicator=True)
    if not enriched['_merge'].eq('both').all():
        save_json(path / 'unmatched.json', enriched.loc[enriched['_merge']!='both', ['city_id','date']].to_dict('records'))
        raise ValueError('Hay mediciones sin clima; se bloquea el lote')
    enriched = enriched.drop(columns='_merge')
    check_keys(enriched, ['city_id', 'date'], NROWS)
    enriched['weather_model'] = 'era5'
    enriched['run_id'] = run_id
    enriched['merged_at_utc'] = utcnow()
    enriched.to_csv(path / 'enriched.csv', index=False)
    print('Merge completo:', enriched.shape)
def validate_dataframe(df, report_path):
    #aqui creamos la suite y ejecutamos el checkpoint de calidad
    import great_expectations as gx
    import great_expectations.expectations as gxe
    #aqui creamos el contexto y las reglas de calidad
    context = gx.get_context(mode='ephemeral')
    suite = gx.ExpectationSuite(name='contrato_calidad_aire_india')
    suite.add_expectation(gxe.ExpectTableRowCountToEqual(value=NROWS))
    for c in ['measurement_id','city_id','city_name','date','aqi_bucket','latitude','longitude','grid_latitude','grid_longitude'] + MEASURES + WEATHER:
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=c))
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column='measurement_id'))
    suite.add_expectation(gxe.ExpectCompoundColumnsToBeUnique(column_list=['city_id','date']))
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column='city_name', value_set=CITIES))
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column='aqi_bucket', value_set=['Good','Satisfactory','Moderate','Poor','Very Poor','Severe']))
    for c in MEASURES + ['precipitation_sum', 'wind_speed_10m_max']:
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column=c, min_value=0, max_value=sys.float_info.max))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column='latitude', min_value=-90, max_value=90))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column='longitude', min_value=-180, max_value=180))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column='grid_latitude', min_value=-90, max_value=90))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column='grid_longitude', min_value=-180, max_value=180))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column='temperature_2m_mean', min_value=-90, max_value=60))
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column='date', value_set=pd.date_range(START, END).strftime('%Y-%m-%d').tolist()))
    suite = context.suites.add(suite)
    source = context.data_sources.add_pandas(name='fuente_airflow')
    asset = source.add_dataframe_asset(name='asset_aire_clima')
    batch = asset.add_batch_definition_whole_dataframe('lote_historico')
    validation = context.validation_definitions.add(gx.ValidationDefinition(data=batch, suite=suite, name='auditoria_india'))
    checkpoint = context.checkpoints.add(gx.Checkpoint(name='control_airflow', validation_definitions=[validation]))
    #aqui ejecutamos las reglas sobre el lote integrado
    result = checkpoint.run(batch_parameters={'dataframe': df})
    details = {'success': bool(result.success),
               'validation_results': [r.to_json_dict() for r in result.run_results.values()]}
    #aqui comprobamos que esten las cinco ciudades y todos los dias esperados
    expected = {(c, d) for c in CITIES for d in pd.date_range(START, END).strftime('%Y-%m-%d')}
    coverage = set(zip(df.city_name, df.date)) == expected
    success = bool(result.success and coverage)
    save_json(report_path, {'success': success, 'coverage_exact': coverage, 'gx_version': gx.__version__,
                           'validated_at_utc': utcnow(), 'checkpoint_result': details})
    save_json(Path(report_path).with_name('expectation_suite.json'), suite.to_json_dict())
    print('Great Expectations:', success)
    return success

def validate_enriched(run_id):
    path = workspace(run_id)
    filename = path / 'enriched.csv'
    save_json(path / 'approval.json', {'run_id':run_id, 'success':False, 'sha256':None})
    df = pd.read_csv(filename)
    if not validate_dataframe(df, path / 'validation.json'):
        raise ValueError('El contrato falló. La carga al warehouse queda bloqueada; revisar validation.json')
    save_json(path / 'approval.json', {'run_id': run_id, 'success': True,
               'sha256': hashlib.sha256(filename.read_bytes()).hexdigest()})

def approved_dataframe(run_id):
    path = workspace(run_id)
    filename = path / 'enriched.csv'
    approval = json.loads((path / 'approval.json').read_text(encoding='utf-8'))
    validation = json.loads((path / 'validation.json').read_text(encoding='utf-8'))
    if not validation['success'] or not approval['success'] or approval['run_id'] != run_id or approval['sha256'] != hashlib.sha256(filename.read_bytes()).hexdigest():
        raise ValueError('No existe aprobación válida para este lote; carga bloqueada')
    return pd.read_csv(filename)
def load_warehouse(run_id):
    import psycopg
    path = workspace(run_id)
    df = approved_dataframe(run_id)
    with psycopg.connect(os.environ['INDIA_DATABASE_URL']) as con:
        with con.cursor() as cur:
            cur.execute((ROOT / 'sql/warehouse.sql').read_text(encoding='utf-8'))
            #aqui hacemos la carga en una transaccion para conservar los datos si falla
            for table in ['fact_air_quality','dim_city','dim_date','dim_aqi']:
                cur.execute('DELETE FROM india_dw.' + table)
            cities = df[['city_id','city_name','latitude','longitude','source_url']].drop_duplicates()
            cur.executemany('INSERT INTO india_dw.dim_city VALUES (%s,%s,%s,%s,%s)', list(cities.itertuples(index=False, name=None)))
            days = pd.to_datetime(df.date.drop_duplicates().sort_values())
            dates = [(d.date(), d.year, d.month, d.day, d.quarter, d.day_name()) for d in days]
            cur.executemany('INSERT INTO india_dw.dim_date VALUES (%s,%s,%s,%s,%s,%s)', dates)
            buckets = sorted(df.aqi_bucket.unique())
            ids = {v:i+1 for i,v in enumerate(buckets)}
            cur.executemany('INSERT INTO india_dw.dim_aqi VALUES (%s,%s)', [(i,v) for v,i in ids.items()])
            facts = df.copy()
            facts['aqi_id'] = facts.aqi_bucket.map(ids)
            cols = ['measurement_id','city_id','date','aqi_id'] + MEASURES + WEATHER + ['grid_latitude','grid_longitude','weather_model','run_id']
            cur.executemany('INSERT INTO india_dw.fact_air_quality VALUES ('+','.join(['%s']*len(cols))+')', list(facts[cols].itertuples(index=False, name=None)))
            cur.execute('SELECT COUNT(*) FROM india_dw.fact_air_quality')
            if cur.fetchone()[0] != NROWS:
                raise ValueError('Reconciliación de warehouse fallida')
            validation = json.loads((path / 'validation.json').read_text(encoding='utf-8'))
            cur.execute('INSERT INTO india_dw.etl_runs(run_id,loaded_at,rows,quality_result) VALUES (%s,%s,%s,%s) ON CONFLICT(run_id) DO UPDATE SET loaded_at=excluded.loaded_at,rows=excluded.rows,quality_result=excluded.quality_result',
                        (run_id, utcnow(), NROWS, json.dumps(validation, default=str)))
    save_json(path / 'load.json', {'database':'PostgreSQL','rows':NROWS,'run_id':run_id,'loaded_at_utc':utcnow()})
    print('PostgreSQL: carga validada completada')

def visualize_warehouse(run_id):
    import psycopg
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import seaborn as sns
    path = workspace(run_id)
    output = ROOT / 'reports' / path.name
    output.mkdir(parents=True, exist_ok=True)
    with psycopg.connect(os.environ['INDIA_DATABASE_URL']) as con:
        with con.cursor() as cur:
            cur.execute('SELECT * FROM india_dw.air_quality_enriched WHERE run_id=%s', (run_id,))
            df = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])
    if len(df) != NROWS:
        raise ValueError('El warehouse no contiene este lote completo')
    monthly = df.assign(month=pd.to_datetime(df.date).dt.to_period('M').astype(str)).groupby(['city_name','month'])[['aqi']+WEATHER].mean().reset_index()
    correlations = df.groupby('city_name')[['aqi','pm25']+WEATHER].corr().reset_index()
    correlations.to_csv(output / 'correlations_by_city.csv', index=False)
    monthly.to_csv(output / 'monthly.csv', index=False)
    sns.set_theme(style='whitegrid')
    fig, ax = plt.subplots(figsize=(10,5))
    sns.barplot(data=df.groupby('city_name',as_index=False).aqi.mean(),x='city_name',y='aqi', ax=ax)
    ax.set(title='AQI promedio por ciudad 2015–2024', xlabel='Ciudad', ylabel='AQI')
    fig.tight_layout();fig.savefig(output/'01_aqi_ciudad.png',dpi=160);plt.close(fig)
    fig, axes = plt.subplots(3,1,figsize=(11,11))
    for c, ax in zip(WEATHER, axes):
        sns.histplot(data=df,x=c,bins=35,ax=ax)
        ax.set_title(c)
    fig.tight_layout();fig.savefig(output/'02_distribuciones_clima.png',dpi=160);plt.close(fig)
    fig, ax = plt.subplots(figsize=(12,6))
    annual = df.assign(year=pd.to_datetime(df.date).dt.year).groupby(['city_name','year'],as_index=False).aqi.mean()
    sns.lineplot(data=annual,x='year',y='aqi',hue='city_name',marker='o',ax=ax)
    ax.set_title('Evolución anual del AQI');fig.tight_layout();fig.savefig(output/'03_aqi_anual.png',dpi=160);plt.close(fig)
    fig, axes = plt.subplots(1,5,figsize=(22,5))
    for city, ax in zip(CITIES,axes):
        m=df[df.city_name==city][['aqi','pm25']+WEATHER].corr()
        sns.heatmap(m[['aqi']],annot=True,fmt='.2f',vmin=-1,vmax=1,cmap='coolwarm',ax=ax)
        ax.set_title(city)
    fig.tight_layout();fig.savefig(output/'04_correlaciones_ciudad.png',dpi=160);plt.close(fig)
    #aqui usamos los datos del warehouse que ya pasaron la validacion
    mean = float(df.aqi.mean())
    counts = df.aqi_bucket.value_counts().to_dict()
    findings = {'rows':len(df),'aqi_mean':mean,'aqi_categories':counts,
                'aqi_weather_correlations_by_city': {city:df[df.city_name==city][['aqi']+WEATHER].corr()['aqi'].drop('aqi').to_dict() for city in CITIES}}
    save_json(output / 'findings.json', findings)
    html = '<!doctype html><html lang="es"><meta charset="utf-8"><title>Calidad del aire en India</title><style>body{font:16px system-ui;max-width:1100px;margin:30px auto;padding:20px;line-height:1.6}img{max-width:100%}table{border-collapse:collapse}td,th{padding:7px;border:1px solid #ddd}</style><h1>Calidad del aire y clima en India</h1>'
    html += f'<p>2015–2024 · {len(df):,} mediciones validadas · AQI promedio {mean:.2f}</p>'
    html += '<p>Datos originales de Kaggle, coordenadas de Wikipedia y clima ERA5 de Open-Meteo. Las asociaciones no prueban causalidad. Las coordenadas ubican ciudades, no estaciones de medición.</p>'
    for image in sorted(output.glob('*.png')):
        html += f'<img src="{image.name}" alt="{image.stem}">'
    html += monthly.head(15).to_html(index=False)
    (output/'dashboard.html').write_text(html,encoding='utf-8')
    print('Dashboard creado:',output/'dashboard.html')
