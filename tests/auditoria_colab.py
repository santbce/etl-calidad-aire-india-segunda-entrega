#aqui comprobamos el pipeline con datos validos y casos de error
import copy
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import zipfile
from pathlib import Path

import pandas as pd
import psycopg

ROOT = Path(os.environ['INDIA_PROJECT_ROOT'])
sys.path.insert(0, str(ROOT / 'src'))
import pipeline_india as p

results = []


def check(name, action):
    try:
        action()
        results.append({'test': name, 'passed': True})
        print('aprobado:', name, flush=True)
    except Exception as error:
        results.append({'test': name, 'passed': False, 'error_type': type(error).__name__})
        print('fallo:', name, type(error).__name__, flush=True)


def require(condition):
    if not condition:
        raise AssertionError('la comprobacion no paso')


def rejected(action, kinds=(ValueError,)):
    try:
        action()
    except kinds:
        return
    raise AssertionError('el caso incorrecto no fue rechazado')


def database_snapshot():
    with psycopg.connect(os.environ['INDIA_DATABASE_URL']) as con:
        with con.cursor() as cur:
            cur.execute('SELECT * FROM india_dw.fact_air_quality ORDER BY measurement_id')
            rows = cur.fetchall()
    return hashlib.sha256(repr(rows).encode()).hexdigest(), len(rows)


with psycopg.connect(os.environ['INDIA_DATABASE_URL']) as con:
    with con.cursor() as cur:
        cur.execute('SELECT DISTINCT run_id FROM india_dw.fact_air_quality')
        runs = [row[0] for row in cur.fetchall()]
        require(len(runs) == 1)
run_id = runs[0]
path = p.workspace(run_id)
valid = p.approved_dataframe(run_id)
first_hash = hashlib.sha256(p.FIRST_DB.read_bytes()).hexdigest()
before = database_snapshot()

check('lote aprobado de 18265 filas', lambda: require(len(valid) == p.NROWS))
check('claves ciudad y fecha sin duplicados', lambda: require(not valid.duplicated(['city_id', 'date']).any()))
check('cinco ciudades con 3653 dias cada una', lambda: require(set(valid.city_name) == set(p.CITIES) and valid.groupby('city_name').size().eq(3653).all()))


def original_unchanged():
    source = Path(os.environ['INDIA_SOURCE_ZIP'])
    with zipfile.ZipFile(source) as archive:
        original = archive.read('etl-calidad-aire-india-main/database/air_quality.db')
    require(hashlib.sha256(original).hexdigest() == first_hash)


check('base de la primera entrega intacta', original_unchanged)
stored_file = next((ROOT / 'data/raw/weather').glob('*.json'))
stored = json.loads(stored_file.read_text(encoding='utf-8'))
payload, params = stored['payload'], stored['params']


def cached_api_contract():
    files = list((ROOT / 'data/raw/weather').glob('*.json'))
    require(len(files) >= 50)
    for file in files:
        data = json.loads(file.read_text(encoding='utf-8'))
        require('url' in data and 'extracted_at_utc' in data)
        p.parse_weather(data['payload'], 1, data['params'])


check('respuestas de la api con estructura y procedencia', cached_api_contract)


def bad_units():
    data = copy.deepcopy(payload)
    data['daily_units']['wind_speed_10m_max'] = 'km/h'
    rejected(lambda: p.parse_weather(data, 1, params))


def incomplete_api():
    data = copy.deepcopy(payload)
    data['daily']['precipitation_sum'].pop()
    rejected(lambda: p.parse_weather(data, 1, params))


def reordered_days():
    data = copy.deepcopy(payload)
    data['daily']['time'][0], data['daily']['time'][1] = data['daily']['time'][1], data['daily']['time'][0]
    rejected(lambda: p.parse_weather(data, 1, params))


check('rechazo de unidades distintas en la api', bad_units)
check('rechazo de respuesta incompleta de la api', incomplete_api)
check('rechazo de dias desordenados de la api', reordered_days)


def invalid_merge(name, change):
    target = p.workspace('auditoria_merge_' + name)
    for source in ['air', 'cities', 'weather']:
        shutil.copy2(path / (source + '.csv'), target / (source + '.csv'))
    change(target)
    rejected(lambda: p.transform_merge('auditoria_merge_' + name), (ValueError, pd.errors.MergeError))


def missing_weather(target):
    file = target / 'weather.csv'
    pd.read_csv(file).iloc[1:].to_csv(file, index=False)


def duplicate_weather(target):
    file = target / 'weather.csv'
    table = pd.read_csv(file)
    pd.concat([table, table.iloc[:1]], ignore_index=True).to_csv(file, index=False)


def malformed_date(target):
    file = target / 'air.csv'
    table = pd.read_csv(file)
    table.loc[0, 'date'] = 'fecha_incorrecta'
    table.to_csv(file, index=False)


check('bloqueo de mediciones sin clima', lambda: invalid_merge('sin_clima', missing_weather))
check('bloqueo de union que duplicaria mediciones', lambda: invalid_merge('duplicado', duplicate_weather))
check('bloqueo de fecha que no se puede convertir', lambda: invalid_merge('fecha', malformed_date))


def quality_case(name, change):
    data = valid.copy()
    change(data)
    case_id = 'auditoria_calidad_' + name
    target = p.workspace(case_id)
    data.to_csv(target / 'enriched.csv', index=False)
    rejected(lambda: p.validate_enriched(case_id))
    rejected(lambda: p.load_warehouse(case_id), (ValueError, FileNotFoundError))


cases = [
    ('aqi_infinito', lambda d: d.loc.__setitem__((0, 'aqi'), float('inf'))),
    ('coordenada_api_nula', lambda d: d.loc.__setitem__((0, 'grid_latitude'), float('nan'))),
    ('aqi_nulo', lambda d: d.__setitem__('aqi', d.aqi.mask(d.index == 0))),
    ('id_duplicado', lambda d: d.loc.__setitem__((1, 'measurement_id'), d.loc[0, 'measurement_id'])),
    ('ciudad_fecha_duplicada', lambda d: d.loc.__setitem__((1, ['city_id', 'city_name', 'date']), d.loc[0, ['city_id', 'city_name', 'date']].values)),
    ('ciudad_no_permitida', lambda d: d.loc.__setitem__((0, 'city_name'), 'ciudad_incorrecta')),
    ('categoria_no_permitida', lambda d: d.loc.__setitem__((0, 'aqi_bucket'), 'categoria_incorrecta')),
    ('lluvia_negativa', lambda d: d.loc.__setitem__((0, 'precipitation_sum'), -1)),
    ('contaminante_negativo', lambda d: d.loc.__setitem__((0, 'pm25'), -1)),
    ('temperatura_imposible', lambda d: d.loc.__setitem__((0, 'temperature_2m_mean'), 100)),
    ('fecha_fuera_del_periodo', lambda d: d.loc.__setitem__((0, 'date'), '2030-01-01')),
]
for name, change in cases:
    check('gx y carga rechazan ' + name, lambda name=name, change=change: quality_case(name, change))
check('warehouse intacto despues de los casos invalidos', lambda: require(database_snapshot() == before))


def copied_approval(case_id):
    target = p.workspace(case_id)
    for name in ['enriched.csv', 'approval.json', 'validation.json']:
        shutil.copy2(path / name, target / name)
    approval = json.loads((target / 'approval.json').read_text(encoding='utf-8'))
    approval['run_id'] = case_id
    p.save_json(target / 'approval.json', approval)
    return target


def changed_csv():
    case_id = 'auditoria_csv_modificado'
    target = copied_approval(case_id)
    with (target / 'enriched.csv').open('a') as stream:
        stream.write('\n')
    rejected(lambda: p.approved_dataframe(case_id))


def wrong_run():
    case_id = 'auditoria_aprobacion_otro_lote'
    target = copied_approval(case_id)
    approval = json.loads((target / 'approval.json').read_text(encoding='utf-8'))
    approval['run_id'] = 'otro_lote'
    p.save_json(target / 'approval.json', approval)
    rejected(lambda: p.approved_dataframe(case_id))


check('bloqueo de csv modificado despues de validar', changed_csv)
check('bloqueo de aprobacion perteneciente a otro lote', wrong_run)


def model_counts():
    expected = {'dim_city': 5, 'dim_date': 3653, 'dim_aqi': 6, 'fact_air_quality': 18265}
    with psycopg.connect(os.environ['INDIA_DATABASE_URL']) as con:
        with con.cursor() as cur:
            for table, rows in expected.items():
                cur.execute('SELECT COUNT(*) FROM india_dw.' + table)
                require(cur.fetchone()[0] == rows)
            cur.execute('SELECT COUNT(*) FROM india_dw.air_quality_enriched')
            require(cur.fetchone()[0] == 18265)


check('conteos de dimensiones hechos y vista', model_counts)


def sql_rejection(statement, kind):
    with psycopg.connect(os.environ['INDIA_DATABASE_URL']) as con:
        with con.cursor() as cur:
            cur.execute('SAVEPOINT prueba_restriccion')
            try:
                cur.execute(statement)
            except kind:
                cur.execute('ROLLBACK TO SAVEPOINT prueba_restriccion')
            else:
                cur.execute('ROLLBACK TO SAVEPOINT prueba_restriccion')
                raise AssertionError('la restriccion de la base no bloqueo el cambio')


first, second = sorted(valid.measurement_id.astype(int).tolist())[:2]
check('postgresql rechaza aqi negativo', lambda: sql_rejection(f'UPDATE india_dw.fact_air_quality SET aqi=-1 WHERE measurement_id={first}', psycopg.errors.CheckViolation))
check('postgresql rechaza ciudad inexistente', lambda: sql_rejection(f'UPDATE india_dw.fact_air_quality SET city_id=999999 WHERE measurement_id={first}', psycopg.errors.ForeignKeyViolation))
check('postgresql rechaza medicion duplicada', lambda: sql_rejection(f'UPDATE india_dw.fact_air_quality SET measurement_id={first} WHERE measurement_id={second}', psycopg.errors.UniqueViolation))
check('postgresql rechaza aqi nulo', lambda: sql_rejection(f'UPDATE india_dw.fact_air_quality SET aqi=NULL WHERE measurement_id={first}', psycopg.errors.NotNullViolation))
check('restricciones probadas sin alterar datos', lambda: require(database_snapshot() == before))


def idempotent_reload():
    p.load_warehouse(run_id)
    require(database_snapshot() == before)


check('recarga sin duplicar ni cambiar mediciones', idempotent_reload)


def reconcile():
    with psycopg.connect(os.environ['INDIA_DATABASE_URL']) as con:
        with con.cursor() as cur:
            cur.execute('SELECT measurement_id,aqi,precipitation_sum FROM india_dw.fact_air_quality ORDER BY measurement_id')
            loaded = pd.DataFrame(cur.fetchall(), columns=['measurement_id', 'aqi', 'precipitation_sum'])
    expected = valid[loaded.columns].sort_values('measurement_id').reset_index(drop=True)
    pd.testing.assert_frame_equal(loaded, expected, check_dtype=False)


check('mediciones cargadas coinciden con el archivo aprobado', reconcile)
p.visualize_warehouse(run_id)
reports = ROOT / 'reports' / path.name


def plot_files():
    from PIL import Image
    files = list(reports.glob('*.png'))
    require(len(files) == 4)
    for file in files:
        with Image.open(file) as image:
            require(image.width >= 1000 and image.height >= 500)
            image.verify()


check('cuatro graficos validos y con resolucion suficiente', plot_files)
check('dashboard enlaza sus cuatro graficos', lambda: require(all(file.name in (reports / 'dashboard.html').read_text(encoding='utf-8') for file in reports.glob('*.png'))))
check('resumen mensual completo de cinco ciudades', lambda: require(len(pd.read_csv(reports / 'monthly.csv')) == 600))


def scheduled_run():
    proof = json.loads((ROOT / 'evidencias/scheduler_run.json').read_text(encoding='utf-8'))
    require(proof['run_type'] == 'scheduled' and proof['tasks_success'] == 9 and proof['state'] == 'success')
    with sqlite3.connect(Path(os.environ['AIRFLOW_HOME']) / 'airflow.db') as con:
        row = con.execute('SELECT state,run_type FROM dag_run WHERE dag_id=? AND run_id=?', (proof['dag_id'], proof['run_id'])).fetchone()
        tasks = con.execute('SELECT state FROM task_instance WHERE dag_id=? AND run_id=?', (proof['dag_id'], proof['run_id'])).fetchall()
    require(row == ('success', 'scheduled') and len(tasks) == 9 and all(row[0] == 'success' for row in tasks))


check('ejecucion diaria real creada por el scheduler', scheduled_run)
check('primera entrega intacta al terminar', lambda: require(hashlib.sha256(p.FIRST_DB.read_bytes()).hexdigest() == first_hash))
report = {'verified_at_utc': p.utcnow(), 'run_id': run_id, 'total': len(results),
          'passed': sum(item['passed'] for item in results), 'failed': sum(not item['passed'] for item in results),
          'tests': results}
p.save_json(ROOT / 'evidencias/auditoria_completa.json', report)
print(json.dumps({'total': report['total'], 'passed': report['passed'], 'failed': report['failed']}), flush=True)
if report['failed']:
    raise RuntimeError('hay comprobaciones pendientes en la auditoria')
