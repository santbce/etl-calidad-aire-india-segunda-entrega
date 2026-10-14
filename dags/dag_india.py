#aqui organizamos las tareas del proyecto con airflow
from datetime import timedelta
from pathlib import Path
import os
import sys
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

ROOT = Path(os.environ.get('INDIA_PROJECT_ROOT','/content/segunda_entrega_india'))
sys.path.insert(0,str(ROOT/'src'))
from pipeline_india import (prepare_run, extract_air, extract_cities, extract_weather,
                            eda_sources, transform_merge, validate_enriched,
                            load_warehouse, visualize_warehouse)

default_args = {'owner':'Samuel Flores y Santiago Becerra', 'depends_on_past':False,'retries':0}
with DAG('etl_calidad_aire_india_enriquecido', default_args=default_args,
         description='Tres fuentes, Great Expectations y PostgreSQL',
         schedule_interval=timedelta(days=1),
         start_date=pendulum.datetime(2026,10,12,tz='Asia/Kolkata'),
         catchup=False,max_active_runs=1,tags=['india','segunda_entrega']) as dag:
    def operator(name, function, minutes=20):
        return PythonOperator(task_id=name,python_callable=function,
                              op_kwargs={'run_id':'{{ run_id }}'},
                              execution_timeout=timedelta(minutes=minutes))
    preparar = operator('preparar_ejecucion',prepare_run)
    aire = operator('extraer_primera_entrega_sqlite',extract_air)
    ciudades = operator('extraer_dataset_ciudades_bd',extract_cities)
    clima = operator('extraer_api_clima',extract_weather,45)
    eda = operator('eda_tres_fuentes',eda_sources)
    transformar = operator('transformar_integrar',transform_merge)
    validar = operator('validar_great_expectations',validate_enriched)
    cargar = operator('cargar_data_warehouse',load_warehouse)
    graficar = operator('visualizar_desde_warehouse',visualize_warehouse)
    preparar >> [aire,ciudades]
    ciudades >> clima
    [aire,ciudades,clima] >> eda >> transformar >> validar >> cargar >> graficar
