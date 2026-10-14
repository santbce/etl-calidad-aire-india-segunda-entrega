[abrir notebook en colab](https://colab.research.google.com/github/santbce/etl-calidad-aire-india-segunda-entrega/blob/main/notebooks/Segunda_entrega_India_Colab.ipynb)

#aqui presentamos la segunda entrega de calidad del aire en india

samuel flores y santiago becerra. curso etl g51.

aqui continuamos el proyecto de la primera entrega sobre aqi y contaminantes en bangalore, chennai, delhi, kolkata y mumbai. lo que queremos revisar es como se relacionan temperatura, precipitación y viento con aqi y pm2.5 por ciudad y periodo, conservando las 18.265 mediciones de 2015–2024. estas comparaciones nos muestran asociaciones; no demuestran que el clima cause los cambios del aqi. la relación temática se mantiene con ods 3 y ods 11.

#aqui explicamos como abrir el proyecto en colab

abre `Segunda_entrega_India_Colab.ipynb` en google colab. ejecuta las celdas en orden. carga el zip original `etl-calidad-aire-india-main.zip` o léelo desde la carpeta `second delivery` en tu drive. el notebook incluye el código completo y genera los archivos de este repositorio. no requiere docker.

se usa un entorno aislado de python 3.12 con airflow 2.11.2 y los constraints oficiales, siguiendo `airflow_intro.ipynb` y `airflow_gx.ipynb` del profesor. gx utiliza el mismo patrón suite → data source → asset → batch definition → validation definition → checkpoint de s8. aqui usamos gx 1.24.0 para repetir las pruebas con la misma version.

colab instala postgresql dentro de la sesión linux. la primera entrega y las coordenadas siguen en sqlite como fuentes. el warehouse final está en postgresql, de acuerdo con el stack solicitado en la segunda entrega. se crea un esquema dedicado `india_dw`; ninguna tarea escribe en la base original.

#aqui explicamos de donde sacamos los datos

1. primera entrega: `primera_entrega/database/air_quality.db`, procedente de kaggle [aqi air quality data india 2015–2024](https://www.kaggle.com/datasets/sathvikisikella/cleaned-air-quality-india). se extraen desde sql solo identificadores, ciudad, fecha, categoría y contaminantes del periodo requerido.
2. dataset adicional: coordenadas de las cinco páginas de ciudades en wikipedia. se extrae la coordenada principal del artículo usando `requests` y `BeautifulSoup`, se conserva el html y se carga en `data/cities.sqlite`. la tarea del dag extrae desde la tabla `city_coordinates`. aporta cinco registros y se relaciona muchos a uno con las mediciones. este es un dataset geográfico pequeño, no una segunda serie de mediciones.
3. api pública: [historical weather api de open-meteo](https://open-meteo.com/en/docs/historical-weather-api), endpoint `https://archive-api.open-meteo.com/v1/archive`. consultas ciudad/año, modelo era5, zona `Asia/Kolkata`, temperatura media diaria en °c, precipitación diaria en mm y viento máximo diario a 10 m en m/s. se conservan coordenadas solicitadas y coordenadas de la celda devueltas. la fuente es reanálisis, no una estación meteorológica específica. la caché guarda parámetros, url, json y fecha utc de extracción. se reutiliza hasta 24 horas, de forma explícita en la procedencia.

#aqui dejamos las preguntas que queremos responder

- comparar aqi por ciudad y evolución anual/mensual.
- explorar las asociaciones de aqi y pm2.5 con las tres variables del clima por ciudad; evitar confundir diferencias entre ciudades con una asociación global.
- conservar las 18.265 combinaciones ciudad/día, sin duplicar ni descartar registros válidos.
- bloquear publicación ante nulos, claves duplicadas, fechas fuera del periodo, valores negativos o falta de coincidencia meteorológica.
- obtener gráficos únicamente desde filas aprobadas y cargadas en postgresql.

#aqui mostramos la arquitectura y dag

```mermaid
flowchart LR
 A[(SQLite primera entrega)] --> E[Extraer SQL aire]
 W[Wikipedia HTML] --> C[(SQLite dataset coordenadas)]
 C --> X[Extraer ciudades desde BD]
 X --> P[Extraer API Open-Meteo]
 E --> D[EDA de tres fuentes]
 X --> D
 P --> D
 D --> T[Normalizar e integrar]
 T --> G[Checkpoint Great Expectations]
 G -->|Aprobado| L[(PostgreSQL esquema estrella)]
 G -->|Rechazado| F[Falló tarea y carga bloqueada]
 L --> V[SQL gráficos y dashboard]
```

el dag `etl_calidad_aire_india_enriquecido` usa `with DAG`, `PythonOperator` y dependencias `>>`, igual que los ejemplos de clase. se programa cada día, con `catchup=False` y `max_active_runs=1`. la primera extracción y las coordenadas pueden ejecutarse independientemente. la api espera las coordenadas. eda, merge, validación, carga y visualización siguen en ese orden.

se detiene la tarea de validación con una excepción si falla el checkpoint. la regla por defecto `all_success` impide ejecutar carga y visualización. se escogió este flujo en vez del branching de demostración porque aquí la carga es real y un lote inválido debe dejar la ejecución fallida. 

#aqui explicamos lo que revisamos antes de unir los datos

el notebook genera tipos, nulos, duplicados de fila y de clave, frecuencias, estadísticas, iqr e histogramas para las tres fuentes antes de publicar. los resultados se guardan por ejecución en `data/runs/<run_id>/eda_sources.json`. el notebook ejecutado muestra estas tablas e incluye la interpretación de los resultados.

se convierte la fecha a iso y las medidas a numérico. las claves de ciudad provienen de la base original. se unen coordenadas por `city_id` con `validate='many_to_one'` y clima por `city_id,date` con `validate='one_to_one'`, conservando todas las filas con `left join`. los registros sin clima se documentan y detienen el lote. no se imputan datos meteorológicos, no se convierten nulos en cero ni se eliminan outliers solo por iqr. no se inventan unidades de contaminantes que el dataset no documenta.

las fechas originales se interpretan como días locales de india. es una hipótesis explícita porque la fuente no documenta la zona horaria; requiere revisión con la documentación del dataset antes de interpretar asociaciones diarias como equivalentes exactas. la categoría aqi se conserva; no se recalcula con umbrales de otra escala.

#aqui explicamos las validaciones

aqui revisamos el conteo exacto, campos obligatorios no nulos, identificador único, unicidad compuesta ciudad/fecha, ciudades y categorías permitidas, fechas del periodo, rangos geográficos, contaminantes/aqi/lluvia/viento finitos y no negativos y un rango físico amplio de temperatura. un control adicional compara el conjunto completo de las cinco ciudades y los 3.653 días esperados.

se guardan `expectation_suite.json`, `validation.json` y una aprobación con sha-256 del csv validado. la carga verifica que el lote no haya cambiado desde la aprobación. una validación anterior no habilita un archivo modificado. aqui guardamos el resultado del checkpoint para revisar cada validacion.

#aqui explicamos como guardamos los datos

aqui cada fila de la tabla de hechos corresponde a una ciudad y un dia. `fact_air_quality` mantiene mediciones originales y clima. las dimensiones son `dim_city` (coordenadas y procedencia), `dim_date` (día/calendario) y `dim_aqi` (categorías). la vista `air_quality_enriched` reconstruye el dataset enriquecido. `etl_runs` registra la ejecución y su validación.

postgresql aplica pk, fk, unique y check. se reemplaza el lote dentro de una sola transacción: fallos revierten la carga, reejecutar no acumula mediciones. es una instantánea histórica, no una carga incremental de datos actuales. la programación diaria renueva la extracción y comprueba el lote, sin afirmar que el csv histórico crece.

#aqui mostramos los resultados y las pruebas

las figuras y el dashboard se generan leyendo únicamente `india_dw.air_quality_enriched` tras la carga. incluyen promedio de aqi por ciudad, distribuciones del clima, evolución anual y correlaciones por ciudad. `findings.json`, `monthly.csv` y `correlations_by_city.csv` permiten comprobar cifras. las conclusiones de la ejecución se presentan en `docs/resultados_ejecucion.md`.

aqui probamos la orquestacion ejecutando `airflow dags test` y consultamos `dag_run` y `task_instance` en la base de metadatos. después iniciamos scheduler/webserver para observar el dag. la prueba deliberada introduce precipitación negativa en un lote separado: gx debe rechazarlo y el lote del warehouse debe conservarse. el notebook descarga el código, reportes y un dump postgresql al terminar.

#aqui explicamos como guardar y entregar el proyecto

airflow puede programar ejecuciones mientras el runtime de colab permanece activo. colab no garantiza un servicio diario permanente y al terminar la sesión pueden desaparecer el entorno y las bases. por eso se exportan resultados y dump al drive. para operación continua futura habría que alojar el mismo dag en un servidor persistente; docker no es necesario para esta práctica.

el notebook completo se ejecutó en colab. la interpretación, los estados de las nueve tareas, el fallo deliberado y la comprobación de idempotencia están incluidos en esta entrega. el repositorio público contiene el código y las evidencias. la entrega al profesor se realiza mediante el enlace o los archivos según la plataforma del curso.

#aqui relacionamos el codigo con las practicas del profe

| material | uso en la implementación |
|---|---|
| s6 extracción y notebooks | sqlite, consulta sql, `requests`, `BeautifulSoup`, json, timeout, retry y merge |
| ingesta_multiple_practica | contratos de entrada, cardinalidad, procedencia y fallo deliberado |
| airflow airflow_intro | python 3.12 aislado, airflow 2.11.2, pythonoperator, dag, `dags test` |
| airflow airflow_retries | distinguir fallos temporales de contrato |
| s8 gx y airflow_gx | suite, asset, lote, validationdefinition, checkpoint y bloqueo de carga |


#aqui dejamos las fuentes que consultamos

- consigna proporcionada: project second delivery, cinco páginas.
- propuesta del equipo: diseño de extracción y orquestación para la calidad del aire en india.
- [documentación de open-meteo](https://open-meteo.com/en/docs/historical-weather-api).
- [dags de apache airflow](https://airflow.apache.org/docs/apache-airflow/2.11.0/core-concepts/dags.html).
- [gx data context](https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/create_a_data_context/).

#aqui explicamos las carpetas del repositorio

`data/etl-calidad-aire-india-main.zip`: proyecto original sin cambios; el notebook lo extrae en `primera_entrega/` durante la ejecución. `src/`: funciones python. `dags/`: orquestación. `sql/`: ddl postgresql. `data/`: zip original; las fuentes adicionales, caché y lotes se generan al ejecutar el notebook. `reports/`: dashboard y hallazgos desde warehouse. `notebooks/`: notebook colab. `tests/`: pruebas de protección de carga.


#aqui mostramos lo que dio la ejecucion

el dag completo termino con nueve tareas exitosas. se validaron 18.265 mediciones, la carga invalida quedo bloqueada y se comprobo idempotencia. ver [informe de resultados](docs/resultados_ejecucion.md), [diagramas](docs/diagramas.md), [comprobaciones](evidencias/comprobaciones.json) y [estados de airflow](evidencias/airflow_task_states.csv). el zip de la primera entrega esta en data para reproducir la ejecucion.

![aqi por ciudad](reports/resultados/01_aqi_ciudad.png)

![evolucion anual](reports/resultados/03_aqi_anual.png)

![correlaciones por ciudad](reports/resultados/04_correlaciones_ciudad.png)


#aqui explicamos como repetir la prueba de carga

después de ejecutar el notebook, establece `INDIA_PROJECT_ROOT` con la carpeta del proyecto y ejecuta `python -m unittest discover -s tests`. la prueba usa el último lote aprobado; `INDIA_TEST_RUN_ID` permite seleccionar uno. comprueba el rechazo de precipitación negativa y de un csv modificado después de aprobarlo. sin un lote previo, informa que la prueba se omite.

#aqui dejamos la comprobacion final

las 42 comprobaciones aprobaron en colab el 13 de octubre de 2026 (hora de colombia). incluyen respuestas de api, uniones, once lotes invalidos, restricciones de postgresql, recarga sin duplicados, conciliacion con el csv, graficos, servicios y restauracion del dump. ver [auditoria completa](evidencias/auditoria_completa.json), [ejecucion diaria](evidencias/scheduler_run.json) y [prueba manual final](evidencias/manual_run.json). el scheduler diario y la prueba manual terminaron con nueve tareas exitosas cada uno.

para repetir la auditoria completa, ejecuta `tests/auditoria_colab.py` con el python del entorno de airflow despues del notebook. necesita `INDIA_PROJECT_ROOT`, `INDIA_DATABASE_URL`, `INDIA_SOURCE_ZIP` y `AIRFLOW_HOME`, ademas del lote aprobado y la evidencia del scheduler. la celda de auditoria configura la ruta del zip.
