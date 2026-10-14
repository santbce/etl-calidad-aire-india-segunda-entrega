#aqui resultados de la segunda entrega en colab

samuel flores y santiago becerra. etl g51.

integramos 18,265 mediciones de calidad del aire con coordenadas y clima histórico. el aqi promedio fue 317.51. las gráficas y las correlaciones se calcularon desde postgresql después de aprobar great expectations.

#aqui eda y decisiones

air: 18,265 filas, 0 valores faltantes y 0 claves duplicadas. los tipos y estadísticas completos se conservan en eda_sources.json.

cities: 5 filas, 0 valores faltantes y 0 claves duplicadas. los tipos y estadísticas completos se conservan en eda_sources.json.

weather: 18,265 filas, 0 valores faltantes y 0 claves duplicadas. los tipos y estadísticas completos se conservan en eda_sources.json.

el iqr marcó 532 observaciones en temperature_2m_mean, 2653 observaciones en precipitation_sum, 262 observaciones en wind_speed_10m_max. conservamos esas observaciones porque una marca estadística no demuestra un error. no imputamos clima ni eliminamos mediciones válidas.

la dimensión geográfica contiene solo cinco ciudades. sus valores extremos por iqr reflejan diferencias entre ubicaciones y no justifican eliminar ciudades. los datos originales no presentan outliers iqr en los contaminantes; este resultado no demuestra representatividad de la fuente de kaggle.

#aqui visualizaciones e interpretación

las categorías aqi más frecuentes fueron: very poor: 5,501, severe: 5,258, moderate: 4,258, poor: 3,129, satisfactory: 113, good: 6. los gráficos muestran promedios por ciudad, evolución anual, distribuciones del clima y correlaciones separadas por ciudad.

           temperature_2m_mean  precipitation_sum  wind_speed_10m_max
bangalore               -0.007              0.042              -0.016
chennai                  0.023             -0.021               0.011
delhi                    0.017              0.005               0.014
kolkata                  0.003              0.033               0.002
mumbai                  -0.004             -0.016               0.006

la mayor magnitud de correlación entre aqi y las variables meteorológicas fue 0.042 en el análisis por ciudad. las asociaciones lineales son pequeñas y no sustentan una explicación causal del aqi.

las mediciones de aire proceden de kaggle y no documentan todas las unidades. era5 representa celdas de reanálisis y las coordenadas corresponden a ciudades, no a estaciones. interpretamos las fechas originales como días locales de india; esta hipótesis requiere confirmación de la procedencia antes de presentar coincidencia temporal exacta.

#aqui evidencia de calidad y orquestación

la suite aprobó el lote válido con cobertura completa. la falla deliberada de precipitación negativa fue rechazada, su carga quedó bloqueada y la huella del warehouse no cambió. la tabla airflow_task_states.csv registra los estados reales del dag. el dump warehouse.sql conserva la instantánea postgresql.

la arquitectura, los pasos etl, el diseño del dag, la api y la suite se documentan en readme.md. el código, los diagramas y las evidencias están publicados en el repositorio público del equipo: https://github.com/santbce/etl-calidad-aire-india-segunda-entrega. la ejecución completa de prueba del dag terminó con nueve tareas exitosas. la comprobación de una ejecución activada por el scheduler está pendiente; no se presenta como una prueba diaria ya completada. después de esa revisión, el equipo realizará la entrega en la plataforma del curso.
