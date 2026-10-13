# Resultados de la segunda entrega en Colab

Samuel Flores y Santiago Becerra. ETL G51.

Integramos 18,265 mediciones de calidad del aire con coordenadas y clima histórico. El AQI promedio fue 317.51. Las gráficas y las correlaciones se calcularon desde PostgreSQL después de aprobar Great Expectations.

## EDA y decisiones

air: 18,265 filas, 0 valores faltantes y 0 claves duplicadas. Los tipos y estadísticas completos se conservan en eda_sources.json.

cities: 5 filas, 0 valores faltantes y 0 claves duplicadas. Los tipos y estadísticas completos se conservan en eda_sources.json.

weather: 18,265 filas, 0 valores faltantes y 0 claves duplicadas. Los tipos y estadísticas completos se conservan en eda_sources.json.

El IQR marcó 532 observaciones en temperature_2m_mean, 2653 observaciones en precipitation_sum, 262 observaciones en wind_speed_10m_max. Conservamos esas observaciones porque una marca estadística no demuestra un error. No imputamos clima ni eliminamos mediciones válidas.

La dimensión geográfica contiene solo cinco ciudades. Sus valores extremos por IQR reflejan diferencias entre ubicaciones y no justifican eliminar ciudades. Los datos originales no presentan outliers IQR en los contaminantes; este resultado no demuestra representatividad de la fuente de Kaggle.

## Visualizaciones e interpretación

Las categorías AQI más frecuentes fueron: Very Poor: 5,501, Severe: 5,258, Moderate: 4,258, Poor: 3,129, Satisfactory: 113, Good: 6. Los gráficos muestran promedios por ciudad, evolución anual, distribuciones del clima y correlaciones separadas por ciudad.

           temperature_2m_mean  precipitation_sum  wind_speed_10m_max
Bangalore               -0.007              0.042              -0.016
Chennai                  0.023             -0.021               0.011
Delhi                    0.017              0.005               0.014
Kolkata                  0.003              0.033               0.002
Mumbai                  -0.004             -0.016               0.006

La mayor magnitud de correlación entre AQI y las variables meteorológicas fue 0.042 en el análisis por ciudad. Las asociaciones lineales son pequeñas y no sustentan una explicación causal del AQI.

Las mediciones de aire proceden de Kaggle y no documentan todas las unidades. ERA5 representa celdas de reanálisis y las coordenadas corresponden a ciudades, no a estaciones. Interpretamos las fechas originales como días locales de India; esta hipótesis requiere confirmación de la procedencia antes de presentar coincidencia temporal exacta.

## Evidencia de calidad y orquestación

La suite aprobó el lote válido con cobertura completa. La falla deliberada de precipitación negativa fue rechazada, su carga quedó bloqueada y la huella del warehouse no cambió. La tabla airflow_task_states.csv registra los estados reales del DAG. El dump warehouse.sql conserva la instantánea PostgreSQL.

La arquitectura, los pasos ETL, el diseño del DAG, la API y la suite se documentan en README.md. Revisar las salidas y publicar el repositorio desde la cuenta del equipo antes de entregar.
