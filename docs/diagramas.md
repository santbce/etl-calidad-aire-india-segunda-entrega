#aqui mostramos los diagramas de la segunda entrega

#aqui mostramos la arquitectura

```mermaid
flowchart LR
  S1[(sqlite primera entrega)] --> A[extraer sql aire]
  W[wikipedia html] --> C[(sqlite coordenadas)]
  C --> B[extraer dataset ciudades desde bd]
  B --> P[extraer api histórica open-meteo]
  A --> E[eda de tres fuentes]
  B --> E
  P --> E
  E --> T[normalizar y unir por ciudad y fecha]
  T --> Q[checkpoint great expectations]
  Q -->|Aprobado| D[(postgresql warehouse estrella)]
  Q -->|Rechazado| F[tarea fallida y carga bloqueada]
  D --> V[consultas sql y visualizaciones]
```

#aqui mostramos el modelo estrella

grano: una medicion por ciudad y dia. el ddl completo esta en `sql/warehouse.sql`; el diagrama destaca las medidas principales.

```mermaid
erDiagram
  dim_city ||--o{ fact_air_quality : city_id
  dim_date ||--o{ fact_air_quality : date
  dim_aqi ||--o{ fact_air_quality : aqi_id
  dim_city {
    integer city_id PK
    text city_name
    real latitude
    real longitude
    text source_url
  }
  dim_date {
    date date PK
    integer year
    integer month
    integer day
    integer quarter
    text day_of_week
  }
  dim_aqi {
    integer aqi_id PK
    text aqi_bucket
  }
  fact_air_quality {
    integer measurement_id PK
    integer city_id FK
    date date FK
    integer aqi_id FK
    real aqi
    real pm25
    real pm10
    real temperature_2m_mean
    real precipitation_sum
    real wind_speed_10m_max
    text weather_model
    text run_id
  }
```
