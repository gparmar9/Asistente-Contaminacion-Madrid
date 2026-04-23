# TFM – Sistema Inteligente de Monitorización de Calidad del Aire (Madrid)

Este proyecto desarrolla un sistema basado en Machine Learning para analizar datos de calidad del aire en Madrid, detectar anomalías automáticamente y proporcionar información accesible mediante informes y un asistente conversacional.

---

## Descripción

La ciudad de Madrid dispone de una red de estaciones que miden la calidad del aire de forma continua. Sin embargo, estos datos suelen consumirse en bruto (CSV), lo que dificulta:

- Detectar fallos en sensores
- Interpretar tendencias
- Conocer el estado del aire en tiempo real

Este proyecto propone una solución integral con dos componentes principales:

### Parte 1 — Detección de anomalías

Modelos de ML aprenden el comportamiento normal de cada estación y contaminante para detectar:

- Fallos de sensores (datos congelados, incoherentes, etc.)
- Valores anómalos respecto al patrón histórico
- Episodios ambientales inusuales

### Parte 2 — Informes y asistente conversacional

- Generación automática de informes diarios
- Chatbot para consultas en lenguaje natural (Whatsapp, Telegram por ejemplo):
  - "¿Dónde está el aire más limpio hoy?"
  - "¿Es normal este nivel de ozono en febrero?"

---

## Arquitectura del sistema

El sistema está diseñado en capas desacopladas:

```
API Madrid (tiempo real)
        |
   ingesta_datos_live.py
        |
   limpiar_datos.py  (normalización, formato largo)
        |
   PostgreSQL (tabla mediciones_live)
        |
   Modelos ML / Detección de anomalías
        |
   Informes + Chatbot LLM
```
![Arquitectura del sistema](docs/images/esquema_flujo_consulta.png)

---

## Instalación

### Requisitos

- Python 3.10+
- PostgreSQL instalado en local

### Dependencias

```bash
pip install -r requirements.txt
```

Las principales dependencias son:

- pandas, numpy — transformación de datos
- requests — llamadas a la API de Madrid
- sqlalchemy, psycopg2 — conexión a PostgreSQL
- python-dotenv — gestión de variables de entorno
- scikit-learn — modelado
- matplotlib, seaborn — visualización

### Variables de entorno

Crea un fichero `.env` en la raíz del proyecto con el siguiente contenido:

```
DATABASE_URL=postgresql://<usuario>:<contraseña>@host:puerto/<nombre_bd>
```

---

## Datos

### Fuentes utilizadas

- Datos históricos de calidad del aire (Madrid)
- Datos en tiempo real vía API de Ciudades Abiertas Madrid (actualización cada ~20 minutos)
- Información de estaciones de control

### Estructura de los datos

La API devuelve datos en formato ancho (H01–H24 por fila). El proceso de limpieza los transforma a formato largo, donde cada fila representa una medición horaria:

| Campo           | Tipo      | Descripción                                      |
|-----------------|-----------|--------------------------------------------------|
| provincia       | INTEGER   | Código de provincia                              |
| municipio       | INTEGER   | Código de municipio                              |
| estacion        | INTEGER   | Identificador de estación                        |
| magnitud        | INTEGER   | Código del contaminante                          |
| contaminante    | TEXT      | Nombre del contaminante (NO2, O3, PM10, etc.)    |
| punto_muestreo  | TEXT      | Identificador completo del punto de medición     |
| nombre_estacion | TEXT      | Nombre de la estación                            |
| ano             | INTEGER   | Año                                              |
| mes             | INTEGER   | Mes                                              |
| dia             | INTEGER   | Día                                              |
| hora            | INTEGER   | Hora de la medición (0–23)                       |
| valor           | FLOAT     | Valor medido del contaminante                    |
| validacion      | TEXT      | Indicador de validez (V = válido, N = no válido) |
| fecha           | TIMESTAMP | Fecha completa de la medición                    |
| dia_semana      | INTEGER   | Día de la semana (0 = lunes, 6 = domingo)        |
| es_fin_semana   | BOOLEAN   | True si es sábado o domingo                      |

### Contaminantes registrados

| Código | Nombre | Descripción              |
|--------|--------|--------------------------|
| 1      | SO2    | Dióxido de azufre        |
| 6      | CO     | Monóxido de carbono      |
| 7      | NO     | Óxido nítrico            |
| 8      | NO2    | Dióxido de nitrógeno     |
| 9      | PM2.5  | Partículas finas         |
| 10     | PM10   | Partículas en suspensión |
| 12     | NOx    | Óxidos de nitrógeno      |
| 14     | O3     | Ozono                    |

---

## Base de datos

### Tabla calidad_aire_horas_live

Almacena las mediciones en tiempo real recibidas de la API. Tiene una restricción de unicidad sobre `(estacion, magnitud, fecha)` para evitar duplicados en cada ejecución del script de ingesta.

La inserción usa `ON CONFLICT DO NOTHING`, por lo que ejecutar el script varias veces es seguro y no genera registros duplicados.

---

## ETL — Ingesta de datos en tiempo real

El script `src/etl/ingesta_datos_live.py` realiza los siguientes pasos:

1. Llama a la API de Madrid y obtiene mediciones de las últimas 24 horas
2. Transforma los datos a formato largo mediante `limpiar_datos_live()`
3. Conecta a PostgreSQL usando la variable de entorno `DATABASE_URL`
4. Crea la tabla `calidad_aire_horas_live` si no existe
5. Inserta los registros nuevos ignorando duplicados

---

## Modelado de anomalías

Se plantean dos tipos de anomalías:

### 1. Anomalías operativas (sensor)

- valores constantes prolongados
- datos faltantes
- incoherencias bruscas

### 2. Anomalías ambientales

- niveles inusuales respecto al histórico
- desviaciones respecto al comportamiento esperado

### Enfoques

Por definir

---

## Chatbot y LLM

El sistema integra un modelo de lenguaje para consultas en lenguaje natural:

### Funcionalidades

- Consultas sobre datos estructurados (vía SQL)
- Recuperación de contexto (normativa, salud)
- Respuestas explicativas

### Ejemplos

- "¿Qué estación tuvo peor calidad del aire ayer?"
- "¿Es habitual este nivel de NO2 en invierno?"

### Enfoque

- Herramientas controladas (no SQL libre expuesto al usuario)
- Separación entre datos estructurados (SQL) y conocimiento contextual (RAG)

---

## Tecnologías

- Python
- Pandas / NumPy
- Scikit-learn
- SQL / PostgreSQL
- SQLAlchemy + psycopg2
- FastAPI (API backend, pendiente)
- LLM (local o cloud, pendiente)

---

## Pipeline del sistema

1. Ingesta de datos (históricos + tiempo real)
2. Limpieza y transformación a formato largo
3. Almacenamiento en PostgreSQL sin duplicados
4. Ejecución de modelos de anomalía
5. Generación de scores
6. Agregación diaria
7. Generación de informes
8. Consulta vía chatbot
