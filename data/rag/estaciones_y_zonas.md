---
titulo: Estaciones de medición y zonas de Madrid
tema: estaciones
contaminantes: []
fuente: "Ayuntamiento de Madrid — datos abiertos"
---

# Estaciones de medición y zonas de Madrid

La red de vigilancia de la calidad del aire de Madrid tiene 24 estaciones repartidas por la
ciudad. Se clasifican por el entorno que representan, lo que es clave para interpretar sus
datos y recomendar zonas:

- **Urbana tráfico**: junto a vías con mucho tráfico. Registran los niveles más altos de NO2
  y NOx. Representan la exposición de quien camina por ejes de tráfico.
- **Urbana fondo**: en zonas residenciales alejadas del tráfico directo. Reflejan el aire que
  respira la mayoría de la población en su barrio.
- **Suburbana**: en la periferia y zonas verdes. Menor NO2 pero mayor ozono en verano.

## Catálogo de estaciones

| Código | Nombre | Tipo | Zona orientativa |
|---|---|---|---|
| 4  | Plaza de España | Urbana tráfico | Centro |
| 8  | Escuelas Aguirre | Urbana tráfico | Retiro / Salamanca (cruce Alcalá-O'Donnell) |
| 11 | Ramón y Cajal | Urbana tráfico | Chamartín |
| 16 | Arturo Soria | Urbana fondo | Ciudad Lineal |
| 17 | Villaverde | Urbana fondo | Villaverde (sur) |
| 18 | Farolillo | Urbana fondo | Carabanchel |
| 24 | Casa de Campo | Suburbana | Gran parque oeste |
| 27 | Barajas Pueblo | Urbana fondo | Barajas (este) |
| 35 | Plaza del Carmen | Urbana fondo | Centro (Sol) |
| 36 | Moratalaz | Urbana tráfico | Moratalaz |
| 38 | Cuatro Caminos | Urbana tráfico | Tetuán |
| 39 | Barrio del Pilar | Urbana tráfico | Fuencarral |
| 40 | Vallecas | Urbana fondo | Puente de Vallecas |
| 47 | Méndez Álvaro | Urbana fondo | Arganzuela |
| 48 | Castellana | Urbana tráfico | Paseo de la Castellana |
| 49 | Parque del Retiro | Urbana fondo | Interior del Retiro (zona verde) |
| 50 | Plaza Castilla | Urbana tráfico | Chamartín (nudo de tráfico) |
| 54 | Ensanche de Vallecas | Urbana fondo | Vallecas (nuevo) |
| 55 | Urb. Embajada | Urbana fondo | Barajas (Alameda de Osuna) |
| 56 | Plaza Elíptica | Urbana tráfico | Usera (uno de los puntos más contaminados) |
| 57 | Sanchinarro | Urbana fondo | Hortaleza (norte) |
| 58 | El Pardo | Suburbana | Monte de El Pardo (aire más limpio) |
| 59 | Juan Carlos I | Suburbana | Parque Juan Carlos I (este) |
| 60 | Tres Olivos | Urbana fondo | Fuencarral norte |

## Cómo usar esto para recomendar zonas

- Para **minimizar NO2/partículas** (alergias, asma, tráfico): las estaciones de fondo y
  suburbanas suelen dar mejores valores — Retiro (49), Casa de Campo (24), El Pardo (58),
  Juan Carlos I (59).
- Para **ozono en verano**: ocurre lo contrario; las suburbanas (24, 58, 59) pueden tener los
  picos más altos por la tarde, mientras que el centro con tráfico tiene menos O3.
- Los puntos históricamente más cargados de NO2 son de tráfico: Plaza Elíptica (56),
  Plaza de España (4), Escuelas Aguirre (8), Castellana (48), Plaza Castilla (50).
- Para dar la "mejor zona hoy", el asistente debe consultar los datos reales del día
  (tool de comparación de estaciones) y no basarse solo en esta tabla orientativa.

## Fuentes

- Ayuntamiento de Madrid — Portal de datos abiertos, «Estaciones de control de calidad del aire»
  (catálogo usado en `data/raw/estaciones-de-control.csv`). <https://datos.madrid.es>

> Este documento es una **síntesis divulgativa** elaborada para el proyecto a partir de las fuentes citadas; no reproduce literalmente su contenido. Verifica las cifras exactas y su vigencia en los documentos originales antes de cualquier uso operativo. Consulta también el [aviso médico](aviso_medico.md).
