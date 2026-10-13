# Web — dashboard y asistente

Frontend del TFM *Asistente de Calidad del Aire de Madrid*. Consume únicamente la API
[`ApiUsuario`](../ApiUsuario/README.md) y no tiene estado propio en servidor.

**Stack:** Vite + React 19 + TypeScript · TanStack Query · Recharts · react-router ·
tipos generados desde el OpenAPI del backend con `openapi-typescript` + cliente
`openapi-fetch`. Sin librería de componentes: CSS moderno con *custom properties*
(modo claro y oscuro con valores propios).

## Pantallas

| Ruta | Pantalla | Qué muestra |
|---|---|---|
| `/` | **Hoy** | Cifra protagonista (anomalías fiables del día de referencia), *stat tiles* con sparkline por contaminante, informe del día (POST `/chat` con prompt rotativo) y bloques marcados |
| `/estaciones` | **Estaciones** | Mapa (por defecto) con un marcador por estación y tooltip, o tabla ordenable (`?vista=lista`). Leaflet con mapa base gris de Esri, claro u oscuro según el tema, sin clave de API; las teselas se descargan de internet. Las estaciones sin coordenadas se listan bajo el mapa |
| `/estaciones/:codigo` | **Detalle** | Línea de tendencia por bloques, heatmap días × bloques, tabla alternativa, rangos 7/30/90/personalizado, filtro por bloque y comparación con hasta 2 estaciones más |
| `/asistente` | **Asistente** | Preguntas independientes al asistente: chips sugeridas, contador de espera, fuentes separadas por tipo (`sql` / `documento`) y citas `[Dn]` enlazadas |
| `/metodologia` | **Metodología** | Datos, bloques, detector, regla de cobertura, limitaciones, aviso completo y bibliografía del corpus |

El **aviso médico** es un banner persistente dentro de la cabecera fija: se ve sin
hacer scroll en todas las pantallas. Su texto es la `advertencia` de la última
respuesta del asistente o, si la API no envía ninguna, el fijo del proyecto.

El asistente también está disponible en todas las pantallas como **burbuja flotante** (abajo a la
derecha) que despliega un panel con los mismos componentes que `/asistente`. Las consultas de la
sesión viven en un contexto global (`ChatProvider`): se conservan al cambiar de pantalla, una
petición en vuelo no se pierde al cerrar el panel, y la burbuja marca con un punto si llegó una
respuesta mientras estaba cerrado. En `/asistente` la burbuja se oculta para no duplicar el campo.
`?chat=abierto` en la URL abre el panel al cargar.

## Reglas de correctitud que implementa la interfaz

- `is_anomaly` **solo se interpreta con `cobertura >= 0,7`**. Por debajo, el bloque se
  muestra con su dato y la etiqueta «Cobertura baja»: ni anomalía ni normal
  (`src/dominio/anomalias.ts`).
- Los valores nulos se dibujan como **huecos**, nunca como ceros (`connectNulls={false}`,
  celdas vacías en el heatmap, «—» en tablas).
- `anomaly_score` es el score negado del Isolation Forest (alto = más anómalo): nunca se
  muestra como porcentaje ni como confianza.
- El color codifica **magnitud** (rampa azul) o **estado de anomalía** (paleta de estado, siempre
  con icono y texto). No hay bandas tipo AQI: la API calcula anomalías, no índices sanitarios.
- Nunca doble eje Y. Comparar estaciones = mismo contaminante, un solo eje, máximo 3 series con
  etiqueta directa y leyenda. Toda gráfica tiene tooltip (y crosshair en las de línea) y una
  alternativa en tabla.
- El sistema **no tiene memoria**: cada pregunta al asistente es independiente y la interfaz
  lo dice; el historial vive en el estado de React y se pierde al recargar.

## Desarrollo

Requisitos: Node ≥ 20.19 y la API levantada en `http://localhost:8000`
(`uvicorn api_usuario.main:app --reload --app-dir src` desde `ApiUsuario/`; sin
`ORCHESTRATOR_URL`, `/chat` responde un *stub* y el asistente se puede probar entero).

```bash
cd Web
npm ci
npm run dev        # http://localhost:5173, con proxy /api -> localhost:8000
npm run lint
npm test           # vitest: regla de cobertura, citas [Dn] y fechas (lógica pura, sin DOM)
npm run build      # tsc -b && vite build
```

El frontend llama **siempre** a rutas relativas `/api/...`. En desarrollo Vite hace de
proxy (`vite.config.ts`, variable opcional `VITE_DEV_API_PROXY` si la API no está en
`:8000`); en producción lo hace nginx. Nunca hay una URL absoluta de la API en el código.

## Contrato con la API (tipos generados)

`openapi.json` es una instantánea del OpenAPI de `ApiUsuario` y `src/api/schema.d.ts`
se genera a partir de él. **No se editan a mano.** Cuando cambie el backend:

```bash
# con las dependencias de ApiUsuario instaladas (fastapi, pydantic, sqlalchemy, httpx)
npm run contrato          # = contrato:exportar (Python) + contrato:tipos (openapi-typescript)
```

El job `frontend` de CI repite esos dos pasos y falla con `git diff --exit-code` si el
contrato versionado no coincide con el código del backend. Así una deriva de esquema se
detecta en el PR, no en producción. Las versiones de FastAPI/pydantic del job están fijadas a
las que generaron el fichero.

## Despliegue: dos contenedores, un solo origen

```
navegador ──► web (nginx :80)  ── /          → build estático (SPA)
                               ── /api/*     → api:8000/*      (proxy_read_timeout 60 s)
                               ── /api/chat  → api:8000/chat   (proxy_read_timeout 180 s)
```

- `Dockerfile`: build multi-stage `node:22-alpine` → `nginx:1.27-alpine`.
- `nginx.conf`: proxy de `/api/*` quitando el prefijo; `/api/chat` con `proxy_read_timeout 180s`
  porque una respuesta del asistente puede tardar ~120 s y el valor por defecto (60 s) la cortaría.
- `docker-compose.yml` (raíz): servicios `db`, `api` (imagen de `ApiUsuario/Dockerfile`) y `web`.

```bash
docker compose up -d --build     # http://localhost:8080  (WEB_PORT en .env para cambiarlo)
```

## Estructura

```
Web/
├── Dockerfile · nginx.conf · openapi.json · scripts/exportar_openapi.py
└── src/
    ├── api/          cliente tipado (openapi-fetch), hooks de TanStack Query, errores por código HTTP
    ├── dominio/      constantes, regla de cobertura y estados, fechas, formato, citas [Dn]
    ├── estado/       tema claro/oscuro, texto del aviso, estación seleccionada
    ├── estilos/      tokens.css (paleta), base.css, componentes.css
    ├── componentes/  layout · ui · filtros · graficas · chat · hoy
    └── paginas/      Hoy · Estaciones · DetalleEstacion · Asistente · Metodologia
```
