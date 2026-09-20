# Instrucciones para agentes de IA

Instrucciones compartidas para cualquier asistente que trabaje en este repositorio (Claude Code,
Codex u otros). Claude Code las carga a través de `CLAUDE.md`.

**Proyecto:** TFM del Máster Pontia. Sistema de monitorización de la calidad del aire de Madrid:
ingesta del histórico y de la API en tiempo real, detección de anomalías con ML, asistente con
RAG + LLM y despliegue en AWS. Visión general y puesta en marcha: `README.md`.

## 1. Notas para la memoria del TFM (obligatorio)

`docs/notas_memoria.md` es la materia prima para redactar la memoria final (**máximo 25 páginas**).
Mantenerlo al día forma parte de cada tarea, no es un trabajo aparte.

**Cuándo anotar:** al terminar un cambio con valor para la memoria: una decisión de diseño, un
resultado con cifras, un cambio de tecnología, un hallazgo o error relevante, una limitación o una
lección aprendida. No anotes tareas triviales (renombrados, formato, arreglos menores).

**Cómo anotar:**
- **El estado actual se sobrescribe.** Cada capítulo describe el sistema tal como es hoy.
- **La historia se conserva.** Si el cambio sustituye algo (motor de base de datos, modelo, servicio,
  librería clave), añade además una fila en **§2 Evolución de decisiones** con fecha, *de → a* y
  motivo. No borres filas antiguas.
- Añade el hito a **§12 Bitácora** indicando quién lo hizo.
- Registra alternativas descartadas y por qué: es lo que más valor tiene en una memoria.
- Solo cifras verificadas (salidas reales, código, consultas). Las estimaciones, marcadas como tales;
  lo no verificado, con `[por confirmar]`.
- **Nunca** anotes secretos ni identificadores sensibles (contraseñas, claves de acceso, ID de cuenta
  de AWS): el repositorio está en GitHub.
- Sin relleno: frases cortas y datos concretos.
- Actualiza las notas **en la misma rama y PR** que el cambio que describen.
- Al terminar, di a la persona en una o dos líneas qué has anotado.

Las convenciones de formato están en la §0 del propio fichero.

## 2. AWS

La cuenta es compartida y la proporciona el máster (región `eu-west-1`).

- **No crees, modifiques, borres ni subas nada en AWS** sin que la persona lo indique explícitamente
  para esa acción concreta. Las consultas de solo lectura están permitidas.
- Explica qué hace cada comando antes de proponerlo: el equipo está aprendiendo AWS.

## 3. Repositorio

- Flujo de ramas: `feature/*` → PR a `development` → PR a `main`. `main` solo acepta PR desde
  `development`.
- Tests unitarios: `pytest tests/ --ignore=tests/test_integracion_db.py -v`
- No subas `.env`, credenciales ni ficheros de datos grandes (revisa `.gitignore`).
- Mensajes de commit en español con prefijo: `feat:`, `fix:`, `docs:`…
