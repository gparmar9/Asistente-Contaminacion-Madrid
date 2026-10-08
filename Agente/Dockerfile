# Imagen del agente LLM (servicio Agente, puerto 8200).
#
# Construir desde ESTA carpeta (el servicio es autocontenido):
#   docker build -t jupiter-agente Agente
# Configuración por variables de entorno (ver .env.example): LLM_PROVEEDOR, RAG_URL...
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencias antes que el código: si solo cambia el código, Docker reutiliza esta capa.
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src/ src/

# Sin privilegios de root dentro del contenedor.
RUN useradd --create-home --uid 10001 app
USER app

EXPOSE 8200

# python:slim no trae curl: la comprobación de salud usa la librería estándar.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8200/salud', timeout=4)"

CMD ["uvicorn", "agente.main:app", "--app-dir", "src", "--host", "0.0.0.0", "--port", "8200"]
