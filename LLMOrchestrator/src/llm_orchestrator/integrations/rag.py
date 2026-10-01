"""Cliente HTTP del servicio de evidencias de la Fase 2 (`rag.api`).

El RAG corre como servicio aparte (`python -m rag.api`, puerto 8010 por
defecto) y este módulo consume su contrato (`src/rag/README.md`, «Contrato
para la API de chat»). Así el orquestador no duplica el trabajo del RAG ni
arrastra sus dependencias (torch + sentence-transformers + chromadb): este
servicio queda ligero y el RAG se despliega, escala y calienta por su cuenta.

    obtener_herramienta   GET /rag/herramienta → definición de la function tool
    recuperar_evidencias  paso 1: POST /rag/evidencias → D1..Dn bajo el umbral
    validar_evidencias    paso 3: POST /rag/validar   → citas comprobadas + texto

Configuración (variables de entorno, ver `.env.example`):
    RAG_URL        base del servicio, p. ej. http://localhost:8010 (obligatoria)
    RAG_TIMEOUT_S  timeout por petición (por defecto 60: la primera llamada al
                   servicio recién arrancado carga el modelo y puede tardar)
"""
import os

import httpx

TIMEOUT_POR_DEFECTO = 60.0


def _base_url() -> str:
    base = (os.getenv("RAG_URL") or "").rstrip("/")
    if not base:
        raise RuntimeError(
            "RAG_URL no está configurada (p. ej. http://localhost:8010; "
            "arranca el servicio con `python -m rag.api` desde la raíz del repo)"
        )
    return base


def _peticion(metodo: str, ruta: str, cuerpo: dict | None,
              transporte: httpx.BaseTransport | None = None) -> dict:
    """Petición al servicio RAG. Errores → RuntimeError con el detalle del servicio
    (la tool y el agente los convierten en {'error': ...} para el modelo)."""
    timeout = float(os.getenv("RAG_TIMEOUT_S") or TIMEOUT_POR_DEFECTO)
    with httpx.Client(timeout=timeout, transport=transporte) as cliente:
        respuesta = cliente.request(metodo, f"{_base_url()}{ruta}", json=cuerpo)
    if respuesta.status_code >= 400:
        raise RuntimeError(f"El servicio RAG respondió {respuesta.status_code}: "
                           f"{_detalle(respuesta)}")
    return respuesta.json()


def _post(ruta: str, cuerpo: dict, transporte: httpx.BaseTransport | None = None) -> dict:
    return _peticion("POST", ruta, cuerpo, transporte)


def _get(ruta: str, transporte: httpx.BaseTransport | None = None) -> dict:
    return _peticion("GET", ruta, None, transporte)


def _detalle(respuesta: httpx.Response) -> str:
    """El error tipado del RAG ({error, detalle}) o, si no, el cuerpo crudo."""
    try:
        cuerpo = respuesta.json()
        return f"{cuerpo['error']}: {cuerpo['detalle']}"
    except Exception:
        return respuesta.text[:200]


def obtener_herramienta() -> dict:
    """GET /rag/herramienta: la definición de la function tool que publica el RAG
    (formato OpenAI). El servicio es la fuente de verdad del esquema; la copia
    local de `tools/rag_tool.py` queda como respaldo si este GET falla."""
    return _get("/rag/herramienta")["herramienta"]


def recuperar_evidencias(pregunta: str, tema: str | None = None) -> dict:
    """Paso 1 del contrato: fragmentos D1..Dn bajo el umbral, listos para el modelo.

    Devuelve un dict plano para que el agente no dependa de la respuesta HTTP:
        estado           "con_evidencias" | "sin_evidencia"
        para_el_modelo   la carga mínima que se entrega como resultado de la tool
        refs             [{id, chunk_id, titulo}] para la validación del paso 3
    """
    d = _post("/rag/evidencias", {"pregunta": pregunta, "tema": tema})
    return {
        "estado": d["estado"],
        "para_el_modelo": d["para_el_modelo"],
        "refs": [{"id": e["id"], "chunk_id": e["chunk_id"], "titulo": e["titulo"]}
                 for e in d["evidencias"]],
    }


def validar_evidencias(pregunta: str, refs: list[dict], salida) -> dict:
    """Paso 3 del contrato: valida la salida del modelo y renderiza el texto final.

    Título, sección y bibliografía los resuelve el servicio desde el corpus a
    partir del chunk_id (nunca de lo que diga el modelo). Devuelve:
        valida               bool
        estado               el `estado` declarado por el modelo (si es válido)
        texto                respuesta final con [Dn], avisos y bibliografía
        errores              qué incumple la salida (si no es válida)
        mensaje_reparacion   turno de usuario listo para reenviar al modelo
        documentos_citados   títulos de los documentos realmente citados
    """
    d = _post("/rag/validar", {
        "pregunta": pregunta,
        "evidencias": [{"id": r["id"], "chunk_id": r["chunk_id"]} for r in refs],
        "salida": salida,
    })
    return {
        "valida": d["valida"],
        "estado": d["estado"],
        "texto": d["texto"],
        "errores": d["errores"],
        "mensaje_reparacion": d["mensaje_reparacion"],
        "documentos_citados": [ref["titulo"] for ref in d["bibliografia"]],
    }
