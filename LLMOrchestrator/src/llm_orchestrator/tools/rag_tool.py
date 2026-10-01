"""Tool `buscar_evidencias`: paso 1 del contrato de evidencias de la Fase 2.

Llama por HTTP al servicio RAG (`POST /rag/evidencias`, vía
`integrations/rag.py`) validando antes los argumentos que genera el LLM. El
umbral de relevancia y la numeración D1..Dn son los del servicio: esta tool no
filtra por su cuenta. La validación de las citas del modelo (paso 3) la hace
el agente al cerrar la respuesta, no esta tool.

La definición de la tool la publica el propio RAG (`GET /rag/herramienta`),
única fuente de verdad del esquema: `esquema_tool()` la pide la primera vez y
la cachea para el resto del proceso. Si el GET falla, el registro no ofrece la
tool en esa vuelta (el agente sigue solo con `query_sql`) y se reintenta en la
siguiente: no hay copia local que pueda divergir del servicio.
"""
from llm_orchestrator.integrations import rag

# Temas del corpus. Duplicado a propósito de `rag.corpus.TEMAS`: el RAG es otro
# servicio y el contrato es la costura, no un paquete común.
TEMAS = ("salud", "normativa", "proyecto")

_esquema_cacheado: dict | None = None


def esquema_tool() -> dict:
    """La definición de la tool (GET /rag/herramienta), cacheada por proceso.

    Lanza si el servicio no responde o si publica una tool con otro nombre,
    que `registro` no sabría despachar: quien llama decide cómo degradar.
    """
    global _esquema_cacheado
    if _esquema_cacheado is None:
        esquema = rag.obtener_herramienta()
        nombre = esquema.get("function", {}).get("name")
        if nombre != "buscar_evidencias":
            raise RuntimeError("GET /rag/herramienta publica la tool "
                               f"{nombre!r}, no 'buscar_evidencias'")
        _esquema_cacheado = esquema
    return _esquema_cacheado


def buscar_evidencias(pregunta: str = "", tema: str | None = None) -> dict:
    """Recupera evidencias citables. Errores como {'error': ...}, nunca excepción."""
    if not isinstance(pregunta, str) or not pregunta.strip():
        return {"error": "'pregunta' no puede estar vacía"}
    if tema is not None:
        tema = str(tema).strip() or None  # espacios accidentales del LLM fuera
        if tema is not None and tema not in TEMAS:
            return {"error": f"'tema' debe ser uno de {list(TEMAS)}, no {tema!r}"}

    try:
        return rag.recuperar_evidencias(pregunta.strip(), tema=tema)
    except Exception as exc:  # índice ausente, modelo no descargado...
        return {"error": f"La búsqueda documental falló: {exc}"}
