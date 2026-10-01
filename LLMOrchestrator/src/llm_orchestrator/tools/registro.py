"""Registro y despacho de las tools del agente.

`ejecutar_tool` nunca lanza excepciones: los fallos vuelven como
`{"error": ...}` para que el LLM pueda leerlos y corregirse en la
siguiente vuelta del bucle (argumento mal formado, filtro imposible...).
"""
import logging

from sqlalchemy.engine import Engine

from llm_orchestrator.tools import rag_tool, sql_tool

logger = logging.getLogger("llm_orchestrator")


def esquemas_tools() -> list[dict]:
    """Esquemas en formato function-calling OpenAI, para el parámetro `tools`.

    El de `buscar_evidencias` lo publica el servicio RAG (GET /rag/herramienta,
    cacheado por proceso). Si el RAG no está disponible, la tool no se ofrece
    en esta vuelta — el agente sigue solo con `query_sql` — y el GET se
    reintenta en la siguiente."""
    esquemas = [sql_tool.ESQUEMA_TOOL]
    try:
        esquemas.append(rag_tool.esquema_tool())
    except Exception as exc:
        logger.warning("Sin la tool buscar_evidencias en esta vuelta: %s", exc)
    return esquemas


def ejecutar_tool(nombre: str, args: dict, engine: Engine) -> dict:
    try:
        if nombre == "query_sql":
            return sql_tool.query_sql(engine, **args)
        if nombre == "buscar_evidencias":
            return rag_tool.buscar_evidencias(**args)
        return {"error": f"Tool desconocida: {nombre!r}"}
    except TypeError as exc:  # argumentos inesperados generados por el LLM
        return {"error": f"Argumentos inválidos para {nombre}: {exc}"}
    except Exception as exc:  # red de seguridad: el bucle no debe romperse
        return {"error": f"La tool {nombre} falló: {exc}"}
