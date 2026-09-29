"""Registro y despacho de las tools del agente.

`ejecutar_tool` nunca lanza excepciones: los fallos vuelven como
`{"error": ...}` para que el LLM pueda leerlos y corregirse en la
siguiente vuelta del bucle (argumento mal formado, filtro imposible...).
"""
from sqlalchemy.engine import Engine

from llm_orchestrator.tools import rag_tool, sql_tool


def esquemas_tools() -> list[dict]:
    """Esquemas en formato function-calling OpenAI, para el parámetro `tools`."""
    return [sql_tool.ESQUEMA_TOOL, rag_tool.ESQUEMA_TOOL]


def ejecutar_tool(nombre: str, args: dict, engine: Engine) -> dict:
    try:
        if nombre == "query_sql":
            return sql_tool.query_sql(engine, **args)
        if nombre == "search_documents":
            return rag_tool.search_documents(**args)
        return {"error": f"Tool desconocida: {nombre!r}"}
    except TypeError as exc:  # argumentos inesperados generados por el LLM
        return {"error": f"Argumentos inválidos para {nombre}: {exc}"}
    except Exception as exc:  # red de seguridad: el bucle no debe romperse
        return {"error": f"La tool {nombre} falló: {exc}"}
