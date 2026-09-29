"""El bucle del agente: pregunta → LLM → tools → respuesta con fuentes.

En cada vuelta el LLM decide si llama a una tool (`query_sql`,
`search_documents`) o responde. Los resultados de las tools vuelven al
historial y el bucle sigue hasta la respuesta final o hasta `max_iteraciones`
(entonces se fuerza una última llamada sin tools para cerrar con lo que haya).

Los tiers gratuitos limitan peticiones/minuto y cada vuelta es una llamada al
LLM: por eso `max_iteraciones` es bajo (4 por defecto) y el cliente reintenta
con backoff los 429 (ver `integrations/llm_client.py`).
"""
import json
import logging

from sqlalchemy.engine import Engine

from llm_orchestrator.business.prompt_sistema import construir_prompt_sistema
from llm_orchestrator.entities.chat import FuenteChat, RespuestaChat
from llm_orchestrator.tools.registro import ejecutar_tool, esquemas_tools
from llm_orchestrator.tools.sql_tool import TABLA_POR_CONSULTA

logger = logging.getLogger("llm_orchestrator")

AVISO_MEDICO = "Demo académica: esta respuesta no constituye consejo médico."
RESPUESTA_VACIA = ("No he podido completar la consulta. Prueba a reformular la "
                   "pregunta o a concretar la estación y el contaminante.")

# Tope de fuentes citadas en la respuesta (evita listas interminables)
MAX_FUENTES = 6


class ErrorLLM(Exception):
    """El proveedor del LLM falló (agotados los reintentos del cliente)."""


def _serializar_tool_calls(tool_calls) -> list[dict]:
    """Vuelca las tool_calls del SDK a dicts planos para reenviarlas al historial."""
    return [
        {"id": tc.id, "type": "function",
         "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
        for tc in tool_calls
    ]


def _registrar_fuentes(nombre: str, args: dict, resultado: dict, fuentes: list[FuenteChat]) -> None:
    """Convierte un resultado de tool con éxito en fuentes citables (sin duplicar)."""
    if "error" in resultado:
        return
    nuevas: list[FuenteChat] = []
    if nombre == "query_sql":
        tabla = TABLA_POR_CONSULTA.get(args.get("consulta", ""), "resumen_datos_ml")
        nuevas.append(FuenteChat(tipo="sql", referencia=tabla))
    elif nombre == "search_documents":
        nuevas.extend(FuenteChat(tipo="documento", referencia=r["titulo"])
                      for r in resultado.get("resultados", []))
    for fuente in nuevas:
        if fuente not in fuentes and len(fuentes) < MAX_FUENTES:
            fuentes.append(fuente)


def _llamar_llm(cliente, modelo: str, mensajes: list[dict], con_tools: bool):
    """Llama al LLM y devuelve la primera choice. Todo fallo del proveedor -> ErrorLLM.

    En la llamada de cierre no se envían `tools` ni `tool_choice`: varios
    proveedores OpenAI-compatibles rechazan `tool_choice` sin `tools`.
    """
    kwargs: dict = {"model": modelo, "messages": mensajes, "temperature": 0.2}
    if con_tools:
        kwargs["tools"] = esquemas_tools()
        kwargs["tool_choice"] = "auto"
    try:
        respuesta = cliente.chat.completions.create(**kwargs)
        if not respuesta.choices:  # algunos gateways devuelven 200 con choices vacío
            raise ErrorLLM("El proveedor del LLM devolvió una respuesta sin contenido")
        return respuesta.choices[0]
    except ErrorLLM:
        raise
    except Exception as exc:  # APIError, timeout, conexión... el SDK ya reintentó
        logger.exception("Fallo llamando al proveedor del LLM")
        raise ErrorLLM(f"El proveedor del LLM falló: {exc}") from exc


def responder(pregunta: str, cliente, engine: Engine, modelo: str,
              max_iteraciones: int = 4) -> RespuestaChat:
    """Ejecuta el bucle del agente y devuelve la respuesta con sus fuentes."""
    mensajes: list[dict] = [
        {"role": "system", "content": construir_prompt_sistema()},
        {"role": "user", "content": pregunta},
    ]
    fuentes: list[FuenteChat] = []
    uso_documentos = False

    for _ in range(max_iteraciones):
        eleccion = _llamar_llm(cliente, modelo, mensajes, con_tools=True)
        mensaje = eleccion.message

        if not mensaje.tool_calls:
            return _cerrar(mensaje.content, fuentes, uso_documentos)

        mensajes.append({"role": "assistant", "content": mensaje.content or "",
                         "tool_calls": _serializar_tool_calls(mensaje.tool_calls)})
        for tc in mensaje.tool_calls:
            nombre = tc.function.name
            try:
                args = json.loads(tc.function.arguments or "{}")
                if not isinstance(args, dict):
                    args = {}
            except ValueError:
                args = {}
            resultado = ejecutar_tool(nombre, args, engine)
            _registrar_fuentes(nombre, args, resultado, fuentes)
            uso_documentos |= nombre == "search_documents" and "error" not in resultado
            mensajes.append({"role": "tool", "tool_call_id": tc.id,
                             "content": json.dumps(resultado, ensure_ascii=False, default=str)})

    # Iteraciones agotadas: una última llamada sin tools para cerrar la respuesta
    mensajes.append({"role": "user", "content":
                     "Responde ahora con la información que ya tienes, sin usar más tools."})
    eleccion = _llamar_llm(cliente, modelo, mensajes, con_tools=False)
    return _cerrar(eleccion.message.content, fuentes, uso_documentos)


def _cerrar(contenido: str | None, fuentes: list[FuenteChat], uso_documentos: bool) -> RespuestaChat:
    respuesta = (contenido or "").strip() or RESPUESTA_VACIA
    return RespuestaChat(
        respuesta=respuesta,
        fuentes=fuentes,
        advertencia=AVISO_MEDICO if uso_documentos else None,
    )
