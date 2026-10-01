"""El bucle del agente: pregunta → LLM → tools → respuesta con fuentes.

En cada vuelta el LLM decide si llama a una tool (`query_sql`,
`buscar_evidencias`) o responde. Los resultados de las tools vuelven al
historial y el bucle sigue hasta la respuesta final o hasta `max_iteraciones`
(entonces se fuerza una última llamada sin tools para cerrar con lo que haya).

Ruta documental (contrato de evidencias de la Fase 2, `src/rag/README.md`):
si `buscar_evidencias` devolvió evidencias, la respuesta final del modelo debe
ser el JSON {estado, afirmaciones[{texto, evidencias:[Dn]}], limitaciones}. El
agente la valida con el servicio de evidencias (las citas se comprueban contra
el corpus, no se creen) y renderiza el texto con [Dn], avisos y bibliografía.
Si la salida no cumple el contrato se reenvía una única reparación, como
propone el plan de la Fase 2. La ruta de datos (solo `query_sql`) sigue siendo
texto libre: las cifras ya son deterministas por construcción.

Los tiers gratuitos limitan peticiones/minuto y cada vuelta es una llamada al
LLM: por eso `max_iteraciones` es bajo (4 por defecto) y el cliente reintenta
con backoff los 429 (ver `integrations/llm_client.py`). Peor caso de llamadas:
`max_iteraciones` + cierre + reparación + cierre de datos = max_iteraciones + 3.
"""
import json
import logging
import re

from sqlalchemy.engine import Engine

from llm_orchestrator.business.prompt_sistema import construir_prompt_sistema
from llm_orchestrator.entities.chat import FuenteChat, RespuestaChat
from llm_orchestrator.integrations import rag
from llm_orchestrator.tools.registro import ejecutar_tool, esquemas_tools
from llm_orchestrator.tools.sql_tool import TABLA_POR_CONSULTA

logger = logging.getLogger("llm_orchestrator")

AVISO_MEDICO = "Demo académica: esta respuesta no constituye consejo médico."
RESPUESTA_VACIA = ("No he podido completar la consulta. Prueba a reformular la "
                   "pregunta o a concretar la estación y el contaminante.")
RESPUESTA_SIN_FUNDAMENTO = (
    "No he podido fundamentar la respuesta en la documentación del asistente: "
    "el modelo no produjo una salida válida según el contrato de citas. Prueba "
    "a reformular la pregunta.")

# Recordatorio que acompaña a cada resultado de `buscar_evidencias`: se paga
# solo cuando se usa la tool, a diferencia del prompt de sistema.
INSTRUCCION_SALIDA_JSON = (
    "Cuando termines de consultar tools, tu respuesta final debe ser ÚNICAMENTE "
    'el objeto JSON {"estado": "respondida" | "parcial" | "sin_evidencia", '
    '"afirmaciones": [{"texto": "...", "evidencias": ["D1"]}], '
    '"limitaciones": ["..."]}. Cada afirmación cita los IDs de las evidencias '
    "que la respaldan. Sin texto fuera del JSON.")

CIERRE_LIBRE = "Responde ahora con la información que ya tienes, sin usar más tools."
CIERRE_DOCUMENTAL = ("Responde ahora, sin usar más tools, con el objeto JSON del "
                     "contrato (estado, afirmaciones con sus evidencias D*, "
                     "limitaciones).")
CIERRE_SOLO_DATOS = ("Las evidencias documentales no respaldan la respuesta. "
                     "Responde en texto plano usando solo los datos que ya "
                     "obtuviste de query_sql.")

# Tope de fuentes citadas en la respuesta (evita listas interminables)
MAX_FUENTES = 6

_RE_FENCE = re.compile(r"^```[a-zA-Z]*\s*(.*?)\s*```$", re.DOTALL)


class ErrorLLM(Exception):
    """El proveedor del LLM falló (agotados los reintentos del cliente)."""


def _serializar_tool_calls(tool_calls) -> list[dict]:
    """Vuelca las tool_calls del SDK a dicts planos para reenviarlas al historial."""
    return [
        {"id": tc.id, "type": "function",
         "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
        for tc in tool_calls
    ]


def _registrar_fuente_sql(args: dict, resultado: dict, fuentes: list[FuenteChat]) -> None:
    """Solo query_sql: las fuentes documentales salen de la validación del paso 3,
    que conoce los documentos realmente citados (no solo los recuperados)."""
    if "error" in resultado:
        return
    tabla = TABLA_POR_CONSULTA.get(args.get("consulta", ""), "resumen_datos_ml")
    fuente = FuenteChat(tipo="sql", referencia=tabla)
    if fuente not in fuentes and len(fuentes) < MAX_FUENTES:
        fuentes.append(fuente)


def _renumerar(resultado: dict, desplazamiento: int) -> dict:
    """D1..Dn de esta llamada → D{desplazamiento+1}..: si el modelo llama a
    `buscar_evidencias` más de una vez, los IDs deben ser únicos en toda la
    conversación para que la validación no sea ambigua."""
    if not desplazamiento:
        return resultado
    mapa: dict[str, str] = {}
    for ref in resultado["refs"]:
        nuevo = f"D{int(ref['id'][1:]) + desplazamiento}"
        mapa[ref["id"]] = nuevo
        ref["id"] = nuevo
    for evidencia in resultado["para_el_modelo"]["evidencias"]:
        evidencia["id"] = mapa.get(evidencia["id"], evidencia["id"])
    return resultado


def _parsear_salida(contenido: str | None):
    """El JSON de la respuesta final del modelo, tolerando vallas ```json y texto
    alrededor. Si no hay JSON, devuelve el texto tal cual: la validación lo
    rechazará con un mensaje de reparación."""
    texto = (contenido or "").strip()
    m = _RE_FENCE.match(texto)
    if m:
        texto = m.group(1)
    try:
        return json.loads(texto)
    except ValueError:
        pass
    inicio, fin = texto.find("{"), texto.rfind("}")
    if 0 <= inicio < fin:
        try:
            return json.loads(texto[inicio:fin + 1])
        except ValueError:
            pass
    return texto


def _validar_documental(pregunta: str, refs: list[dict], salida) -> dict | None:
    """Paso 3 del contrato. None si la validación en sí falla (servicio RAG caído,
    corpus ilegible...): es un fallo nuestro, no del modelo, sin reparación posible."""
    try:
        return rag.validar_evidencias(pregunta, refs, salida)
    except Exception:
        logger.exception("La validación documental falló")
        return None


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
    refs_documentales: list[dict] = []   # {id, chunk_id, titulo} acumulados y renumerados
    hay_evidencias = False

    for _ in range(max_iteraciones):
        eleccion = _llamar_llm(cliente, modelo, mensajes, con_tools=True)
        mensaje = eleccion.message

        if not mensaje.tool_calls:
            if hay_evidencias:
                return _cerrar_documental(pregunta, mensaje.content, mensajes,
                                          cliente, modelo, refs_documentales, fuentes)
            return _cerrar(mensaje.content, fuentes)

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
            if nombre == "buscar_evidencias" and "error" not in resultado:
                resultado = _renumerar(resultado, len(refs_documentales))
                refs_documentales.extend(resultado["refs"])
                hay_evidencias |= resultado["estado"] == "con_evidencias"
                contenido = {**resultado["para_el_modelo"],
                             "instrucciones": INSTRUCCION_SALIDA_JSON}
            else:
                if nombre == "query_sql":
                    _registrar_fuente_sql(args, resultado, fuentes)
                contenido = resultado
            mensajes.append({"role": "tool", "tool_call_id": tc.id,
                             "content": json.dumps(contenido, ensure_ascii=False, default=str)})

    # Iteraciones agotadas: una última llamada sin tools para cerrar la respuesta
    mensajes.append({"role": "user", "content":
                     CIERRE_DOCUMENTAL if hay_evidencias else CIERRE_LIBRE})
    eleccion = _llamar_llm(cliente, modelo, mensajes, con_tools=False)
    if hay_evidencias:
        return _cerrar_documental(pregunta, eleccion.message.content, mensajes,
                                  cliente, modelo, refs_documentales, fuentes)
    return _cerrar(eleccion.message.content, fuentes)


def _cerrar(contenido: str | None, fuentes: list[FuenteChat],
            advertencia: str | None = None) -> RespuestaChat:
    respuesta = (contenido or "").strip() or RESPUESTA_VACIA
    return RespuestaChat(respuesta=respuesta, fuentes=fuentes, advertencia=advertencia)


def _cerrar_documental(pregunta: str, contenido: str | None, mensajes: list[dict],
                       cliente, modelo: str, refs: list[dict],
                       fuentes: list[FuenteChat]) -> RespuestaChat:
    """Pasos 2 y 3 del contrato: parsear la salida del modelo, validarla contra el
    corpus y renderizar. Una única reparación, como propone la Fase 2."""
    validacion = _validar_documental(pregunta, refs, _parsear_salida(contenido))
    if validacion is None:
        return _cerrar(None, fuentes)

    if not validacion["valida"]:
        mensajes.append({"role": "assistant", "content": contenido or ""})
        mensajes.append({"role": "user", "content": validacion["mensaje_reparacion"]})
        eleccion = _llamar_llm(cliente, modelo, mensajes, con_tools=False)
        contenido = eleccion.message.content
        validacion = _validar_documental(pregunta, refs, _parsear_salida(contenido))
        if validacion is None or not validacion["valida"]:
            if validacion is not None:
                logger.warning("Salida documental inválida tras la reparación: %s",
                               validacion["errores"])
            return _cerrar(RESPUESTA_SIN_FUNDAMENTO, fuentes)

    if validacion["estado"] == "sin_evidencia" and any(f.tipo == "sql" for f in fuentes):
        # El modelo consultó documentos pero no respaldan nada, y sí hay datos de
        # query_sql: se cierra por la ruta de datos en vez de declarar insuficiencia.
        mensajes.append({"role": "assistant", "content": contenido or ""})
        mensajes.append({"role": "user", "content": CIERRE_SOLO_DATOS})
        eleccion = _llamar_llm(cliente, modelo, mensajes, con_tools=False)
        return _cerrar(eleccion.message.content, fuentes)

    documentales = [FuenteChat(tipo="documento", referencia=titulo)
                    for titulo in validacion["documentos_citados"]]
    finales = (fuentes + [f for f in documentales if f not in fuentes])[:MAX_FUENTES]
    advertencia = AVISO_MEDICO if documentales else None
    return _cerrar(validacion["texto"], finales, advertencia)
