"""El turno del agente: bucle de herramientas escrito a mano sobre el cliente LLM de LlamaIndex.

    pregunta
      -> CLASIFICAR (intencion.py): una o dos intenciones y tema; si falla, DESCONOCIDA
      -> DECIDIR (código): frase fija y fin, o qué herramientas se ofrecen y con qué prompt
      -> vuelta 1..MAX: el modelo pide herramientas -> el código las ejecuta y añade el mensaje `tool`
      -> el modelo deja de pedir herramientas (o se agotan las vueltas) y:
           búsqueda obligada sin hacer -> la hace el código con la pregunta y el tema
                                          (salvo que la del modelo encontrara el RAG caído: frase fija)
           consulta de datos obligada sin hacer -> la hace el código con la pregunta; sin filas
                                          (base caída, SQL sin resolver o periodo fuera de las
                                          mediciones): frase fija
           hubo evidencias y datos     -> ruta mixta: bloque de datos + bloque documental (compuesta)
           hubo evidencias del RAG     -> ruta documental (sintesis.py): JSON validado y passthrough
           hubo resultados de datos    -> ruta de datos: síntesis sin herramientas con las filas
           no hubo ni unas ni otros    -> su texto es el final (o síntesis forzada sin herramientas)
      -> nota final si una de las dos intenciones era de frase fija (DATOS, PREDICCION)

En una mixta (DATOS, DOCUMENTAL), cada parte acaba con su contenido o con su frase fija (RAG o base
caídos, sin evidencias, sin filas): el fallo de una no anula la otra.

Intervención: si una búsqueda devuelve `sin_evidencia` y el turno no tiene evidencias ni una
parte de datos, el código cierra el turno con una frase fija sin volver a llamar al modelo.

Las fases de sesión y comprobaciones se añaden encima de este esqueleto sin cambiar su forma.

Cada paso deja un span (ver `agente.observabilidad`): turno > clasificar | bucle |
busqueda_forzada | consulta_forzada | sintesis_documental | sintesis_datos | sintesis_forzada >
llm y herramientas.

Las herramientas ofrecidas pueden añadir contexto al prompt del sistema (`contexto_prompt()`):
la de datos añade la fecha de hoy y el rango con mediciones, para resolver «el mes pasado».

Con historial (los turnos previos de la sesión que caben en la ventana), el contexto llega a todas
las llamadas: al clasificador y a las síntesis como bloque de texto, al bucle como pares
`user`/`assistant`, y a la búsqueda que lanza el código como preguntas concatenadas. `/rag/validar`
sigue recibiendo solo la pregunta actual. Sin historial, los mensajes son los de siempre.

Con un emisor (stream), el turno avisa de cada fase y entrega como tokens solo las respuestas
definitivas: la llamada del bucle sin herramientas ofrecidas, la síntesis de datos (salvo en la
ruta mixta) y la síntesis forzada. El resto (frases fijas, rutas documental y mixta, texto libre
tras ofrecer herramientas) sale entero al final.

Al final del turno, las comprobaciones posteriores (comprobaciones.py) revisan lo que escribió el
modelo: el texto libre o las afirmaciones y limitaciones de la ruta documental (las frases fijas
son del código y no se comprueban). Cada hallazgo es un evento `comprobacion` en el span `turno`.
Si una regla bloquea, la respuesta pasa a ser una frase fija, sin fuentes ni aviso, y es lo que
entra en el historial. Con alguna regla en bloqueo no se emiten tokens: la respuesta libre se
comprueba entera y sale al final como las demás.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Iterable, Sequence

from llama_index.core.base.llms.types import ChatMessage, ChatResponse, MessageRole
from llama_index.core.llms.function_calling import FunctionCallingLLM
from llama_index.core.llms.llm import ToolSelection
from llama_index.core.tools.types import BaseTool
from opentelemetry.trace import Status, StatusCode

from agente import observabilidad
from agente.business import comprobaciones, frases
from agente.business.intencion import Decision, clasificar, decidir
from agente.business.memoria import mensajes_historial, pregunta_contextual, preguntas_previas, texto_historial
from agente.business.sintesis import EvidenciasTurno, sintesis_documental
from agente.entities.chat import Fuente
from agente.entities.comprobaciones import Hallazgo, MaterialTurno
from agente.entities.eventos import Emisor, Evento, EventoEstado, EventoTexto, Fase
from agente.entities.intencion import DESCONOCIDA, Clasificacion, Tema
from agente.entities.memoria import TurnoGuardado
from agente.tools import rag, sql_libre
from agente.tools.base import Herramienta, ResultadoHerramienta, como_llamaindex

logger = logging.getLogger("agente.bucle")

ID_BUSQUEDA_FORZADA = "busqueda_forzada"  # tool_id de la búsqueda que lanza el código
ID_CONSULTA_FORZADA = "consulta_forzada"  # tool_id de la consulta de datos que lanza el código


class LLMNoDisponible(RuntimeError):
    """El proveedor del LLM falló (red, cuota, error interno)."""


@dataclass
class ResultadoTurno:
    respuesta: str
    fuentes: list[Fuente] = field(default_factory=list)
    advertencia: str | None = None
    intencion: str = DESCONOCIDA.etiqueta                # una o dos: «DATOS, DOCUMENTAL»
    tema: str = DESCONOCIDA.tema.value
    vueltas: int = 0                                   # llamadas al LLM con herramientas ofrecidas
    herramientas_usadas: list[str] = field(default_factory=list)
    busqueda_forzada: bool = False                     # la búsqueda obligada la lanzó el código
    consulta_forzada: bool = False                     # la consulta de datos obligada la lanzó el código
    sintesis_forzada: bool = False
    ruta: str = "libre"                                # libre | documental | datos | mixta | sin_evidencia | fija
    valida: bool | None = None                         # ruta documental: /rag/validar la aceptó
    reparaciones: int = 0                              # ruta documental: reintentos tras validar
    traza_id: str | None = None
    emitida: bool = False                              # la respuesta ya salió como tokens por el emisor
    respuesta_contexto: str = ""                       # la que entra en el historial; documental: sin [Dn]
    hallazgos: list[Hallazgo] = field(default_factory=list)  # comprobaciones posteriores que saltaron
    bloqueada: bool = False                            # una regla que bloquea sustituyó la respuesta
    material: MaterialTurno | None = field(default=None, repr=False)  # lo que escribió el modelo


class Bucle:
    """`llm_clasificador` None = sin clasificador: todos los turnos son DESCONOCIDA.
    `comprobaciones_bloquean`: reglas que sustituyen la respuesta; las demás solo observan."""

    def __init__(self, llm: FunctionCallingLLM, herramientas: Sequence[Herramienta],
                 max_vueltas: int = 3, llm_clasificador: FunctionCallingLLM | None = None,
                 clasificador_timeout_s: float = 10.0, comprobaciones_bloquean: Iterable[str] = ()):
        self._llm = llm
        self._herramientas = {h.nombre: h for h in herramientas}
        self._max_vueltas = max(1, max_vueltas)
        self._llm_clasificador = llm_clasificador
        self._clasificador_timeout_s = clasificador_timeout_s
        bloquean = frozenset(comprobaciones_bloquean)
        if desconocidas := bloquean - set(comprobaciones.REGLAS):
            logger.warning("COMPROBACIONES_BLOQUEAN: se ignoran las reglas desconocidas %s", sorted(desconocidas))
        self._bloquean = bloquean & set(comprobaciones.REGLAS)

    async def responder(self, pregunta: str, emitir: Emisor | None = None,
                        historial: Sequence[TurnoGuardado] = ()) -> ResultadoTurno:
        """`emitir`: recibe los eventos del turno (stream). Es por turno, nunca del `Bucle`:
        hay turnos concurrentes. None = sin eventos, como en `/responder`.
        `historial`: turnos previos de la sesión, ya recortados por la ventana. El `Bucle` no
        conoce el almacén."""
        historial = list(historial)
        with observabilidad.turno(pregunta) as span:
            span.set_attribute("agente.historial_turnos", len(historial))
            resultado = ResultadoTurno(respuesta="", traza_id=observabilidad.trace_id_actual())
            # Con alguna regla en bloqueo, nada sale como token antes de comprobarlo.
            emitir_texto = None if self._bloquean else _texto_al_cliente(emitir, resultado)
            try:
                await self._turno(pregunta, historial, resultado, emitir or _no_emitir, emitir_texto)
            except asyncio.CancelledError:  # el cliente del stream se fue: no se hacen más llamadas
                span.set_attribute("agente.cancelado", True)
                raise
            self._comprobar(resultado)
            resultado.emitida &= resultado.respuesta != frases.RESPUESTA_VACIA  # vacía: sale entera
            resultado.respuesta_contexto = resultado.respuesta_contexto or resultado.respuesta
            span.set_output(resultado.respuesta)
            span.set_attributes(_atributos_turno(resultado))
            return resultado

    async def _turno(self, pregunta: str, historial: list[TurnoGuardado], resultado: ResultadoTurno,
                     emitir: Emisor, emitir_texto: Emisor | None) -> ResultadoTurno:
        clasificacion = await self._clasificar(pregunta, historial, emitir)
        decision = decidir(clasificacion.intenciones)
        resultado.intencion = clasificacion.etiqueta
        resultado.tema = clasificacion.tema.value
        if decision.frase:
            observabilidad.decision(f"frase_fija:{resultado.intencion}")
            return _fija(resultado, decision.frase)
        await self._resolver(pregunta, historial, clasificacion.tema, decision, resultado, emitir, emitir_texto)
        if decision.nota:
            await _anadir_nota(resultado, decision.nota, emitir_texto)
        return resultado

    async def _resolver(self, pregunta: str, historial: list[TurnoGuardado], tema: Tema, decision: Decision,
                        resultado: ResultadoTurno, emitir: Emisor, emitir_texto: Emisor | None) -> ResultadoTurno:
        """Cada parte obligada (documental, datos) acaba con su contenido o con su frase fija. En una
        mixta, el fallo de una parte no cierra el turno: la otra responde igual (cierre compuesto)."""
        disponibles = await self._herramientas_disponibles(decision)
        frase_doc: str | None = None    # la parte documental se resuelve con esta frase
        frase_datos: str | None = None  # la parte de datos se resuelve con esta frase
        if decision.busqueda_obligada and rag.NOMBRE not in disponibles:
            observabilidad.decision("documentacion_no_disponible")
            frase_doc = frases.DOCUMENTACION_NO_DISPONIBLE
        if decision.datos_obligados and sql_libre.NOMBRE not in disponibles:
            observabilidad.decision("datos_no_disponibles")
            frase_datos = frases.FRASE_DATOS_NO_DISPONIBLE
        queda_doc = decision.busqueda_obligada and frase_doc is None
        queda_datos = decision.datos_obligados and frase_datos is None
        if (frase_doc or frase_datos) and not (queda_doc or queda_datos):
            return _fija(resultado, "\n\n".join(f for f in (frase_datos, frase_doc) if f))

        mensajes = [
            ChatMessage(role=MessageRole.SYSTEM, content=self._prompt_sistema(decision.prompt, disponibles)),
            *mensajes_historial(historial),
            ChatMessage(role=MessageRole.USER, content=pregunta),
        ]
        evidencias = EvidenciasTurno()
        consultas: list[str] = []  # resultados en texto para la síntesis forzada
        consultas_datos: list[ResultadoHerramienta] = []  # resultados de consultar_datos
        final: ChatResponse | None = None  # respuesta del modelo sin peticiones de herramienta
        rag_caido = False  # una búsqueda del modelo encontró el RAG sin servicio
        sin_evidencia = False  # una búsqueda del modelo no encontró nada

        with observabilidad.span("bucle", "chain"):
            for _ in range(self._max_vueltas):
                resultado.vueltas += 1
                await emitir(EventoEstado(Fase.REDACTANDO))
                # Sin herramientas ofrecidas, el texto de esta llamada es la respuesta final (D1).
                respuesta = await self._llamar(mensajes, list(disponibles.values()),
                                               None if disponibles else emitir_texto)
                llamadas = self._llamadas_pedidas(respuesta) if disponibles else []
                if not llamadas:
                    final = respuesta
                    break
                mensajes.append(respuesta.message)
                hubo_sin_evidencia = False
                for llamada in llamadas:
                    if llamada.tool_name == sql_libre.NOMBRE:
                        llamada = _con_preguntas_usuario(llamada, pregunta, historial)
                    r, vacia = await self._ejecutar_y_anotar(llamada, disponibles, evidencias, resultado, emitir)
                    hubo_sin_evidencia |= vacia
                    if llamada.tool_name == rag.NOMBRE:
                        rag_caido |= bool(r.internos) and r.internos.get("estado") == rag.ESTADO_NO_DISPONIBLE
                    elif llamada.tool_name == sql_libre.NOMBRE:
                        consultas_datos.append(r)
                    mensajes.append(_mensaje_tool(llamada, r))
                    consultas.append(f"{llamada.tool_name}: {r.para_el_modelo()}")
                sin_evidencia |= hubo_sin_evidencia
                if hubo_sin_evidencia and not evidencias and not queda_datos and not consultas_datos:
                    observabilidad.decision("sin_evidencia")
                    return _sin_evidencia(resultado)
            if final is None:
                observabilidad.decision("limite_vueltas")

        if queda_doc and not evidencias:
            frase_doc = await self._parte_documental(pregunta, historial, tema, disponibles, evidencias,
                                                     resultado, emitir, rag_caido, sin_evidencia)

        if queda_datos and not any(r.internos for r in consultas_datos):
            # El modelo no consultó (o su petición no llegó a la herramienta): consulta el código.
            # Si su consulta falló dentro de la herramienta (base caída, SQL sin resolver), repetirla
            # costaría otros dos intentos del redactor para el mismo resultado.
            with observabilidad.span("consulta_forzada", "chain"):
                resultado.consulta_forzada = True
                llamada = _con_preguntas_usuario(ToolSelection(
                    tool_id=ID_CONSULTA_FORZADA, tool_name=sql_libre.NOMBRE,
                    tool_kwargs={"pregunta": pregunta_contextual(historial, pregunta)}), pregunta, historial)
                r, _ = await self._ejecutar_y_anotar(llamada, disponibles, evidencias, resultado, emitir)
                consultas_datos.append(r)
        datos = [r.para_el_modelo() for r in consultas_datos if r.ok]
        if queda_datos and not datos:
            # Periodo entero fuera de las mediciones: su frase («no hay mediciones de 2019...»).
            sin_datos = next((r.internos["frase"] for r in consultas_datos if r.internos
                              and r.internos.get("estado") == sql_libre.ESTADO_SIN_DATOS), None)
            observabilidad.decision("sin_datos_periodo" if sin_datos else "datos_no_disponibles")
            frase_datos = sin_datos or frases.FRASE_DATOS_NO_DISPONIBLE

        # El texto libre del modelo se descarta si hay contenido de alguna parte: lo sustituyen las síntesis.
        if (evidencias or frase_doc) and (datos or frase_datos):
            return await self._cerrar_mixta(pregunta, historial, evidencias, frase_doc, datos, frase_datos,
                                            resultado, emitir)
        if evidencias:
            return await self._cerrar_documental(pregunta, historial, evidencias, resultado, emitir)
        if frase_doc:
            return _sin_evidencia(resultado) if frase_doc == frases.SIN_EVIDENCIA else _fija(resultado, frase_doc)
        if datos:
            return await self._cerrar_datos(pregunta, historial, datos, resultado, emitir, emitir_texto)
        if frase_datos:
            return _fija(resultado, frase_datos)
        if final is not None:
            resultado.respuesta = _texto(final)
            resultado.material = comprobaciones.material_libre(pregunta, consultas, resultado.respuesta)
            return resultado
        # Límite de vueltas: cerrar sin herramientas para que el usuario siempre reciba respuesta.
        with observabilidad.span("sintesis_forzada", "chain"):
            await emitir(EventoEstado(Fase.REDACTANDO))
            respuesta = await self._llamar(
                _mensajes_sintesis(frases.PROMPT_SINTESIS_FORZADA, pregunta, consultas, historial), [], emitir_texto)
            resultado.respuesta = _texto(respuesta)
            resultado.sintesis_forzada = True
        resultado.material = comprobaciones.material_libre(pregunta, consultas, resultado.respuesta)
        return resultado

    # ------------------------------------------------------------------ pasos

    async def _parte_documental(self, pregunta: str, historial: list[TurnoGuardado], tema: Tema,
                                disponibles: dict[str, BaseTool], evidencias: EvidenciasTurno,
                                resultado: ResultadoTurno, emitir: Emisor, rag_caido: bool,
                                sin_evidencia: bool) -> str | None:
        """Búsqueda obligada sin evidencias tras el bucle. Devuelve la frase con la que se resuelve la
        parte documental, o None si la búsqueda del código trajo evidencias."""
        if rag_caido:  # repetir la búsqueda solo añadiría otra espera (hasta el timeout del RAG)
            observabilidad.decision("documentacion_no_disponible")
            return frases.DOCUMENTACION_NO_DISPONIBLE
        if sin_evidencia:  # la búsqueda del modelo no encontró nada (solo llega aquí en una mixta)
            observabilidad.decision("sin_evidencia")
            return frases.SIN_EVIDENCIA
        # El modelo no buscó (o su búsqueda falló por la petición): busca el código.
        with observabilidad.span("busqueda_forzada", "chain"):
            resultado.busqueda_forzada = True
            argumentos = {"pregunta": pregunta_contextual(historial, pregunta)}
            if tema is not Tema.NINGUNO:
                argumentos["tema"] = tema.value
            llamada = ToolSelection(tool_id=ID_BUSQUEDA_FORZADA, tool_name=rag.NOMBRE, tool_kwargs=argumentos)
            r, vacia = await self._ejecutar_y_anotar(llamada, disponibles, evidencias, resultado, emitir)
            if vacia:
                observabilidad.decision("sin_evidencia")
                return frases.SIN_EVIDENCIA
            if not r.ok:
                observabilidad.decision("documentacion_no_disponible")
                return frases.DOCUMENTACION_NO_DISPONIBLE
        return None

    async def _cerrar_mixta(self, pregunta: str, historial: list[TurnoGuardado], evidencias: EvidenciasTurno,
                            frase_doc: str | None, datos: list[str], frase_datos: str | None,
                            resultado: ResultadoTurno, emitir: Emisor) -> ResultadoTurno:
        """Respuesta compuesta: primero el bloque de datos, después el documental tal como lo
        renderiza el RAG. La síntesis documental no recibe las filas. La parte que falló va con su
        frase. No sale como tokens: el passthrough lleva además las fuentes y el aviso sanitario."""
        textos: list = []
        if datos:
            await self._cerrar_datos(pregunta, historial, datos, resultado, emitir, None, mixta=True)
            textos += resultado.material.textos
        bloque_datos = resultado.respuesta if datos else frase_datos
        ruta_material = "libre"
        if evidencias:
            resultado.material = None
            await self._cerrar_documental(pregunta, historial, evidencias, resultado, emitir)
            bloque_doc, contexto_doc = resultado.respuesta, resultado.respuesta_contexto or resultado.respuesta
            if resultado.material is not None:  # válida: las afirmaciones pueden llevar [Dn]
                textos += resultado.material.textos
                ruta_material = "documental"
        else:
            bloque_doc = contexto_doc = frase_doc
        resultado.ruta = "mixta"
        resultado.respuesta = f"{bloque_datos}\n\n{bloque_doc}"
        resultado.respuesta_contexto = f"{bloque_datos}\n\n{contexto_doc}"
        resultado.material = MaterialTurno(ruta_material, tuple(textos)) if textos else None
        return resultado

    async def _cerrar_documental(self, pregunta: str, historial: list[TurnoGuardado],
                                 evidencias: EvidenciasTurno, resultado: ResultadoTurno,
                                 emitir: Emisor) -> ResultadoTurno:
        documental = await sintesis_documental(self._llamar, self._herramientas[rag.NOMBRE],
                                               pregunta, evidencias, emitir, historial)
        resultado.ruta = "documental"
        resultado.respuesta = documental.respuesta
        resultado.advertencia = documental.advertencia
        resultado.valida = documental.valida
        resultado.reparaciones = documental.reparaciones
        resultado.respuesta_contexto = documental.respuesta_contexto or ""
        _acumular_fuentes(resultado.fuentes, documental.fuentes)  # solo los documentos citados
        if documental.valida:  # si no, la respuesta es la frase de insuficiencia: nada que comprobar
            resultado.material = comprobaciones.material_documental(
                pregunta, evidencias.para_sintesis(), documental.afirmaciones, documental.limitaciones)
        return resultado

    async def _cerrar_datos(self, pregunta: str, historial: list[TurnoGuardado], datos: list[str],
                            resultado: ResultadoTurno, emitir: Emisor, emitir_texto: Emisor | None,
                            mixta: bool = False) -> ResultadoTurno:
        """Síntesis sin herramientas con las filas de las consultas y las fechas con datos. Es una
        respuesta definitiva: sale como tokens. Las cifras se comprueban contra filas y fechas.
        `mixta`: solo describe las mediciones; la parte de salud es del bloque documental."""
        contexto = self._herramientas[sql_libre.NOMBRE].contexto_prompt()
        prompt = frases.PROMPT_SINTESIS_DATOS + (frases.SINTESIS_DATOS_MIXTA if mixta else "")
        with observabilidad.span("sintesis_datos", "chain"):
            await emitir(EventoEstado(Fase.REDACTANDO))
            respuesta = await self._llamar(
                _mensajes_sintesis(prompt, pregunta, datos, historial, contexto), [], emitir_texto)
        resultado.ruta = "datos"
        resultado.respuesta = _texto(respuesta)
        resultado.material = comprobaciones.material_libre(pregunta, [contexto, *datos], resultado.respuesta)
        return resultado

    def _comprobar(self, resultado: ResultadoTurno) -> None:
        """Comprobaciones posteriores sobre lo que escribió el modelo; sin material, nada."""
        if resultado.material is None:
            return
        resultado.hallazgos = comprobaciones.comprobar(resultado.material, self._herramientas, self._bloquean)
        for h in resultado.hallazgos:
            observabilidad.comprobacion(h.regla, h.detalle, h.bloquea)
        if any(h.bloquea for h in resultado.hallazgos):
            # La ruta no cambia. El texto bloqueado no entra en el historial: entra la frase.
            frase = frases.INSUFICIENCIA if resultado.ruta == "documental" else frases.RESPUESTA_RETENIDA
            resultado.respuesta = resultado.respuesta_contexto = frase
            resultado.fuentes, resultado.advertencia = [], None
            resultado.bloqueada = True

    async def _clasificar(self, pregunta: str, historial: list[TurnoGuardado],
                          emitir: Emisor) -> Clasificacion:
        if self._llm_clasificador is None:
            return DESCONOCIDA
        await emitir(EventoEstado(Fase.CLASIFICANDO))
        return await clasificar(self._llm_clasificador, pregunta, self._clasificador_timeout_s, historial)

    def _prompt_sistema(self, prompt: str, disponibles: dict[str, BaseTool]) -> str:
        """El prompt de la decisión más el contexto de las herramientas ofrecidas que lo tengan."""
        contextos = [c for nombre in disponibles if (c := self._herramientas[nombre].contexto_prompt())]
        return " ".join([prompt, *contextos])

    async def _herramientas_disponibles(self, decision: Decision) -> dict[str, BaseTool]:
        """Se ofrecen las herramientas que permite la decisión y cuya definición se pudo obtener ahora."""
        disponibles: dict[str, BaseTool] = {}
        for nombre, herramienta in self._herramientas.items():
            if decision.herramientas is not None and nombre not in decision.herramientas:
                continue
            definicion = await herramienta.definicion()
            if definicion is None:
                logger.warning("La herramienta %s no se ofrece en este turno", nombre)
                observabilidad.decision(f"herramienta_no_disponible:{nombre}")
                continue
            disponibles[nombre] = como_llamaindex(definicion)
        return disponibles

    async def _llamar(self, mensajes: list[ChatMessage], herramientas: list[BaseTool],
                      emitir_texto: Emisor | None = None) -> ChatResponse:
        """Una llamada al LLM, drenando el stream. Devuelve la última respuesta (mensaje completo).
        La fase a la que pertenece la da el span padre (bucle, síntesis...).
        `emitir_texto`: recibe cada `delta` no vacío; solo para respuestas definitivas (D1).
        El razonamiento de los modelos que razonan no viaja en `delta` (verificado en Bedrock)."""
        nombres = [h.metadata.name for h in herramientas]
        with observabilidad.llamada_llm(self._llm, mensajes, nombres) as span:
            try:
                if herramientas:
                    flujo = await self._llm.astream_chat_with_tools(herramientas, chat_history=list(mensajes))
                else:
                    flujo = await self._llm.astream_chat(list(mensajes))
                ultima: ChatResponse | None = None
                async for trozo in flujo:
                    ultima = trozo
                    if emitir_texto is not None and trozo.delta:
                        await emitir_texto(EventoTexto(trozo.delta))
            except Exception as exc:  # el proveedor puede lanzar de todo: se traduce a un error propio
                logger.error("Fallo del LLM: %s", exc)
                raise LLMNoDisponible(str(exc)) from exc
            if ultima is None:
                raise LLMNoDisponible("El LLM no devolvió ninguna respuesta")
            observabilidad.anotar_respuesta(span, ultima)
            return ultima

    def _llamadas_pedidas(self, respuesta: ChatResponse) -> list[ToolSelection]:
        return self._llm.get_tool_calls_from_response(respuesta, error_on_no_tool_call=False)

    async def _ejecutar(self, llamada: ToolSelection, disponibles: dict[str, BaseTool]) -> ResultadoHerramienta:
        herramienta = self._herramientas.get(llamada.tool_name)
        if herramienta is None or llamada.tool_name not in disponibles:
            plantilla = (frases.ERROR_HERRAMIENTA_DESCONOCIDA if herramienta is None
                         else frases.ERROR_HERRAMIENTA_NO_PERMITIDA)
            return ResultadoHerramienta.fallo(plantilla.format(
                nombre=llamada.tool_name, disponibles=", ".join(disponibles) or "ninguna"))
        argumentos = llamada.tool_kwargs if isinstance(llamada.tool_kwargs, dict) else {}
        try:
            return await herramienta.ejecutar(argumentos)
        except Exception as exc:  # las herramientas no deben lanzar, pero el bucle no se cae si lo hacen
            logger.exception("La herramienta %s lanzó una excepción", llamada.tool_name)
            return ResultadoHerramienta.fallo(f"Error interno al ejecutar {llamada.tool_name}: {exc}")


    async def _ejecutar_y_anotar(self, llamada: ToolSelection, disponibles: dict[str, BaseTool],
                                 evidencias: EvidenciasTurno, resultado: ResultadoTurno, emitir: Emisor,
                                 ) -> tuple[ResultadoHerramienta, bool]:
        """Ejecuta una llamada, incorpora las evidencias del RAG y la anota en el resultado.
        Devuelve el resultado (renumerado si trae evidencias) y si la búsqueda salió vacía.
        El span guarda lo que vio el modelo: el resultado ya renumerado."""
        argumentos = llamada.tool_kwargs if isinstance(llamada.tool_kwargs, dict) else {}
        with observabilidad.herramienta(llamada.tool_name, argumentos) as span:
            await emitir(EventoEstado(Fase.BUSCANDO, llamada.tool_name))
            r = await self._ejecutar(llamada, disponibles)
            sin_evidencia = False
            if llamada.tool_name == rag.NOMBRE and r.ok and r.internos:
                if r.internos["estado"] == "sin_evidencia":
                    sin_evidencia = True
                else:
                    r = evidencias.incorporar(r)
            resultado.herramientas_usadas.append(llamada.tool_name)
            _acumular_fuentes(resultado.fuentes, r.fuentes)
            span.set_output(r.para_el_modelo())
            span.set_attributes({
                "agente.ok": r.ok,
                "agente.permitida": llamada.tool_name in disponibles,
                "agente.forzada": llamada.tool_id in (ID_BUSQUEDA_FORZADA, ID_CONSULTA_FORZADA),
            })
            if r.internos and "estado" in r.internos:
                span.set_attribute("agente.estado", r.internos["estado"])
            if not r.ok:
                span.set_status(Status(StatusCode.ERROR, r.error))
            return r, sin_evidencia


# ---------------------------------------------------------------------- utilidades

async def _no_emitir(evento: Evento) -> None:
    """Emisor de los turnos sin stream."""


def _texto_al_cliente(emitir: Emisor | None, resultado: ResultadoTurno) -> Emisor | None:
    """Emisor de tokens que marca `resultado.emitida`. None si el turno no tiene emisor."""
    if emitir is None:
        return None

    async def emitir_texto(evento: Evento) -> None:
        resultado.emitida = True
        await emitir(evento)
    return emitir_texto


async def _anadir_nota(resultado: ResultadoTurno, nota: str, emitir_texto: Emisor | None) -> None:
    """Nota de la intención con frase fija que acompañaba a otra (`DATOS, PREDICCION`). Es del
    código: no se comprueba. Si la respuesta ya salió como tokens, la nota sale también así."""
    observabilidad.decision("nota_final")
    resultado.respuesta = f"{resultado.respuesta}\n\n{nota}"
    if resultado.respuesta_contexto:
        resultado.respuesta_contexto = f"{resultado.respuesta_contexto}\n\n{nota}"
    if resultado.emitida and emitir_texto is not None:
        await emitir_texto(EventoTexto(f"\n\n{nota}"))


def _fija(resultado: ResultadoTurno, frase: str) -> ResultadoTurno:
    resultado.respuesta = frase
    resultado.ruta = "fija"
    return resultado


def _sin_evidencia(resultado: ResultadoTurno) -> ResultadoTurno:
    resultado.respuesta = frases.SIN_EVIDENCIA
    resultado.ruta = "sin_evidencia"
    return resultado


def _atributos_turno(resultado: ResultadoTurno) -> dict:
    atributos = {
        "agente.ruta": resultado.ruta,
        "agente.intencion": resultado.intencion,
        "agente.tema": resultado.tema,
        "agente.vueltas": resultado.vueltas,
        "agente.busqueda_forzada": resultado.busqueda_forzada,
        "agente.consulta_forzada": resultado.consulta_forzada,
        "agente.sintesis_forzada": resultado.sintesis_forzada,
        "agente.herramientas": resultado.herramientas_usadas,
    }
    if resultado.valida is not None:
        atributos["agente.valida"] = resultado.valida
        atributos["agente.reparaciones"] = resultado.reparaciones
    if resultado.hallazgos:
        atributos["agente.comprobaciones"] = list(dict.fromkeys(h.regla for h in resultado.hallazgos))
    if resultado.bloqueada:
        atributos["agente.bloqueada"] = True
    return atributos


def _texto(respuesta: ChatResponse) -> str:
    contenido = (respuesta.message.content or "").strip()
    return contenido or frases.RESPUESTA_VACIA


def _mensajes_sintesis(prompt: str, pregunta: str, consultas: list[str],
                       historial: list[TurnoGuardado], contexto: str = "") -> list[ChatMessage]:
    """Síntesis forzada y de datos. `contexto`: fechas con datos, entre la pregunta y los resultados."""
    # Mensajes nuevos, como en la síntesis documental: Bedrock Converse rechaza los bloques
    # toolUse/toolResult del historial cuando la llamada no lleva herramientas (toolConfig).
    usuario = f"Pregunta: {pregunta}\n\n" + (f"{contexto}\n\n" if contexto else "")
    usuario += "Resultados de las consultas:\n" + "\n".join(consultas)
    if historial:
        usuario = f"{frases.CONVERSACION_PREVIA_SINTESIS}\n{texto_historial(historial)}\n\n{usuario}"
    return [ChatMessage(role=MessageRole.SYSTEM, content=prompt),
            ChatMessage(role=MessageRole.USER, content=usuario)]


def _con_preguntas_usuario(llamada: ToolSelection, pregunta: str,
                           historial: list[TurnoGuardado]) -> ToolSelection:
    """`consultar_datos` recibe además las preguntas del usuario: las fechas salen de ellas, no de
    la reescritura del modelo (`business/periodos.py`)."""
    argumentos = llamada.tool_kwargs if isinstance(llamada.tool_kwargs, dict) else {}
    return ToolSelection(tool_id=llamada.tool_id, tool_name=llamada.tool_name, tool_kwargs={
        **argumentos, sql_libre.ARG_PREGUNTA_USUARIO: pregunta,
        sql_libre.ARG_PREGUNTAS_PREVIAS: preguntas_previas(historial)})


def _mensaje_tool(llamada: ToolSelection, resultado: ResultadoHerramienta) -> ChatMessage:
    # `tool_call_id` es lo que OpenAI y Bedrock Converse necesitan para casar resultado y petición.
    return ChatMessage(
        role=MessageRole.TOOL,
        content=resultado.para_el_modelo(),
        additional_kwargs={"tool_call_id": llamada.tool_id, "name": llamada.tool_name},
    )


def _acumular_fuentes(destino: list[Fuente], nuevas: Sequence[Fuente]) -> None:
    for f in nuevas:
        if f not in destino:
            destino.append(f)
