"""Clasificador de intención y decisión en código, con el LLM falso y el RAG fingido."""
import json

import httpx
import pytest

from agente.business import frases
from agente.business.bucle import Bucle
from agente.business.intencion import decidir, interpretar
from agente.entities.eventos import EventoTexto
from agente.entities.intencion import Intencion
from agente.llm.falso import LLMFalso, llamada, texto
from agente.tools import rag as herramienta_rag, sql_libre
from agente.tools.base import Herramienta, ResultadoHerramienta
from agente.tools.rag import HerramientaRag
from tests.conftest import SALIDA_NO2, TEXTO_NO2, RagFingido, herramienta_datos

pytestmark = pytest.mark.anyio

PREGUNTA = "¿Qué efectos tiene el NO2 en el asma?"
JSON_NO2 = json.dumps(SALIDA_NO2, ensure_ascii=False)


class HerramientaMediciones(Herramienta):
    """Segunda herramienta de prueba, para vetarla en preguntas documentales."""

    nombre = "consultar_mediciones"

    def __init__(self):
        self.ejecuciones = 0

    async def definicion(self) -> dict:
        return {"type": "function", "function": {
            "name": self.nombre, "description": "Mediciones de las estaciones.",
            "parameters": {"type": "object", "properties": {}}}}

    async def ejecutar(self, argumentos: dict) -> ResultadoHerramienta:
        self.ejecuciones += 1
        return ResultadoHerramienta.exito({"no2": 50})


def _bucle(clasificador, guion, rag: RagFingido | None = None, extra: list[Herramienta] = ()):
    llm = LLMFalso(guion=list(guion))
    llm_clasificador = LLMFalso(guion=list(clasificador))
    herramientas = [HerramientaRag("http://rag:8010", transport=httpx.MockTransport(rag or RagFingido())),
                    *extra]
    return llm, Bucle(llm, herramientas, llm_clasificador=llm_clasificador)


def _clase(intencion: str, tema: str = "ninguno"):
    return texto(f"intencion: {intencion}\ntema: {tema}")


@pytest.mark.parametrize("intencion, frase", [
    ("DATOS", frases.FRASE_DATOS_NO_DISPONIBLE),  # sin herramienta de datos (sin base de datos)
    ("PREDICCION", frases.FRASE_PREDICCION),
    ("FUERA_DE_ALCANCE", frases.FRASE_FUERA_DE_ALCANCE),
])
async def test_intenciones_con_frase_fija_no_llaman_al_modelo(intencion, frase):
    llm, bucle = _bucle([_clase(intencion)], [])
    r = await bucle.responder("¿Qué estación está peor ahora?")
    assert r.respuesta == frase and r.ruta == "fija" and r.intencion == intencion
    assert llm.registro == []



async def test_clasificacion_en_negrita_se_acepta():
    llm, bucle = _bucle([texto("intencion: **FUERA_DE_ALCANCE**\ntema: **ninguno**")], [])
    r = await bucle.responder("¿Me recomiendas un restaurante?")
    assert r.intencion == "FUERA_DE_ALCANCE" and r.ruta == "fija"

async def test_documental_sin_peticion_busca_el_codigo_con_el_tema():
    rag = RagFingido()
    llm, bucle = _bucle([_clase("DOCUMENTAL", "salud")],
                        [texto("Respuesta sin buscar."), texto(JSON_NO2)], rag)
    r = await bucle.responder(PREGUNTA)
    assert r.respuesta == TEXTO_NO2 and r.ruta == "documental" and r.busqueda_forzada
    assert rag.busquedas == [{"pregunta": PREGUNTA, "tema": "salud"}]


async def test_charla_responde_sin_herramientas():
    llm, bucle = _bucle([_clase("CHARLA")], [texto("Hola, te ayudo con la calidad del aire.")])
    r = await bucle.responder("Hola, ¿qué sabes hacer?")
    assert r.respuesta == "Hola, te ayudo con la calidad del aire." and r.ruta == "libre"
    assert llm.registro[0]["herramientas"] == []
    assert llm.registro[0]["mensajes"][0].content == frases.PROMPT_CHARLA


@pytest.mark.parametrize("fallo", [RuntimeError("proveedor caído"), texto("Es DOCUMENTAL")])
async def test_clasificador_caido_sigue_como_desconocida(fallo):
    llm, bucle = _bucle([fallo], [texto("Respuesta libre.")], extra=[HerramientaMediciones()])
    r = await bucle.responder(PREGUNTA)
    assert r.intencion == "DESCONOCIDA" and r.respuesta == "Respuesta libre."
    assert llm.registro[0]["herramientas"] == ["buscar_evidencias", "consultar_mediciones"]


async def test_herramienta_vetada_se_rechaza_sin_ejecutarla():
    mediciones = HerramientaMediciones()
    llm, bucle = _bucle([_clase("DOCUMENTAL", "salud")],
                        [llamada("consultar_mediciones", {}, "c1"), texto("Sin datos."), texto(JSON_NO2)],
                        extra=[mediciones])
    r = await bucle.responder(PREGUNTA)
    assert llm.registro[0]["herramientas"] == ["buscar_evidencias"]
    assert mediciones.ejecuciones == 0
    assert "no está disponible" in llm.registro[1]["mensajes"][-1].content
    assert r.respuesta == TEXTO_NO2  # y la búsqueda obligada la hizo el código


async def test_documental_con_rag_caido_da_frase_fija():
    llm, bucle = _bucle([_clase("DOCUMENTAL", "salud")], [], RagFingido(caido=True))
    r = await bucle.responder(PREGUNTA)
    assert r.respuesta == frases.DOCUMENTACION_NO_DISPONIBLE and llm.registro == []


async def test_documental_con_busqueda_caida_no_repite_la_busqueda():
    rag = RagFingido(busqueda_caida=True)
    llm, bucle = _bucle([_clase("DOCUMENTAL", "salud")],
                        [llamada("buscar_evidencias", {"pregunta": PREGUNTA}, "c1"), texto("Ahora no puedo.")], rag)
    r = await bucle.responder(PREGUNTA)
    assert r.respuesta == frases.DOCUMENTACION_NO_DISPONIBLE and r.ruta == "fija"
    assert len(rag.busquedas) == 1 and not r.busqueda_forzada


# ---------------------------------------------------------------------- varias intenciones

@pytest.mark.parametrize("salida, intenciones", [
    ("intencion: DATOS, DOCUMENTAL\ntema: salud", {"DATOS", "DOCUMENTAL"}),
    ("intencion: **DATOS**\ntema: ninguno", {"DATOS"}),
    ("intencion: DATOS, DOCUMENTAL, CHARLA\ntema: salud", None),  # tres o más
    ("intencion: DATOS, DATOS\ntema: ninguno", None),
    ("intencion: DATOS, OTRA\ntema: ninguno", None),
])
async def test_clasificador_acepta_una_o_dos_intenciones(salida, intenciones):
    c = interpretar(salida)
    assert (c and {i.value for i in c.intenciones}) == intenciones


@pytest.mark.parametrize("intenciones, frase, herramientas, obligadas", [
    ({Intencion.DATOS, Intencion.DOCUMENTAL}, None, {herramienta_rag.NOMBRE, sql_libre.NOMBRE}, (True, True)),
    ({Intencion.PREDICCION, Intencion.FUERA_DE_ALCANCE}, frases.FRASE_PREDICCION, None, (False, False)),
])
async def test_decision_por_conjunto(intenciones, frase, herramientas, obligadas):
    d = decidir(frozenset(intenciones))
    assert d.frase == frase and (d.herramientas and set(d.herramientas)) == herramientas
    assert (d.busqueda_obligada, d.datos_obligados) == obligadas


async def test_datos_y_prediccion_responde_los_datos_y_anade_la_nota(bd_mediciones):
    sql = ("SELECT nombre_estacion, media FROM mediciones_bloques "
           "WHERE fecha = '2026-04-30' AND contaminante = 'NO2' ORDER BY media DESC")
    sintesis = "El 30 de abril de 2026, último día disponible, el NO2 más alto fue el de Plaza Elíptica (45,0 µg/m³)."
    _, datos = herramienta_datos(bd_mediciones, [texto(sql)])
    llm = LLMFalso(guion=[texto("Texto libre que la síntesis sustituye."), texto(sintesis)])
    bucle = Bucle(llm, [datos], llm_clasificador=LLMFalso(guion=[_clase("DATOS, PREDICCION")]))
    tokens = []

    async def emitir(evento):
        if isinstance(evento, EventoTexto):
            tokens.append(evento.texto)
    r = await bucle.responder("¿Cómo está hoy el aire en Vallecas y cómo estará mañana?", emitir)

    assert (r.intencion, r.ruta) == ("DATOS, PREDICCION", "datos")
    assert r.respuesta == f"{sintesis}\n\n{frases.NOTA_PREDICCION}"
    assert r.emitida and "".join(tokens) == r.respuesta  # la nota sale también como token
