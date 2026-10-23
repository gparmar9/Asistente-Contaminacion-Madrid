"""Reconstruye turnos y calcula tokens y coste a partir de spans.

Compartido por los KPIs y el informe de evaluación. Solo usa la biblioteca estándar."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

# USD por millón de tokens (entrada, salida), Bedrock eu-west-1 bajo demanda.
# Clave: ID del modelo en Bedrock. Sin precio conocido, el coste queda incompleto.
PRECIOS_FECHA = "2026-10-08, Bedrock eu-west-1"
PRECIOS = {
    "mistral.ministral-3-14b-instruct": (0.23, 0.23),
    "openai.gpt-oss-120b-1:0": (0.18, 0.70),
    "openai.gpt-oss-20b-1:0": (0.08, 0.35),
    "qwen.qwen3-coder-30b-a3b-v1:0": (0.18, 0.70),
    "eu.anthropic.claude-haiku-5-5": (0.11, 0.55),  # confirmado el 2026-10-16 (AWS Marketplace)
    "minimax.minimax-m2.5": (0.36, 1.44),  # consultado el 2026-10-09
    "qwen.qwen3-next-80b-a3b": (0.168, 1.44),  # consultado el 2026-10-16
    "nvidia.nemotron-super-3-120b": (0.18, 0.78),  # consultado el 2026-10-16
}


@dataclass
class Turno:
    trace_id: str
    spans: list[dict]                 # todos los spans de la traza, en orden de inicio
    raiz: dict                        # span `turno`
    etiqueta: str
    ruta: str
    intencion: str
    duracion_ms: float
    error: bool
    cancelado: bool                   # Stream cerrado por el cliente
    modelos: set[str] = field(default_factory=set)
    tokens_entrada: int | None = None  # None = incompleto
    tokens_salida: int | None = None
    coste_usd: float | None = None     # None = incompleto o modelo sin precio

    def atributo(self, clave: str, defecto=None):
        return self.raiz["attributes"].get(clave, defecto)


def turnos(spans: list[dict]) -> tuple[list[Turno], int]:
    """Turnos completos y número de trazas sin span `turno` (proceso cortado a mitad)."""
    por_traza: dict[str, list[dict]] = defaultdict(list)
    for s in spans:
        por_traza[s["trace_id"]].append(s)
    resultado, huerfanas = [], 0
    for trace_id, propios in por_traza.items():
        propios.sort(key=lambda s: s["start"] or "")
        raiz = next((s for s in propios if s["name"] == "turno" and not s["parent_id"]), None)
        if raiz is None:
            huerfanas += 1
            continue
        a = raiz["attributes"]
        cancelado = bool(a.get("agente.cancelado"))
        turno = Turno(trace_id=trace_id, spans=propios, raiz=raiz,
                      etiqueta=a.get("agente.etiqueta") or "sin_etiqueta",
                      ruta=a.get("agente.ruta", "cancelado" if cancelado else "error"),
                      intencion=a.get("agente.intencion", "?"),
                      duracion_ms=raiz["duracion_ms"], error=raiz["status"] == "ERROR", cancelado=cancelado)
        _tokens_y_coste(turno)
        resultado.append(turno)
    resultado.sort(key=lambda t: t.raiz["start"] or "")
    return resultado, huerfanas


def coste_llamada(modelo: str, entrada: int, salida: int) -> float | None:
    """USD de una llamada al LLM; None si el modelo no tiene precio."""
    if modelo not in PRECIOS:
        return None
    precio_e, precio_s = PRECIOS[modelo]
    return (entrada * precio_e + salida * precio_s) / 1e6


def _tokens_y_coste(turno: Turno) -> None:
    entrada = salida = 0
    coste = 0.0
    completo, con_precio = True, True
    for s in turno.spans:
        if s["kind"] != "LLM":
            continue
        a = s["attributes"]
        modelo = a.get("llm.model_name") or "?"
        turno.modelos.add(modelo)
        e, o = a.get("llm.token_count.prompt"), a.get("llm.token_count.completion")
        if not (isinstance(e, int) and isinstance(o, int)):
            completo = False
            continue
        entrada, salida = entrada + e, salida + o
        coste_de_esta = coste_llamada(modelo, e, o)
        if coste_de_esta is None:
            con_precio = False
        else:
            coste += coste_de_esta
    if completo:
        turno.tokens_entrada, turno.tokens_salida = entrada, salida
        if con_precio:
            turno.coste_usd = coste
