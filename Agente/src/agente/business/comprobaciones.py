"""Comprobaciones posteriores: el código revisa el texto que escribió el modelo en el turno.

    cifras    números del texto que no están en su base (pregunta + evidencias o resultados)
    fuga      10 palabras seguidas compartidas con un prompt del sistema
    internos  nombres de herramientas, chunk_id, marcas [Dn] fuera de la ruta documental, JSON crudo

Funciones puras (texto + base -> hallazgos), sin LLM ni red: las usan igual el `Bucle`, los
scripts de evaluación y los tests. Cada regla observa (solo deja constancia) salvo que esté en
`COMPROBACIONES_BLOQUEAN`; entonces el `Bucle` sustituye la respuesta por una frase fija.

Solo se comprueba lo que escribió el modelo: en la ruta documental, los textos de las afirmaciones
y limitaciones, no el texto que renderiza el RAG (lleva a propósito chunk_id, fechas y URLs).
"""
from __future__ import annotations

import re
import unicodedata
from decimal import Decimal
from functools import lru_cache
from typing import Iterable, Sequence

from agente.business import frases
from agente.entities.comprobaciones import Hallazgo, MaterialTurno, TextoComprobado

CIFRAS, FUGA, INTERNOS = "cifras", "fuga", "internos"
REGLAS = (CIFRAS, FUGA, INTERNOS)

TAM_NGRAMA = 10
UMBRAL_FUGA = 1  # n-gramas compartidos a partir de los que salta la regla

# Número aislado, con decimales o miles con coma o punto. No cuenta si va pegado detrás de una
# letra o un dígito (PM2.5, NO2, O3, D1). Los superíndices y subíndices (m³, NO₂) no son \d.
_RE_CIFRA = re.compile(r"(?<![^\W_])(?<![^\W_][.,])\d+(?:[.,]\d+)*")
# archivo:slug:ordinal, p. ej. oxidos_nitrogeno_salud:efectos-en-la-salud:0
_RE_CHUNK_ID = re.compile(r"\b[a-z][a-z0-9_]*:[a-z0-9][a-z0-9-]*:\d+\b")
_RE_MARCA = re.compile(r"\[D\d+\]?")


def cifras(texto: str, base: str) -> list[str]:
    """Cifras del texto que no aparecen en la base (`2,5` = `2.5`). Años 1900–2099 excluidos."""
    respaldadas = {_valor(c) for c in _cifras(base)}
    sin_respaldo: list[str] = []
    for c in _cifras(texto):
        if _valor(c) not in respaldadas and c not in sin_respaldo:
            sin_respaldo.append(c)
    return sin_respaldo


def fuga(texto: str, prompts: Sequence[str] = frases.PROMPTS,
         publicas: Sequence[str] = frases.FRASES_PUBLICAS) -> list[str]:
    """N-gramas del texto que también están en algún prompt, en orden de aparición. Las frases
    públicas (identidad, capacidades) no cuentan: el modelo las repite al presentarse."""
    del_prompt = _ngramas_prompts(tuple(prompts), tuple(publicas))
    compartidos: list[str] = []
    for n in _ngramas(_palabras(texto)):
        if n in del_prompt and n not in compartidos:
            compartidos.append(n)
    return compartidos


def internos(texto: str, herramientas: Iterable[str], ruta: str) -> list[str]:
    """Términos internos del texto. Las marcas [Dn] solo cuentan fuera de la ruta documental."""
    encontrados = [n for n in herramientas if re.search(rf"\b{re.escape(n)}\b", texto, re.IGNORECASE)]
    encontrados += _RE_CHUNK_ID.findall(texto)
    if ruta != "documental":
        encontrados += _RE_MARCA.findall(texto)
    if texto.lstrip().startswith("{") or "```json" in texto:
        encontrados.append("JSON")
    return list(dict.fromkeys(encontrados))


def comprobar(material: MaterialTurno, herramientas: Iterable[str],
              bloquean: Iterable[str] = ()) -> list[Hallazgo]:
    """Aplica las tres reglas a cada texto del turno. Un hallazgo por regla y texto."""
    herramientas, bloquean = list(herramientas), set(bloquean)
    hallazgos: list[Hallazgo] = []
    for t in material.textos:
        if sin_respaldo := cifras(t.texto, t.base):
            hallazgos.append(Hallazgo(CIFRAS, ", ".join(sin_respaldo), CIFRAS in bloquean))
        if len(compartidos := fuga(t.texto)) >= UMBRAL_FUGA:
            hallazgos.append(Hallazgo(FUGA, f"{compartidos[0]} ({len(compartidos)})", FUGA in bloquean))
        if encontrados := internos(t.texto, herramientas, material.ruta):
            hallazgos.append(Hallazgo(INTERNOS, ", ".join(encontrados), INTERNOS in bloquean))
    return hallazgos


def material_libre(pregunta: str, consultas: Sequence[str], respuesta: str) -> MaterialTurno:
    """Ruta libre: la respuesta, con la pregunta y los resultados de las herramientas como base."""
    return MaterialTurno("libre", (TextoComprobado(respuesta, "\n".join([pregunta, *consultas])),))


def material_documental(pregunta: str, evidencias: Sequence[dict], afirmaciones: Sequence[dict],
                        limitaciones: Sequence[str]) -> MaterialTurno:
    """Ruta documental: cada afirmación contra las evidencias que cita; cada limitación, contra
    todas las del turno. `evidencias`: las que vio la síntesis ({id, texto, ...}, ya renumeradas)."""
    texto_por_id = {e["id"]: e.get("texto", "") for e in evidencias}
    todas = "\n".join([pregunta, *texto_por_id.values()])
    textos = [TextoComprobado(a["texto"], "\n".join([pregunta, *(texto_por_id.get(i, "") for i in a["evidencias"])]))
              for a in afirmaciones]
    textos += [TextoComprobado(str(l), todas) for l in limitaciones]
    return MaterialTurno("documental", tuple(textos))


# ---------------------------------------------------------------------- utilidades

def _cifras(texto: str) -> list[str]:
    # El ordinal de un chunk_id no es una cifra: ese término ya lo señala la regla de internos.
    candidatas = _RE_CIFRA.findall(_RE_CHUNK_ID.sub(" ", texto))
    return [c for c in candidatas if not (len(c) == 4 and c.isdigit() and 1900 <= int(c) <= 2099)]


def _valor(cifra: str) -> Decimal:
    """`2,5` y `2.5` valen lo mismo. Con más de un separador son miles (`1.000.000`)."""
    normalizada = cifra.replace(",", ".")
    if normalizada.count(".") > 1:
        normalizada = normalizada.replace(".", "")
    return Decimal(normalizada)


def _palabras(texto: str) -> list[str]:
    # NFKC: NO₂ -> no2, como en los prompts.
    return re.findall(r"\w+", unicodedata.normalize("NFKC", texto).lower())


def _ngramas(palabras: list[str]) -> list[str]:
    return [" ".join(palabras[i:i + TAM_NGRAMA]) for i in range(len(palabras) - TAM_NGRAMA + 1)]


@lru_cache
def _ngramas_prompts(prompts: tuple[str, ...], publicas: tuple[str, ...]) -> frozenset[str]:
    """N-gramas de los prompts sin las frases públicas: se cortan por ellas para que ningún
    n-grama las atraviese."""
    separador = re.compile("|".join(re.escape(p) for p in publicas)) if publicas else None
    ngramas: set[str] = set()
    for prompt in prompts:
        for trozo in (separador.split(prompt) if separador else [prompt]):
            ngramas.update(_ngramas(_palabras(trozo)))
    return frozenset(ngramas)
