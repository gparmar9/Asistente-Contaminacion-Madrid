"""Evidencias para un LLM externo: recuperar, validar sus citas y renderizar.

Este paquete NO llama a ningún modelo. El LLM vive en la API de chat y usa este
RAG como herramienta (function tool) en dos pasos:

  1. `recuperar(pregunta)`  → fragmentos numerados D1..Dn bajo el umbral de
     distancia, con bibliografía leída del frontmatter y avisos fijos. Si nada
     supera el umbral, `estado = sin_evidencia` y la API de chat no debería
     pedir al modelo que responda con documentos.
  2. El modelo redacta {estado, afirmaciones[{texto, evidencias}], limitaciones}.
  3. `validar_y_renderizar(pregunta, evidencias, salida)` → comprueba el contrato
     (esquema, estado, ninguna afirmación sin evidencias, ningún ID fuera de
     D1..Dn) y, si es válido, devuelve el texto con [Dn], aviso sanitario,
     limitación de actualidad y bibliografía. Si no lo es, devuelve la lista de
     errores y un mensaje de reparación para reenviar al modelo. La API de chat
     decide cuántas reparaciones permite (el plan propone una).

Uso por consola (solo el paso 1):
    python -m rag.evidencias "¿qué son los bloques del día?"
"""
from __future__ import annotations

import argparse
import json
import os
import re
import unicodedata
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Callable

from rag import buscar as mod_buscar
from rag.corpus import RUTA_CORPUS, Documento, leer_documento, slug
from rag.errores import EvidenciaDesconocida

# Umbral de distancia coseno por encima del cual un fragmento NO cuenta como
# evidencia útil. Provisional (plan §7.4): con e5-base las preguntas del corpus
# dieron 0,11–0,20 en su mejor fragmento y las ajenas 0,237–0,247. Se calibra
# con los casos de evaluación.
UMBRAL_DISTANCIA_POR_DEFECTO = 0.22

ESTADOS = ("respondida", "parcial", "sin_evidencia")
MAX_AFIRMACIONES = 8
MAX_LIMITACIONES = 6
MAX_CARACTERES_TEXTO = 700
_RE_ID = re.compile(r"^D[1-9][0-9]*$")
_RE_CHUNK_ID = re.compile(r"^([a-z0-9_]+):([a-z0-9-]+):([0-9]+)$")

# Contrato de la salida del modelo. Es JSON Schema para que la API de chat pueda
# pasarlo tal cual como `response_format` / `format` a su proveedor.
ESQUEMA_SALIDA: dict = {
    "type": "object",
    "additionalProperties": False,
    "required": ["estado", "afirmaciones", "limitaciones"],
    "properties": {
        "estado": {"type": "string", "enum": list(ESTADOS)},
        "afirmaciones": {
            "type": "array",
            "maxItems": MAX_AFIRMACIONES,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["texto", "evidencias"],
                "properties": {
                    "texto": {"type": "string", "maxLength": MAX_CARACTERES_TEXTO},
                    "evidencias": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"type": "string", "pattern": "^D[0-9]+$"},
                    },
                },
            },
        },
        "limitaciones": {
            "type": "array",
            "maxItems": MAX_LIMITACIONES,
            "items": {"type": "string", "maxLength": MAX_CARACTERES_TEXTO},
        },
    },
}

AVISO_SANITARIO = (
    "Aviso: esta información es divulgativa y general. No sustituye la valoración de un "
    "profesional sanitario ni los avisos oficiales. Si tienes síntomas o una enfermedad "
    "diagnosticada, sigue las indicaciones de tu médico; ante una urgencia, llama al 112."
)
LIMITACION_ACTUALIDAD = (
    "El asistente todavía no consulta mediciones ni avisos en tiempo real: esta respuesta "
    "describe la información documental, no la situación de hoy. Para el estado actual, "
    "consulta las fuentes oficiales del Ayuntamiento de Madrid."
)
LIMITACION_SIN_EVIDENCIA = (
    "No hay documentos en el corpus que respondan a esta pregunta con suficiente "
    "confianza. El asistente solo responde con información documental revisada."
)

_RE_ACTUALIDAD = re.compile(
    r"\b(hoy|ahora|ahora mismo|en este momento|actualmente|en tiempo real|"
    r"esta (?:manana|tarde|noche|semana)|este fin de semana|"
    r"esta activ(?:o|a|ado|ada)|hay (?:un )?episodio|hay (?:un )?aviso)\b"
)
_RE_SALUD = re.compile(
    r"\b(salud|sintoma\w*|asma\w*|alergi\w*|alergic\w*|epoc|bronqui\w*|pulmon\w*|"
    r"respirator\w*|cardiovascular\w*|cardiac\w*|embaraz\w*|ninos?|ninas?|bebes?|"
    r"mayores|ancian\w*|enferm\w*|medic\w*|inhalador\w*|tos|riesgo\w*|peligros\w*|"
    r"deporte|correr|ejercicio|entrenar|mascarilla\w*|vulnerable\w*)\b"
)


# --------------------------------------------------------------------------- tipos

@dataclass
class Evidencia:
    id: str                 # D1, D2...
    chunk_id: str
    titulo: str
    seccion: str
    archivo: str
    tema: str
    distancia: float | None = None
    texto: str = ""
    citada: bool = False


@dataclass
class Afirmacion:
    texto: str
    evidencias: list[str]


@dataclass
class Referencia:
    """Bibliografía de un documento, leída del frontmatter (nunca del modelo)."""
    archivo: str
    titulo: str
    fecha_revision: str
    ids: list[str]                      # Dn que apuntan a este documento
    fuentes: list[dict]                 # [{titulo, organismo, url}]


@dataclass
class Avisos:
    sanitario: bool
    actualidad: bool
    textos: list[str] = field(default_factory=list)


@dataclass
class Recuperacion:
    """Resultado del paso 1: lo que la API de chat entrega al modelo como tool result."""
    pregunta: str
    estado: str                         # "con_evidencias" | "sin_evidencia"
    evidencias: list[Evidencia]
    bibliografia: list[Referencia]
    avisos: Avisos
    umbral: float
    descartados: int                    # fragmentos devueltos por buscar pero fuera del umbral

    def a_dict(self) -> dict:
        d = asdict(self)
        for e in d["evidencias"]:
            e.pop("citada", None)
        return d

    def para_el_modelo(self) -> dict:
        """Carga mínima para el mensaje de herramienta: solo lo que el modelo debe citar."""
        return {
            "pregunta": self.pregunta,
            "estado": self.estado,
            "evidencias": [
                {"id": e.id, "titulo": e.titulo, "seccion": e.seccion, "texto": e.texto}
                for e in self.evidencias
            ],
            "avisos": self.avisos.textos,
        }


@dataclass
class Respuesta:
    """Resultado del paso 3: salida del modelo validada y renderizada."""
    pregunta: str
    valida: bool
    errores: list[str]
    mensaje_reparacion: str
    estado: str
    afirmaciones: list[Afirmacion]
    limitaciones: list[str]
    evidencias: list[Evidencia]
    bibliografia: list[Referencia]
    aviso_sanitario: bool
    texto: str = ""

    def a_dict(self) -> dict:
        d = asdict(self)
        for e in d["evidencias"]:
            e.pop("texto", None)
        return d


# --------------------------------------------------------------------------- utilidades

def _normalizar(texto: str) -> str:
    sin_tildes = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in sin_tildes if not unicodedata.combining(c)).lower()


def pregunta_de_actualidad(pregunta: str) -> bool:
    return bool(_RE_ACTUALIDAD.search(_normalizar(pregunta)))


def pregunta_sanitaria(pregunta: str) -> bool:
    return bool(_RE_SALUD.search(_normalizar(pregunta)))


def umbral_distancia() -> float:
    valor = os.getenv("RAG_UMBRAL_DISTANCIA")
    if not valor:
        return UMBRAL_DISTANCIA_POR_DEFECTO
    try:
        umbral = float(valor)
    except ValueError as e:
        raise ValueError("RAG_UMBRAL_DISTANCIA debe ser un número") from e
    if not 0 < umbral <= 2:
        raise ValueError("RAG_UMBRAL_DISTANCIA debe estar entre 0 y 2 (distancia coseno)")
    return umbral


def _avisos(pregunta: str, evidencias: list[Evidencia], solo_citadas: bool) -> Avisos:
    relevantes = [e for e in evidencias if e.citada] if solo_citadas else evidencias
    sanitario = pregunta_sanitaria(pregunta) or any(e.tema == "salud" for e in relevantes)
    actualidad = pregunta_de_actualidad(pregunta)
    textos = []
    if actualidad:
        textos.append(LIMITACION_ACTUALIDAD)
    if sanitario:
        textos.append(AVISO_SANITARIO)
    return Avisos(sanitario=sanitario, actualidad=actualidad, textos=textos)


def _bibliografia(evidencias: list[Evidencia], directorio: Path, solo_citadas: bool) -> list[Referencia]:
    por_archivo: dict[str, list[str]] = {}
    for e in evidencias:
        if solo_citadas and not e.citada:
            continue
        por_archivo.setdefault(e.archivo, []).append(e.id)
    referencias: list[Referencia] = []
    for archivo, ids in por_archivo.items():
        doc = leer_documento(Path(directorio) / archivo)
        referencias.append(Referencia(
            archivo=archivo, titulo=doc.titulo, fecha_revision=doc.fecha_revision,
            ids=ids, fuentes=[asdict(f) for f in doc.fuentes],
        ))
    return referencias


# --------------------------------------------------------------------------- paso 1: recuperar

def seleccionar_evidencias(resultados: list[dict], umbral: float) -> list[Evidencia]:
    """Numera D1..Dn los resultados de `buscar` con distancia <= umbral (orden de búsqueda)."""
    evidencias: list[Evidencia] = []
    for r in resultados:
        if r["distancia"] > umbral:
            continue
        m = r["metadatos"]
        evidencias.append(Evidencia(
            id=f"D{len(evidencias) + 1}",
            chunk_id=r["chunk_id"],
            titulo=m.get("titulo", ""),
            seccion=m.get("seccion", ""),
            archivo=m.get("archivo", ""),
            tema=m.get("tema", ""),
            distancia=float(r["distancia"]),
            texto=r["texto"],
        ))
    return evidencias


def recuperar(pregunta: str, k: int = mod_buscar.K_POR_DEFECTO, tema: str | None = None,
              umbral: float | None = None,
              buscar: Callable[..., list[dict]] | None = None,
              directorio_corpus: Path = RUTA_CORPUS) -> Recuperacion:
    """Paso 1: evidencias D1..Dn bajo el umbral, con bibliografía y avisos. No llama a ningún LLM."""
    buscar = buscar or mod_buscar.buscar
    umbral = umbral_distancia() if umbral is None else umbral
    resultados = buscar(pregunta, k=k, tema=tema)      # valida pregunta, k y tema
    pregunta = pregunta.strip()
    evidencias = seleccionar_evidencias(resultados, umbral)
    avisos = _avisos(pregunta, evidencias, solo_citadas=False)
    if not evidencias:
        avisos.textos.insert(0, LIMITACION_SIN_EVIDENCIA)
    return Recuperacion(
        pregunta=pregunta,
        estado="con_evidencias" if evidencias else "sin_evidencia",
        evidencias=evidencias,
        bibliografia=_bibliografia(evidencias, directorio_corpus, solo_citadas=False),
        avisos=avisos,
        umbral=umbral,
        descartados=len(resultados) - len(evidencias),
    )


# --------------------------------------------------------------------------- paso 3: validar

def resolver_evidencias(referencias: list[dict], directorio_corpus: Path = RUTA_CORPUS) -> list[Evidencia]:
    """Reconstruye las evidencias desde [{id, chunk_id}] leyendo título, sección y tema del corpus.

    Así la bibliografía y los títulos nunca dependen de lo que envíe el cliente ni el
    modelo: solo del chunk_id. Lanza EvidenciaDesconocida si el documento o la
    sección no existen.
    """
    evidencias: list[Evidencia] = []
    ids_vistos: set[str] = set()
    docs: dict[str, Documento] = {}
    for ref in referencias:
        id_, chunk_id = str(ref.get("id", "")), str(ref.get("chunk_id", ""))
        if not _RE_ID.match(id_) or id_ in ids_vistos:
            raise EvidenciaDesconocida(f"id de evidencia inválido o repetido: {id_!r}")
        ids_vistos.add(id_)
        m = _RE_CHUNK_ID.match(chunk_id)
        if not m:
            raise EvidenciaDesconocida(f"chunk_id con formato inválido: {chunk_id!r}")
        nombre, slug_seccion, _ordinal = m.groups()
        archivo = f"{nombre}.md"
        if archivo not in docs:
            ruta = Path(directorio_corpus) / archivo
            if not ruta.is_file():
                raise EvidenciaDesconocida(f"{chunk_id!r}: no existe el documento {archivo}")
            docs[archivo] = leer_documento(ruta)
        doc = docs[archivo]
        seccion = next((enc for enc, _ in doc.secciones if slug(enc) == slug_seccion), None)
        if seccion is None:
            raise EvidenciaDesconocida(f"{chunk_id!r}: el documento {archivo} no tiene esa sección")
        evidencias.append(Evidencia(
            id=id_, chunk_id=chunk_id, titulo=doc.titulo, seccion=seccion,
            archivo=archivo, tema=doc.tema,
        ))
    return evidencias


def validar_salida(salida, ids_validos: set[str]) -> list[str]:
    """Comprueba el contrato de salida del modelo. Devuelve la lista de errores (vacía si es válida)."""
    errores: list[str] = []
    if not isinstance(salida, dict):
        return ["la salida debe ser un objeto JSON"]
    desconocidas = set(salida) - set(ESQUEMA_SALIDA["properties"])
    if desconocidas:
        errores.append(f"claves no permitidas: {sorted(desconocidas)}")
    for clave in ESQUEMA_SALIDA["required"]:
        if clave not in salida:
            errores.append(f"falta la clave '{clave}'")
    if errores:
        return errores

    estado = salida["estado"]
    if estado not in ESTADOS:
        errores.append(f"'estado' debe ser uno de {list(ESTADOS)}, no {estado!r}")

    afirmaciones = salida["afirmaciones"]
    if not isinstance(afirmaciones, list):
        errores.append("'afirmaciones' debe ser una lista")
        afirmaciones = []
    if len(afirmaciones) > MAX_AFIRMACIONES:
        errores.append(f"como máximo {MAX_AFIRMACIONES} afirmaciones")
    validos_ordenados = sorted(ids_validos, key=lambda s: int(s[1:]))
    for i, a in enumerate(afirmaciones, start=1):
        if not isinstance(a, dict) or set(a) != {"texto", "evidencias"}:
            errores.append(f"afirmación {i}: debe tener exactamente las claves 'texto' y 'evidencias'")
            continue
        if not isinstance(a["texto"], str) or not a["texto"].strip():
            errores.append(f"afirmación {i}: 'texto' debe ser un texto no vacío")
        elif len(a["texto"]) > MAX_CARACTERES_TEXTO:
            errores.append(f"afirmación {i}: 'texto' supera {MAX_CARACTERES_TEXTO} caracteres")
        refs = a["evidencias"]
        if not isinstance(refs, list) or not refs:
            errores.append(f"afirmación {i}: 'evidencias' debe ser una lista no vacía de IDs")
            continue
        for ref in refs:
            if not isinstance(ref, str) or ref not in ids_validos:
                errores.append(f"afirmación {i}: el ID {ref!r} no existe; los válidos son {validos_ordenados}")

    limitaciones = salida["limitaciones"]
    if not isinstance(limitaciones, list) or not all(isinstance(l, str) for l in limitaciones):
        errores.append("'limitaciones' debe ser una lista de textos")
    elif len(limitaciones) > MAX_LIMITACIONES:
        errores.append(f"como máximo {MAX_LIMITACIONES} limitaciones")

    if estado in ("respondida", "parcial") and not afirmaciones:
        errores.append(f"estado '{estado}' exige al menos una afirmación")
    if estado == "sin_evidencia" and afirmaciones:
        errores.append("estado 'sin_evidencia' exige la lista de afirmaciones vacía")
    return errores


def mensaje_reparacion(errores: list[str]) -> str:
    """Texto listo para reenviar al modelo como turno de usuario tras una salida inválida."""
    lista = "\n".join(f"- {e}" for e in errores)
    return (
        "Tu respuesta anterior no cumple el contrato. Errores:\n"
        f"{lista}\n"
        "Devuelve de nuevo el objeto JSON completo corregido, usando solo las evidencias "
        "de esta petición. Si no puedes respaldar una afirmación, elimínala o cambia el "
        "estado a \"sin_evidencia\"."
    )


def renderizar(r: Respuesta) -> str:
    """Texto final para el usuario. Solo usa datos ya validados y el frontmatter."""
    lineas: list[str] = []
    if r.afirmaciones:
        for a in r.afirmaciones:
            marcas = "".join(f"[{ref}]" for ref in a.evidencias)
            lineas.append(f"{a.texto.strip()} {marcas}")
    else:
        lineas.append("No puedo responder a esta pregunta con la documentación disponible.")
    if r.limitaciones:
        lineas.append("")
        lineas.append("Limitaciones:")
        lineas.extend(f"- {l}" for l in r.limitaciones)
    if r.aviso_sanitario:
        lineas.append("")
        lineas.append(AVISO_SANITARIO)
    citadas = [e for e in r.evidencias if e.citada]
    if citadas:
        lineas.append("")
        lineas.append("Evidencias citadas:")
        for e in citadas:
            lineas.append(f"[{e.id}] {e.titulo} — {e.seccion} ({e.archivo}, fragmento {e.chunk_id})")
    if r.bibliografia:
        lineas.append("")
        lineas.append("Fuentes de los documentos citados:")
        for ref in r.bibliografia:
            ids = ", ".join(ref.ids)
            lineas.append(f"- {ref.titulo} ({ref.archivo}, revisado {ref.fecha_revision}; {ids})")
            for f in ref.fuentes:
                organismo = f" — {f['organismo']}" if f.get("organismo") else ""
                url = f" — {f['url']}" if f.get("url") else ""
                lineas.append(f"    · {f['titulo']}{organismo}{url}")
    return "\n".join(lineas)


def validar_y_renderizar(pregunta: str, evidencias: list[Evidencia], salida,
                         directorio_corpus: Path = RUTA_CORPUS) -> Respuesta:
    """Paso 3: valida la salida del modelo contra las evidencias entregadas y renderiza.

    `evidencias` son las del paso 1 (o las reconstruidas con `resolver_evidencias`).
    Si la salida no es válida, `valida=False`, `errores` y `mensaje_reparacion`
    describen qué corregir y `texto` queda vacío. Esta función no reintenta: la
    API de chat decide si reenvía la reparación al modelo o se rinde.
    """
    pregunta = pregunta.strip()
    ids_validos = {e.id for e in evidencias}
    errores = validar_salida(salida, ids_validos)
    for e in evidencias:
        e.citada = False
    if errores:
        return Respuesta(
            pregunta=pregunta, valida=False, errores=errores,
            mensaje_reparacion=mensaje_reparacion(errores), estado="sin_evidencia",
            afirmaciones=[], limitaciones=[], evidencias=evidencias, bibliografia=[],
            aviso_sanitario=False,
        )

    afirmaciones = [
        Afirmacion(texto=a["texto"].strip(), evidencias=list(dict.fromkeys(a["evidencias"])))
        for a in salida["afirmaciones"]
    ]
    citados = {ref for a in afirmaciones for ref in a.evidencias}
    for e in evidencias:
        e.citada = e.id in citados

    limitaciones = [l.strip() for l in salida["limitaciones"] if l.strip()]
    if salida["estado"] == "sin_evidencia" and not limitaciones:
        limitaciones.append(LIMITACION_SIN_EVIDENCIA)
    avisos = _avisos(pregunta, evidencias, solo_citadas=True)
    if avisos.actualidad:
        limitaciones.append(LIMITACION_ACTUALIDAD)

    r = Respuesta(
        pregunta=pregunta, valida=True, errores=[], mensaje_reparacion="",
        estado=salida["estado"], afirmaciones=afirmaciones, limitaciones=limitaciones,
        evidencias=evidencias,
        bibliografia=_bibliografia(evidencias, directorio_corpus, solo_citadas=True),
        aviso_sanitario=bool(avisos.sanitario and afirmaciones),
    )
    r.texto = renderizar(r)
    return r


# --------------------------------------------------------------------------- CLI

def main() -> None:
    parser = argparse.ArgumentParser(description="Evidencias del corpus RAG para una pregunta (sin LLM).")
    parser.add_argument("pregunta", nargs="+")
    parser.add_argument("--k", type=int, default=mod_buscar.K_POR_DEFECTO)
    parser.add_argument("--tema", choices=("salud", "normativa", "proyecto"), default=None)
    parser.add_argument("--umbral", type=float, default=None, help="distancia coseno máxima")
    parser.add_argument("--json", action="store_true", help="imprime la recuperación completa en JSON")
    args = parser.parse_args()

    r = recuperar(" ".join(args.pregunta), k=args.k, tema=args.tema, umbral=args.umbral)
    if args.json:
        print(json.dumps(r.a_dict(), ensure_ascii=False, indent=2))
        return
    print(f"Pregunta: {r.pregunta}")
    print(f"Estado: {r.estado}   (umbral {r.umbral}, descartados {r.descartados})")
    for e in r.evidencias:
        print(f"  {e.id:>3} {e.distancia:.3f}  {e.chunk_id}")
    for t in r.avisos.textos:
        print(f"  aviso: {t}")
    for ref in r.bibliografia:
        print(f"  {ref.archivo}: {len(ref.fuentes)} fuentes")


if __name__ == "__main__":
    main()
