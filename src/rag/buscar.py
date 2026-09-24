"""Búsqueda por vector sobre el índice Chroma del corpus.

Uso por consola:
    python -m rag.buscar "¿qué son los bloques del día?"
    python -m rag.buscar --k 6 --tema salud "¿puedo correr si soy asmático?"

`buscar` devuelve una lista de dicts {chunk_id, texto, metadatos, distancia},
ordenada por distancia coseno ascendente (0 = idéntico). Los IDs son los
`chunk_id` estables del corpus (plan §2.4) y son lo que `responder` (fase B)
numerará como D1..Dn.

Errores tipados (rag.errores):
    ConsultaInvalida   consulta vacía, k fuera de rango, tema desconocido, consulta
                       más larga que el presupuesto de tokens
    IndiceNoPreparado  no existe el índice: ejecutar `python -m rag.indexar`
    ModeloNoCoincide   el índice se construyó con otro modelo de embeddings
"""
from __future__ import annotations

import argparse
from pathlib import Path

from rag.corpus import TEMAS
from rag.embeddings import (
    MODELO_EMBEDDINGS,
    contar_tokens,
    embed_consulta,
    obtener_coleccion,
    presupuesto_tokens,
)
from rag.errores import ConsultaInvalida, ModeloNoCoincide
from rag.indexar import metadatos_desde_chroma

K_POR_DEFECTO = 4
K_MAXIMO = 20


def _validar(consulta: str, k: int, tema: str | None) -> str:
    if not isinstance(consulta, str) or not consulta.strip():
        raise ConsultaInvalida("la consulta no puede estar vacía")
    if isinstance(k, bool) or not isinstance(k, int) or not 1 <= k <= K_MAXIMO:
        raise ConsultaInvalida(f"k debe ser un entero entre 1 y {K_MAXIMO}, no {k!r}")
    if tema is not None and tema not in TEMAS:
        raise ConsultaInvalida(f"tema debe ser uno de {list(TEMAS)}, no {tema!r}")
    return consulta.strip()


def buscar(consulta: str, k: int = K_POR_DEFECTO, tema: str | None = None,
           ruta_chroma: Path | None = None) -> list[dict]:
    """Devuelve los k fragmentos más cercanos a la consulta (opcionalmente por tema)."""
    consulta = _validar(consulta, k, tema)

    # Comprobaciones sobre el índice antes de cargar el modelo (más barato y más claro).
    coleccion = obtener_coleccion(ruta_chroma)
    modelo_indice = (coleccion.metadata or {}).get("modelo_embeddings")
    if modelo_indice != MODELO_EMBEDDINGS:
        raise ModeloNoCoincide(
            f"El índice se construyó con '{modelo_indice}' y la configuración usa "
            f"'{MODELO_EMBEDDINGS}'. Ejecuta `python -m rag.indexar` o ajusta "
            f"RAG_MODELO_EMBEDDINGS."
        )

    presupuesto = presupuesto_tokens()
    n_tokens = contar_tokens(consulta, consulta=True)
    if n_tokens > presupuesto:
        raise ConsultaInvalida(
            f"la consulta tiene {n_tokens} tokens y el máximo es {presupuesto}; acórtala"
        )

    total = coleccion.count()
    if total == 0:
        return []
    resultado = coleccion.query(
        query_embeddings=[embed_consulta(consulta)],
        n_results=min(k, total),
        where={"tema": tema} if tema else None,
        include=["documents", "metadatas", "distances"],
    )
    return [
        {
            "chunk_id": chunk_id,
            "texto": doc,
            "metadatos": metadatos_desde_chroma(meta),
            "distancia": float(dist),
        }
        for chunk_id, doc, meta, dist in zip(
            resultado["ids"][0],
            resultado["documents"][0],
            resultado["metadatas"][0],
            resultado["distances"][0],
        )
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Búsqueda semántica en el corpus RAG.")
    parser.add_argument("consulta", nargs="+", help="pregunta en lenguaje natural")
    parser.add_argument("--k", type=int, default=K_POR_DEFECTO)
    parser.add_argument("--tema", choices=TEMAS, default=None)
    parser.add_argument("--texto", action="store_true", help="imprime también el texto del fragmento")
    args = parser.parse_args()

    consulta = " ".join(args.consulta)
    print(f"Consulta: {consulta}\n")
    for r in buscar(consulta, k=args.k, tema=args.tema):
        m = r["metadatos"]
        print(f"[{r['distancia']:.3f}] {r['chunk_id']}  ({m['n_tokens']} tokens)")
        if args.texto:  # el texto ya empieza por "título — sección"
            print("        " + r["texto"].replace("\n", "\n        ") + "\n")
        else:
            print(f"        {m['titulo']} — {m['seccion']}")


if __name__ == "__main__":
    main()
