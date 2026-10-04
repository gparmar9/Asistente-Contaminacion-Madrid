"""Mide el clasificador de intención con un proveedor real (fuera de CI: usa red y claves).

Preguntas etiquetadas a mano en `preguntas_clasificador.json`: 15 de `Preguntas.txt` y 5
escritas para CHARLA y FUERA_DE_ALCANCE (`"nueva": true`). El tema solo se compara en las
preguntas que lo llevan etiquetado.

Uso, desde la carpeta Agente/ y con el proveedor en .env:
    set -a && . ./.env && set +a
    .venv/bin/python evaluacion/evaluar_clasificador.py
"""
import asyncio
import json
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agente.business.intencion import clasificar  # noqa: E402
from agente.config.settings import get_settings  # noqa: E402
from agente.llm.cliente import crear_llm  # noqa: E402

PREGUNTAS = Path(__file__).with_name("preguntas_clasificador.json")


async def main() -> None:
    settings = get_settings()
    llm = crear_llm(settings, temperatura=0.0)
    casos = json.loads(PREGUNTAS.read_text(encoding="utf-8"))
    aciertos, temas, temas_ok, fallos = 0, 0, 0, Counter()

    print(f"Proveedor: {settings.llm_proveedor} · modelo: {settings.llm_modelo}\n")
    for caso in casos:
        inicio = time.perf_counter()
        c = await clasificar(llm, caso["pregunta"], settings.clasificador_timeout_s)
        ms = (time.perf_counter() - inicio) * 1000
        ok = c.intencion.value == caso["intencion"]
        aciertos += ok
        if not ok:
            fallos[f'{caso["intencion"]} -> {c.intencion.value}'] += 1
        if "tema" in caso:
            temas += 1
            temas_ok += c.tema.value == caso["tema"]
        marca = "ok " if ok else "MAL"
        print(f"{marca} {ms:6.0f} ms  {c.intencion.value:<16} {c.tema.value:<9} {caso['pregunta']}")

    print(f"\nIntención: {aciertos}/{len(casos)}")
    if temas:
        print(f"Tema (solo etiquetados): {temas_ok}/{temas}")
    for error, n in fallos.most_common():
        print(f"  {error}: {n}")


if __name__ == "__main__":
    asyncio.run(main())
