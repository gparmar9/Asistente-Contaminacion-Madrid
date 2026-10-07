"""Exporta el esquema OpenAPI de `ApiUsuario` a `Web/openapi.json`.

Es la única fuente de los tipos del frontend: `npm run generar:tipos` convierte
este JSON en `src/api/schema.d.ts` con openapi-typescript. Así un cambio de
contrato en el backend se detecta al compilar, no en producción.

Uso (desde cualquier directorio, con las dependencias de ApiUsuario instaladas):

    python Web/scripts/exportar_openapi.py

No necesita base de datos ni variables de entorno: solo importa la app y
serializa `app.openapi()`. El fichero resultante se versiona; el job `frontend`
de CI lo regenera y falla si difiere del versionado (contrato desincronizado).
"""
import json
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
SRC_API = RAIZ / "ApiUsuario" / "src"
DESTINO = RAIZ / "Web" / "openapi.json"


def main() -> int:
    sys.path.insert(0, str(SRC_API))
    from api_usuario.main import app  # noqa: PLC0415  (import tras ajustar sys.path)

    esquema = app.openapi()
    # Título y versión salen de variables de entorno (APP_NAME, API_VERSION):
    # se fijan aquí para que el fichero no cambie según la máquina que lo genere.
    esquema["info"]["title"] = "ApiUsuario"
    esquema["info"]["version"] = "1.0.0"

    DESTINO.write_text(
        json.dumps(esquema, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(f"OpenAPI exportado a {DESTINO.relative_to(RAIZ)} ({len(esquema['paths'])} rutas)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
