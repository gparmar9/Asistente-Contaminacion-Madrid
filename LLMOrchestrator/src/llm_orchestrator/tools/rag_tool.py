"""Tool `search_documents`: búsqueda en el corpus de salud y normativa (Fase 2).

Envuelve `buscar_documentos` (ChromaDB + embeddings) validando los argumentos
que genera el LLM y filtrando resultados poco relevantes, para que el modelo
reciba solo fragmentos citables.
"""
from llm_orchestrator.integrations import rag

# Distancia coseno máxima para considerar un fragmento relevante (0 = idéntico).
# Por encima de este umbral el fragmento se descarta en vez de dárselo al LLM.
UMBRAL_DISTANCIA = 0.8

# Tamaño máximo de cada fragmento devuelto al LLM (controla los tokens por vuelta)
MAX_CARACTERES_FRAGMENTO = 1200

ESQUEMA_TOOL = {
    "type": "function",
    "function": {
        "name": "search_documents",
        "description": (
            "Busca en el corpus documental del proyecto: salud (efectos de los "
            "contaminantes, recomendaciones por perfil, alergias), normativa "
            "(límites OMS 2021 y UE, protocolo anticontaminación de Madrid) y "
            "contexto (estaciones y zonas, glosario). Usa esta tool para "
            "preguntas de salud o normativa; para cifras de mediciones usa query_sql."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "consulta": {
                    "type": "string",
                    "description": "Qué buscar, en lenguaje natural y en español",
                },
                "k": {
                    "type": "integer",
                    "description": "Cuántos fragmentos devolver (1-10, por defecto 4)",
                },
                "tema": {
                    "type": "string",
                    "description": ("Filtro opcional por el campo 'tema' del corpus. "
                                    "Si no conoces el valor exacto, no lo uses: sin filtro "
                                    "se busca en todo el corpus"),
                },
            },
            "required": ["consulta"],
        },
    },
}


def search_documents(consulta: str = "", k: int = 4, tema: str | None = None) -> dict:
    """Busca fragmentos relevantes. Errores como {'error': ...}, nunca excepción."""
    if not isinstance(consulta, str) or not consulta.strip():
        return {"error": "'consulta' no puede estar vacía"}
    try:
        k = int(k)
    except (TypeError, ValueError):
        return {"error": f"'k' debe ser un entero entre 1 y 10, no {k!r}"}
    if not 1 <= k <= 10:
        return {"error": f"'k' debe estar entre 1 y 10, no {k}"}
    if tema is not None:
        tema = str(tema).strip() or None  # espacios accidentales del LLM fuera

    try:
        fragmentos = rag.buscar_en_corpus(consulta.strip(), k=k, tema=tema)
    except Exception as exc:  # índice ausente, modelo no descargado...
        return {"error": f"La búsqueda documental falló: {exc}"}

    relevantes = [f for f in fragmentos if f.get("distancia", 1.0) <= UMBRAL_DISTANCIA]
    if not relevantes:
        return {"resultados": [],
                "mensaje": "No se encontraron documentos relevantes para esa consulta"}

    return {"resultados": [
        {
            "texto": str(f["texto"])[:MAX_CARACTERES_FRAGMENTO],
            "titulo": f.get("metadatos", {}).get("titulo", "documento sin título"),
            "seccion": f.get("metadatos", {}).get("seccion", ""),
            "fuente": f.get("metadatos", {}).get("fuente", ""),
            "distancia": round(float(f.get("distancia", 0.0)), 3),
        }
        for f in relevantes
    ]}
