"""RAG documental del asistente de calidad del aire de Madrid.

Módulos:
    corpus      lectura y troceado de data/rag/*.md (lógica pura, sin modelo)
    embeddings  modelo de embeddings, recuento de tokens y colección Chroma
    indexar     comando que reconstruye el índice completo
    buscar      búsqueda por vector con filtros; devuelve chunk_id, texto y metadatos
    evidencias  recuperar evidencias D1..Dn con umbral y bibliografía; validar la salida del
                modelo y renderizar con [Dn] (lógica pura, sin LLM)
    api         servicio FastAPI: POST /rag/evidencias y POST /rag/validar para la API de chat
    errores     excepciones tipadas del paquete
"""
