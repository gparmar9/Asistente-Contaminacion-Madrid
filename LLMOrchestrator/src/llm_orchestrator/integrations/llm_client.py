"""Cliente del LLM: cualquier proveedor con API OpenAI-compatible.

El proveedor se elige por configuración (LLM_BASE_URL + LLM_API_KEY +
LLM_MODEL): Mistral, Groq, Gemini u Ollama exponen esta misma interfaz de
chat/completions con `tools`, así que cambiar de proveedor no toca código.

`max_retries` del SDK reintenta con backoff exponencial los errores 429 y 5xx,
que son la fricción esperable de los tiers gratuitos.
"""
from functools import lru_cache

from openai import OpenAI

from llm_orchestrator.config.settings import get_settings


@lru_cache(maxsize=1)
def get_cliente_llm() -> OpenAI:
    settings = get_settings()
    if not (settings.llm_base_url and settings.llm_api_key and settings.llm_model):
        raise RuntimeError(
            "LLM_BASE_URL, LLM_API_KEY y LLM_MODEL deben estar configuradas "
            "para hablar con el modelo de lenguaje"
        )
    # Presupuesto de tiempo: el peor caso de una llamada es
    # (max_retries + 1) x llm_timeout_s + esperas de backoff, y el bucle hace
    # hasta MAX_ITERACIONES + 1 llamadas. ORCHESTRATOR_TIMEOUT_S de ApiUsuario
    # debe cubrir el caso típico (1-2 llamadas); ver ambos .env.example.
    return OpenAI(
        base_url=settings.llm_base_url,
        api_key=settings.llm_api_key,
        timeout=settings.llm_timeout_s,
        max_retries=2,
    )
