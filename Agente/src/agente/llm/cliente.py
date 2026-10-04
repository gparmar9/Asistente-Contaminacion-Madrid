"""Fábrica del cliente LLM. El resto del agente ve siempre un `FunctionCallingLLM` de LlamaIndex.

- `openai_compatible`: cualquier API con interfaz OpenAI (Mistral API, Groq, Ollama...).
- `bedrock`: Amazon Bedrock por la API Converse; credenciales del entorno, de un perfil
  (`AWS_PROFILE`) o del rol de la instancia EC2. Sin probar aún contra AWS (fase 7).

Los paquetes de cada proveedor se importan de forma perezosa: los tests no cargan `boto3`.
"""
from __future__ import annotations

from llama_index.core.llms.function_calling import FunctionCallingLLM

from agente.config.settings import PROVEEDORES, Settings


class ConfiguracionLLMInvalida(ValueError):
    pass


def crear_llm(settings: Settings, temperatura: float | None = None) -> FunctionCallingLLM:
    """`temperatura` None = la de la configuración (síntesis); el clasificador pide 0."""
    t = settings.llm_temperatura if temperatura is None else temperatura
    if settings.llm_proveedor == "openai_compatible":
        return _openai_compatible(settings, t)
    if settings.llm_proveedor == "bedrock":
        return _bedrock(settings, t)
    raise ConfiguracionLLMInvalida(
        f"LLM_PROVEEDOR={settings.llm_proveedor!r} no es válido; opciones: {', '.join(PROVEEDORES)}"
    )


def _openai_compatible(settings: Settings, temperatura: float) -> FunctionCallingLLM:
    if not (settings.llm_modelo and settings.llm_base_url and settings.llm_api_key):
        raise ConfiguracionLLMInvalida(
            "Con LLM_PROVEEDOR=openai_compatible hacen falta LLM_MODELO, LLM_BASE_URL y LLM_API_KEY"
        )
    from llama_index.llms.openai_like import OpenAILike

    return OpenAILike(
        model=settings.llm_modelo,
        api_base=settings.llm_base_url,
        api_key=settings.llm_api_key,
        temperature=temperatura,
        timeout=settings.llm_timeout_s,
        max_retries=settings.llm_max_reintentos,
        # Sin estas dos marcas LlamaIndex trataría el modelo como de completado sin tools.
        is_chat_model=True,
        is_function_calling_model=True,
    )


def _bedrock(settings: Settings, temperatura: float) -> FunctionCallingLLM:
    if not settings.llm_modelo:
        raise ConfiguracionLLMInvalida("Con LLM_PROVEEDOR=bedrock hace falta LLM_MODELO (ID del modelo o perfil de inferencia)")
    from llama_index.llms.bedrock_converse import BedrockConverse

    return BedrockConverse(
        model=settings.llm_modelo,
        region_name=settings.aws_region,
        profile_name=settings.aws_profile or None,
        temperature=temperatura,
        timeout=settings.llm_timeout_s,
        max_retries=settings.llm_max_reintentos,
    )
