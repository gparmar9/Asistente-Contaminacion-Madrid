"""Fábrica del cliente LLM. El resto del agente ve siempre un `FunctionCallingLLM` de LlamaIndex.

- `openai_compatible`: cualquier API con interfaz OpenAI (Mistral API, Groq, Ollama...).
- `bedrock`: Amazon Bedrock por la API Converse; credenciales del entorno, de un perfil
  (`AWS_PROFILE`) o del rol de la instancia EC2. Sin probar aún contra AWS (fase 7).

Los paquetes de cada proveedor se importan de forma perezosa: los tests no cargan `boto3`.
"""
from __future__ import annotations

from dataclasses import replace

from llama_index.core.llms.function_calling import FunctionCallingLLM

from agente.config.settings import PROVEEDORES, Settings


class ConfiguracionLLMInvalida(ValueError):
    pass


def crear_llm(settings: Settings, temperatura: float | None = None, modelo: str = "") -> FunctionCallingLLM:
    """`temperatura` None = la de la configuración (síntesis); el clasificador y el redactor SQL
    piden 0. `modelo` vacío = `LLM_MODELO`; el redactor SQL pasa `LLM_MODELO_SQL` y el clasificador,
    `LLM_MODELO_CLASIFICADOR`."""
    t = settings.llm_temperatura if temperatura is None else temperatura
    if modelo:
        settings = replace(settings, llm_modelo=modelo)
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
        max_tokens=settings.llm_max_tokens,
        timeout=settings.llm_timeout_s,
        max_retries=settings.llm_max_reintentos,
        # Sin estas dos marcas LlamaIndex trataría el modelo como de completado sin tools.
        is_chat_model=True,
        is_function_calling_model=True,
        # En streaming los tokens solo llegan si se piden (último trozo). LlamaIndex lo quita en
        # las llamadas sin stream. Que cada proveedor lo respete está [por confirmar].
        additional_kwargs={"stream_options": {"include_usage": True}},
    )


def _bedrock(settings: Settings, temperatura: float) -> FunctionCallingLLM:
    if not settings.llm_modelo:
        raise ConfiguracionLLMInvalida("Con LLM_PROVEEDOR=bedrock hace falta LLM_MODELO (ID del modelo o perfil de inferencia)")
    from llama_index.core.base.llms.types import LLMMetadata
    from llama_index.llms.bedrock_converse import BedrockConverse
    from llama_index.llms.bedrock_converse import utils as bedrock_utils

    # Con un perfil de inferencia (`eu.`, `global.`...) LlamaIndex exige que el modelo base esté
    # en su lista fija y el constructor falla con los nuevos (p. ej. eu.anthropic.claude-haiku-5-5,
    # 0.15.3). Se añade el modelo base a esa lista; Bedrock valida el perfil en la llamada.
    prefijo, _, base = settings.llm_modelo.partition(".")
    if prefijo in ("us", "us-gov", "eu", "apac", "jp", "global", "ca", "au") \
            and base not in bedrock_utils.BEDROCK_INFERENCE_PROFILE_SUPPORTED_MODELS:
        bedrock_utils.BEDROCK_INFERENCE_PROFILE_SUPPORTED_MODELS += (base,)

    class _BedrockConverse(BedrockConverse):
        # LlamaIndex lleva una lista fija de modelos y `metadata` lanza `ValueError: Unknown model`
        # con los que no conoce (p. ej. mistral.ministral-3-14b-instruct). Fuera de la lista se
        # declaran a mano; el uso de herramientas lo confirma (o no) la prueba contra Bedrock.
        @property
        def metadata(self) -> LLMMetadata:
            try:
                return super().metadata
            except ValueError:
                return LLMMetadata(
                    context_window=32_000,  # conservador; el agente no lo usa todavía
                    num_output=self.max_tokens,
                    is_chat_model=True,
                    model_name=self.model,
                    is_function_calling_model=True,
                )

    return _BedrockConverse(
        model=settings.llm_modelo,
        region_name=settings.aws_region,
        profile_name=settings.aws_profile or None,
        temperature=temperatura,
        max_tokens=settings.llm_max_tokens,
        timeout=settings.llm_timeout_s,
        max_retries=settings.llm_max_reintentos,
    )
