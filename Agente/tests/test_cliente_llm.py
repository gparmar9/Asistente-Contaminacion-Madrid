from agente.config.settings import Settings
from agente.llm.cliente import crear_llm


def test_openai_compatible_configura_tool_use():
    settings = Settings(
        app_name="Agente", app_env="test", api_version="test", api_port=8200,
        llm_proveedor="openai_compatible", llm_modelo="mistral-small", llm_base_url="https://api.ejemplo/v1",
        llm_api_key="clave-de-prueba", llm_temperatura=0.2, llm_timeout_s=30, llm_max_reintentos=2,
        aws_region="eu-west-1", aws_profile="", rag_url="", rag_timeout_s=20, max_vueltas=3,
        clasificador_timeout_s=10,
    )
    llm = crear_llm(settings)
    assert llm.metadata.is_function_calling_model and llm.metadata.model_name == "mistral-small"
