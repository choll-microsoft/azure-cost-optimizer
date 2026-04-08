from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Service principal fields are optional — if omitted, AzureCliCredential is used
    azure_tenant_id: str | None = None
    azure_client_id: str | None = None
    azure_client_secret: str | None = None
    azure_subscription_id: str

    # Azure OpenAI
    azure_openai_endpoint: str = "https://spare-part-ai-oai-w3x7t3r7c5sji.openai.azure.com/"
    azure_openai_deployment: str = "gpt-4o"
    azure_openai_api_version: str = "2024-12-01-preview"

    # Anthropic (optional fallback)
    anthropic_api_key: str | None = None

    lookback_days: int = 30
    output_dir: str = "./outputs"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
