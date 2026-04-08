from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    azure_tenant_id: str
    azure_client_id: str
    azure_client_secret: str
    azure_subscription_id: str
    anthropic_api_key: str
    lookback_days: int = 30
    output_dir: str = "./outputs"
    claude_model: str = "claude-sonnet-4-6"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
