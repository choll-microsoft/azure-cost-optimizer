"""Azure OpenAI client factory — uses AzureCliCredential (no API key required)."""

from azure.identity import AzureCliCredential, ClientSecretCredential
from openai import AzureOpenAI
from openai.lib.azure import AzureADTokenProvider

from .config import settings


def _get_token_provider() -> AzureADTokenProvider:
    """Return a token provider backed by the active Azure credential."""
    if (
        settings.azure_client_id
        and settings.azure_client_secret
        and settings.azure_tenant_id
    ):
        credential = ClientSecretCredential(
            tenant_id=settings.azure_tenant_id,
            client_id=settings.azure_client_id,
            client_secret=settings.azure_client_secret,
        )
    else:
        credential = AzureCliCredential()

    from azure.identity import get_bearer_token_provider

    return get_bearer_token_provider(
        credential, "https://cognitiveservices.azure.com/.default"
    )


def get_openai_client() -> AzureOpenAI:
    """Create an AzureOpenAI client authenticated via Azure identity."""
    return AzureOpenAI(
        azure_endpoint=settings.azure_openai_endpoint,
        azure_ad_token_provider=_get_token_provider(),
        api_version=settings.azure_openai_api_version,
    )
