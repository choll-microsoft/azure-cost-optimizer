from azure.identity import AzureCliCredential, ClientSecretCredential

from .config import settings


def get_credential() -> ClientSecretCredential | AzureCliCredential:
    """
    Return an Azure credential.

    Uses ClientSecretCredential when service principal env vars are set,
    otherwise falls back to AzureCliCredential (picks up `az login` session).
    """
    if settings.azure_client_id and settings.azure_client_secret and settings.azure_tenant_id:
        return ClientSecretCredential(
            tenant_id=settings.azure_tenant_id,
            client_id=settings.azure_client_id,
            client_secret=settings.azure_client_secret,
        )
    return AzureCliCredential()
