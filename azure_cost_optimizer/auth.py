from azure.identity import ClientSecretCredential

from .config import settings


def get_credential() -> ClientSecretCredential:
    """Create an Azure service principal credential from environment settings."""
    return ClientSecretCredential(
        tenant_id=settings.azure_tenant_id,
        client_id=settings.azure_client_id,
        client_secret=settings.azure_client_secret,
    )
