from azure.identity import AzureCliCredential, ClientSecretCredential

from .config import settings


def get_credential(tenant_id: str | None = None):
    """
    Return an Azure credential.

    Priority:
    1. Tenant registry (if tenant_id is given and registered)
    2. Service principal env vars (AZURE_CLIENT_ID / SECRET / TENANT_ID)
    3. AzureCliCredential (falls back to active az login session)
    """
    if tenant_id:
        from .tenant_registry import get_credential as registry_get
        return registry_get(tenant_id)

    if settings.azure_client_id and settings.azure_client_secret and settings.azure_tenant_id:
        return ClientSecretCredential(
            tenant_id=settings.azure_tenant_id,
            client_id=settings.azure_client_id,
            client_secret=settings.azure_client_secret,
        )

    return AzureCliCredential()
