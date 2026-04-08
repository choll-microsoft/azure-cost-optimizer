"""
Tenant registry — stores per-tenant authentication configuration locally.

Supports two auth methods:
  cli  — uses the current az CLI session for that tenant (no secrets stored)
  sp   — stores service principal credentials in a local JSON file

Secrets are never written to the Git repository (.gitignore covers config/).
"""

import json
from pathlib import Path
from typing import Literal

from azure.identity import AzureCliCredential, ClientSecretCredential

# Local config directory — gitignored
_CONFIG_PATH = Path("config/tenants.json")

AuthMethod = Literal["cli", "sp"]


def _load() -> dict:
    if _CONFIG_PATH.exists():
        try:
            return json.loads(_CONFIG_PATH.read_text())
        except Exception:
            pass
    return {}


def _save(data: dict) -> None:
    _CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    _CONFIG_PATH.write_text(json.dumps(data, indent=2))


# ──────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────

def list_tenants() -> list[dict]:
    """
    Return all configured tenants as a list of dicts:
      {tenant_id, display_name, auth_method, client_id?}
    Client secrets are never returned.
    """
    data = _load()
    result = []
    for tid, cfg in data.items():
        entry = {
            "tenant_id": tid,
            "display_name": cfg.get("display_name", tid[:8] + "…"),
            "auth_method": cfg.get("auth_method", "cli"),
        }
        if cfg.get("client_id"):
            entry["client_id"] = cfg["client_id"]
        result.append(entry)
    return result


def add_cli_tenant(tenant_id: str, display_name: str) -> None:
    """Register a tenant that will authenticate via the active az CLI session."""
    data = _load()
    data[tenant_id] = {
        "display_name": display_name,
        "auth_method": "cli",
    }
    _save(data)


def add_sp_tenant(
    tenant_id: str,
    display_name: str,
    client_id: str,
    client_secret: str,
) -> None:
    """Register a tenant with Service Principal credentials."""
    data = _load()
    data[tenant_id] = {
        "display_name": display_name,
        "auth_method": "sp",
        "client_id": client_id,
        "client_secret": client_secret,   # stored locally only, never committed
    }
    _save(data)


def remove_tenant(tenant_id: str) -> None:
    data = _load()
    data.pop(tenant_id, None)
    _save(data)


def get_credential(tenant_id: str):
    """
    Return the correct Azure credential for the given tenant.
    Falls back to AzureCliCredential if no config found.
    """
    data = _load()
    cfg = data.get(tenant_id, {})
    method = cfg.get("auth_method", "cli")

    if method == "sp":
        return ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=cfg["client_id"],
            client_secret=cfg["client_secret"],
        )

    # cli — scoped to the specific tenant
    return AzureCliCredential(tenant_id=tenant_id)


def is_registered(tenant_id: str) -> bool:
    return tenant_id in _load()
