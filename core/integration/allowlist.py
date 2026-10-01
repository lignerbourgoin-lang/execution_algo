"""
Service identity allowlist.

Loaded from a JSON config file. The service fails closed: a missing,
unreadable or malformed file denies every identity rather than allowing all.
"""

# [FEATURE: INTEGRATION_HARDENING] Allowlist stricte avec echec ferme.
# Raison: une integration legitime doit prouver son identite service,
#         et ne jamais s'ouvrir par defaut si la config est absente.
# Attention: le fichier JSON est la source de verite unique; pas de fallback implicite.

import ipaddress
import json
from pathlib import Path
from typing import Iterable, Union


class ServiceAllowlist:
    """Strict allowlist of IPv4/IPv6 addresses and service identities."""

    def __init__(
        self,
        allowed_ips: Iterable[str] = (),
        allowed_identities: Iterable[str] = (),
    ):
        self.allowed_ips = frozenset(ipaddress.ip_address(raw) for raw in allowed_ips)
        self.allowed_identities = frozenset(str(raw) for raw in allowed_identities)

    @classmethod
    def from_json_file(cls, config_path: Union[str, Path]) -> "ServiceAllowlist":
        """Load the allowlist; any missing or malformed file yields an empty (deny-all) list."""
        try:
            raw_config = json.loads(Path(config_path).read_text(encoding="utf-8"))
            allowed_ips = raw_config.get("allowed_ips", [])
            allowed_identities = raw_config.get("allowed_identities", [])
            if not isinstance(allowed_ips, list) or not isinstance(allowed_identities, list):
                raise ValueError("allowlist entries must be lists")
            return cls(allowed_ips=allowed_ips, allowed_identities=allowed_identities)
        except (OSError, ValueError, TypeError):
            return cls()

    def is_ip_allowed(self, ip: str) -> bool:
        try:
            return ipaddress.ip_address(ip) in self.allowed_ips
        except ValueError:
            return False

    def is_identity_allowed(self, identity: str) -> bool:
        return str(identity) in self.allowed_identities

    def is_request_allowed(self, ip: str, identity: str) -> bool:
        return self.is_ip_allowed(ip) and self.is_identity_allowed(identity)
