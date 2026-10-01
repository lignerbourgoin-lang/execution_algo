"""
Integration hardening toolkit (integrator side).

Legitimate building blocks for integrating with an official API behind a WAF:
service identity allowlist, authenticated HTTP client, polite request pacing,
staging WAF validation, false-positive log analysis and server-side hardening.
"""

from core.integration.allowlist import ServiceAllowlist
from core.integration.official_api_client import OAuth2ClientCredentials, OfficialApiClient
from core.integration.polite_client import PoliteClient
from core.integration.server_hardening import (
    ApiKeyAuthenticator,
    JwtAuthenticator,
    RateLimitMiddleware,
    RejectedRequestLogger,
)
from core.integration.waf_log_analyzer import WafFalsePositiveReport, WafLogAnalyzer
from core.integration.waf_staging import WafStagingConfig, WafStagingValidator

__all__ = [
    "ServiceAllowlist",
    "OfficialApiClient",
    "OAuth2ClientCredentials",
    "PoliteClient",
    "RateLimitMiddleware",
    "ApiKeyAuthenticator",
    "JwtAuthenticator",
    "RejectedRequestLogger",
    "WafStagingConfig",
    "WafStagingValidator",
    "WafLogAnalyzer",
    "WafFalsePositiveReport",
]
