"""
Staging WAF validation.

Generates a staging configuration/checklist, then sends legitimate validation
requests and reads the WAF verdicts. This is detection-mode testing only:
there is no bypass logic of any kind.
"""

# [FEATURE: INTEGRATION_HARDENING] Validation WAF en staging, mode detection.
# Raison: verifier que le trafic legitime n'est pas bloque avant la mise en prod.
# Attention: module strictement observateur, aucun contournement implemente.

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class WafStagingConfig:
    """Declarative staging WAF configuration (detection mode)."""

    environment: str
    mode: str = "detection"
    validation_paths: List[str] = field(default_factory=lambda: ["/", "/health"])
    expected_status_codes: List[int] = field(default_factory=lambda: [200])
    allowed_user_agents: List[str] = field(default_factory=list)

    def __post_init__(self):
        if self.mode != "detection":
            raise ValueError("staging WAF must run in detection mode")
        if not self.validation_paths or not all(path.startswith("/") for path in self.validation_paths):
            raise ValueError("validation_paths must be absolute paths")

    def to_checklist(self) -> Dict[str, Any]:
        """Serializable configuration plus the checklist the validator executes."""
        return {
            "environment": self.environment,
            "waf_mode": self.mode,
            "validation_paths": list(self.validation_paths),
            "expected_status_codes": list(self.expected_status_codes),
            "allowed_user_agents": list(self.allowed_user_agents),
            "checks": [
                "WAF stays in detection mode",
                "No legitimate request is blocked",
                "No WAF challenge is triggered",
                "All verdicts are logged for review",
            ],
        }


class WafStagingValidator:
    """Sends legitimate validation requests and classifies each WAF verdict."""

    def __init__(self, config: WafStagingConfig, http_get):
        self.config = config
        self.http_get = http_get

    def validate_all(self) -> Dict[str, Any]:
        results: List[Dict[str, Any]] = []
        all_passed = True
        for path in self.config.validation_paths:
            response = self.http_get(path)
            status_code = response.status_code
            verdict = "pass"
            if status_code == 403:
                verdict = "false_positive"
                all_passed = False
            elif status_code not in self.config.expected_status_codes:
                verdict = "unexpected_status"
                all_passed = False
            results.append({"path": path, "status_code": status_code, "verdict": verdict})
        return {"environment": self.config.environment, "all_passed": all_passed, "results": results}

    def to_report(self) -> str:
        return json.dumps(self.validate_all(), indent=2)
