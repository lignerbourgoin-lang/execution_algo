"""
Generic JSON-lines WAF log analyzer.

Detects likely false positives (blocked legitimate traffic) and emits a
correction report for the WAF owner.
"""

# [FEATURE: INTEGRATION_HARDENING] Detection des faux positifs WAF.
# Raison: corriger les regles qui bloquent du trafic legitime sans toucher au WAF.
# Attention: format JSON lines generique, chaque ligne invalide est comptabilisee.

import json
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class WafFalsePositiveReport:
    """Aggregated correction report for the WAF owner."""

    total_events: int = 0
    blocked_events: int = 0
    false_positives: List[Dict[str, Any]] = field(default_factory=list)
    top_offending_rules: Dict[str, int] = field(default_factory=dict)
    malformed_lines: int = 0

    def to_report(self) -> Dict[str, Any]:
        return {
            "total_events": self.total_events,
            "blocked_events": self.blocked_events,
            "false_positive_count": len(self.false_positives),
            "top_offending_rules": self.top_offending_rules,
            "malformed_lines": self.malformed_lines,
            "recommended_actions": (
                [f"Review rule {rule_id} against legitimate traffic" for rule_id in self.top_offending_rules]
                if self.false_positives
                else ["No correction required: no false positive detected"]
            ),
        }


class WafLogAnalyzer:
    """Parses JSON-lines WAF logs and flags likely false positives."""

    BLOCKED_STATUS_CODES = (403, 406, 429)
    TRUSTED_UA_MARKERS = ("IntegrationBot",)

    def __init__(self, trusted_user_agents: List[str] = None):
        self.trusted_user_agents = tuple(
            trusted_user_agents if trusted_user_agents is not None else list(self.TRUSTED_UA_MARKERS)
        )

    def analyze(self, log_text: str) -> WafFalsePositiveReport:
        report = WafFalsePositiveReport()
        offending_rule_counter: Counter = Counter()

        for raw_line in log_text.splitlines():
            stripped_line = raw_line.strip()
            if not stripped_line:
                continue
            report.total_events += 1
            try:
                event = json.loads(stripped_line)
                status_code = int(event.get("status", 0))
            except (json.JSONDecodeError, TypeError, ValueError):
                report.malformed_lines += 1
                continue

            if status_code not in self.BLOCKED_STATUS_CODES:
                continue
            report.blocked_events += 1

            user_agent = str(event.get("user_agent", ""))
            rule_id = str(event.get("rule_id", "unknown"))
            is_trusted_agent = any(marker in user_agent for marker in self.trusted_user_agents)
            if is_trusted_agent:
                report.false_positives.append(event)
                offending_rule_counter[rule_id] += 1

        report.top_offending_rules = dict(offending_rule_counter.most_common())
        return report
