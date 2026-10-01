"""
WAF and Anti-Bot Challenge Detector
-----------------------------------
Detects passive and interactive anti-bot challenges:
- Cloudflare Turnstile / Managed Challenge (cf-mitigated, 403 / 503 challenge)
- DataDome device blocks (x-datadome, datadome cookie/payload)
- Akamai Ghost reference blocks
- AWS WAF Captcha / Token challenges
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Union


@dataclass
class WafDetectionResult:
    is_blocked: bool
    is_interactive_challenge: bool
    waf_name: str
    reason: str
    should_handover_to_browser: bool


def detect_waf_challenge(
    status_code: int,
    headers: Optional[Dict[str, Any]] = None,
    body_text_or_bytes: Optional[Union[str, bytes]] = None,
) -> WafDetectionResult:
    """
    Inspects response status code, HTTP headers, and response payload
    to identify whether a WAF blocked the request or requires browser interaction.
    """
    # [FEATURE: WAF_WAITING_ROOM_DETECTION] Catch Queue-it redirects and Turnstile 200 OK interstitials
    # Raison: Virtual waiting rooms (Queue-it) often return HTTP 302 redirect or 200 OK with JS queue clients.
    # Attention: Blindly assuming 200/302 is healthy causes ticket engine to crash on missing JSON tokens.
    headers_dict = {k.lower(): str(v).lower() for k, v in (headers or {}).items()}
    body_str = ""
    if body_text_or_bytes is not None:
        if isinstance(body_text_or_bytes, bytes):
            try:
                body_str = body_text_or_bytes.decode("utf-8", errors="ignore").lower()
            except Exception:
                body_str = ""
        else:
            body_str = str(body_text_or_bytes).lower()

    location_header = headers_dict.get("location", "")

    # 1. Queue-it Virtual Waiting Room Detection (can occur on 200, 302, 303, 307)
    is_queue_it = (
        "queue-it.net" in location_header
        or "queue-it.net" in body_str
        or "x-queueit-challengerange" in headers_dict
        or "queueviewmodel" in body_str
        or ("waiting-room" in location_header and "queue" in location_header)
    )
    if is_queue_it:
        return WafDetectionResult(
            is_blocked=True,
            is_interactive_challenge=True,
            waf_name="queue_it",
            reason="Queue-it virtual waiting room redirect/page detected",
            should_handover_to_browser=True,
        )

    # 2. Cloudflare Detection
    server_header = headers_dict.get("server", "")
    has_cf_ray = "cf-ray" in headers_dict
    has_cf_mitigated = "cf-mitigated" in headers_dict or headers_dict.get("cf-mitigated") == "challenge"
    is_cloudflare = "cloudflare" in server_header or has_cf_ray

    if is_cloudflare and status_code == 200:
        if any(marker in body_str for marker in ("challenges.cloudflare.com/turnstile", "turnstile-wrapper", "cf-turnstile")):
            return WafDetectionResult(
                is_blocked=True,
                is_interactive_challenge=True,
                waf_name="cloudflare",
                reason="Cloudflare Turnstile challenge interstitial detected on 200 OK",
                should_handover_to_browser=True,
            )

    if status_code in (200, 201, 204, 301, 302, 304):
        return WafDetectionResult(
            is_blocked=False,
            is_interactive_challenge=False,
            waf_name="none",
            reason="Response is healthy",
            should_handover_to_browser=False,
        )

    if is_cloudflare:
        # Turnstile or Managed Challenge
        if has_cf_mitigated or (
            status_code in (403, 503)
            and any(marker in body_str for marker in (
                "turnstile",
                "challenge-platform",
                "just a moment...",
                "challenges.cloudflare.com",
                "cf-browser-verification",
                "cf_chl_prog",
            ))
        ):
            return WafDetectionResult(
                is_blocked=True,
                is_interactive_challenge=True,
                waf_name="cloudflare",
                reason="Cloudflare Managed Challenge / Turnstile detected",
                should_handover_to_browser=True,
            )

        if status_code == 403:
            return WafDetectionResult(
                is_blocked=True,
                is_interactive_challenge=False,
                waf_name="cloudflare",
                reason="Cloudflare HTTP 403 Access Denied (IP/WAF rule)",
                should_handover_to_browser=False,
            )

    # 2. DataDome Detection
    has_datadome_header = any("datadome" in k for k in headers_dict)
    has_datadome_body = "datadome" in body_str or "captcha-delivery" in body_str
    if has_datadome_header or has_datadome_body:
        is_challenge = status_code in (403, 428) and ("captcha" in body_str or "geo.captcha-delivery" in body_str)
        return WafDetectionResult(
            is_blocked=True,
            is_interactive_challenge=is_challenge,
            waf_name="datadome",
            reason="DataDome anti-bot detection",
            should_handover_to_browser=is_challenge,
        )

    # 3. Akamai Detection
    if "akamai" in server_header or "reference #" in body_str or "akamaighost" in server_header:
        if status_code in (403, 503):
            return WafDetectionResult(
                is_blocked=True,
                is_interactive_challenge=False,
                waf_name="akamai",
                reason="Akamai Ghost access restriction",
                should_handover_to_browser=False,
            )

    # 4. AWS WAF Captcha
    if "x-amzn-waf-action" in headers_dict or "aws-waf" in body_str:
        is_captcha = "captcha" in headers_dict.get("x-amzn-waf-action", "") or "challenge" in body_str
        return WafDetectionResult(
            is_blocked=True,
            is_interactive_challenge=is_captcha,
            waf_name="aws_waf",
            reason="AWS WAF challenge/block",
            should_handover_to_browser=is_captcha,
        )

    # 5. Generic HTTP 403 or 429
    if status_code == 403:
        return WafDetectionResult(
            is_blocked=True,
            is_interactive_challenge=False,
            waf_name="generic",
            reason=f"HTTP 403 Forbidden: {body_str[:120]}",
            should_handover_to_browser=False,
        )

    return WafDetectionResult(
        is_blocked=False,
        is_interactive_challenge=False,
        waf_name="none",
        reason="No WAF blocking pattern matched",
        should_handover_to_browser=False,
    )
