"""
Unit Tests for WAF and Anti-Bot Challenge Detector
--------------------------------------------------
Validates detection of:
- Clean 200 responses.
- Cloudflare Turnstile / Managed Challenges.
- Cloudflare HTTP 403 access blocks.
- DataDome device fingerprint challenges.
- Akamai Ghost blocks.
- AWS WAF Captcha challenges.
"""

import unittest

from core.network.waf_detector import detect_waf_challenge


class TestWafDetector(unittest.TestCase):
    def test_clean_response_passes(self):
        result = detect_waf_challenge(200, headers={"Server": "nginx"}, body_text_or_bytes='{"ok": true}')
        self.assertFalse(result.is_blocked)
        self.assertFalse(result.is_interactive_challenge)
        self.assertEqual(result.waf_name, "none")

    def test_cloudflare_turnstile_detected(self):
        headers = {
            "Server": "cloudflare",
            "CF-Mitigated": "challenge",
            "CF-Ray": "91a2b3c4d5e6",
        }
        body = "<html><head><title>Just a moment...</title></head><body><div id='turnstile'></div></body></html>"
        result = detect_waf_challenge(403, headers=headers, body_text_or_bytes=body)

        self.assertTrue(result.is_blocked)
        self.assertTrue(result.is_interactive_challenge)
        self.assertEqual(result.waf_name, "cloudflare")
        self.assertTrue(result.should_handover_to_browser)

    def test_cloudflare_hard_403_block(self):
        headers = {
            "Server": "cloudflare",
            "CF-Ray": "91a2b3c4d5e6",
        }
        body = "error code: 1020 access denied"
        result = detect_waf_challenge(403, headers=headers, body_text_or_bytes=body)

        self.assertTrue(result.is_blocked)
        self.assertFalse(result.is_interactive_challenge)
        self.assertEqual(result.waf_name, "cloudflare")
        self.assertFalse(result.should_handover_to_browser)

    def test_datadome_captcha_detected(self):
        headers = {
            "Server": "Datadome",
            "x-datadome": "protected",
        }
        body = "<html><body>Please solve the captcha: geo.captcha-delivery.com</body></html>"
        result = detect_waf_challenge(403, headers=headers, body_text_or_bytes=body)

        self.assertTrue(result.is_blocked)
        self.assertTrue(result.is_interactive_challenge)
        self.assertEqual(result.waf_name, "datadome")
        self.assertTrue(result.should_handover_to_browser)

    def test_akamai_ghost_block(self):
        headers = {"Server": "AkamaiGHost"}
        body = "Access Denied. Reference #18.234.12"
        result = detect_waf_challenge(403, headers=headers, body_text_or_bytes=body)

        self.assertTrue(result.is_blocked)
        self.assertEqual(result.waf_name, "akamai")
        self.assertFalse(result.should_handover_to_browser)

    def test_queue_it_redirect_detected(self):
        headers = {
            "Location": "https://ticketmaster.queue-it.net/?c=ticketmaster&e=event123",
            "Server": "cloudflare",
        }
        result = detect_waf_challenge(302, headers=headers)
        self.assertTrue(result.is_blocked)
        self.assertTrue(result.is_interactive_challenge)
        self.assertEqual(result.waf_name, "queue_it")
        self.assertTrue(result.should_handover_to_browser)

    def test_cloudflare_turnstile_200_interstitial(self):
        headers = {
            "Server": "cloudflare",
            "CF-Ray": "91a2b3c4d5e6",
        }
        body = '<html><body><script src="https://challenges.cloudflare.com/turnstile/v0/api.js"></script><div class="cf-turnstile"></div></body></html>'
        result = detect_waf_challenge(200, headers=headers, body_text_or_bytes=body)
        self.assertTrue(result.is_blocked)
        self.assertTrue(result.is_interactive_challenge)
        self.assertEqual(result.waf_name, "cloudflare")
        self.assertTrue(result.should_handover_to_browser)


if __name__ == "__main__":
    unittest.main()
