from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any

import httpx

from .settings import (
    STRIPE_SECRET_KEY,
    STRIPE_TOKEN_PRICE_CENTS,
    STRIPE_WEBHOOK_SECRET,
    STRIPE_WEBHOOK_TOLERANCE_SECONDS,
)

STRIPE_API_BASE_URL = "https://api.stripe.com/v1"


class StripeBillingError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int = 400,
        upstream: Any | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.upstream = upstream


def _as_record(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _require_secret_key() -> str:
    key = STRIPE_SECRET_KEY.strip()
    if not key:
        raise StripeBillingError("STRIPE_SECRET_KEY is not configured", status_code=500)
    return key


def _require_webhook_secret() -> str:
    secret = STRIPE_WEBHOOK_SECRET.strip()
    if not secret:
        raise StripeBillingError("STRIPE_WEBHOOK_SECRET is not configured", status_code=500)
    return secret


async def _stripe_request(
    method: str,
    path: str,
    *,
    form_data: dict[str, str] | None = None,
) -> dict[str, Any]:
    url = f"{STRIPE_API_BASE_URL}{path}"
    headers = {
        "Authorization": f"Bearer {_require_secret_key()}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.request(
                method,
                url,
                headers=headers,
                data=form_data,
            )
    except Exception as exc:
        raise StripeBillingError(f"Stripe network error: {exc}", status_code=502) from exc

    try:
        payload = response.json()
    except Exception:
        payload = {"raw": response.text}

    if response.status_code >= 400:
        message = (
            _as_record(_as_record(payload).get("error")).get("message")
            or _as_record(payload).get("message")
            or f"Stripe error: {response.status_code}"
        )
        raise StripeBillingError(
            str(message),
            status_code=response.status_code,
            upstream=payload,
        )

    return _as_record(payload)


def _parse_stripe_signature(signature_header: str) -> tuple[int, list[str]]:
    timestamp = 0
    signatures: list[str] = []
    for chunk in signature_header.split(","):
        key, _, value = chunk.partition("=")
        normalized_key = key.strip()
        normalized_value = value.strip()
        if not normalized_key or not normalized_value:
            continue
        if normalized_key == "t":
            try:
                timestamp = int(normalized_value)
            except Exception:
                timestamp = 0
        elif normalized_key == "v1":
            signatures.append(normalized_value)
    return timestamp, signatures


def verify_and_parse_webhook(payload: bytes, signature_header: str | None) -> dict[str, Any]:
    if not signature_header:
        raise StripeBillingError("Missing Stripe-Signature header", status_code=400)

    timestamp, signatures = _parse_stripe_signature(signature_header)
    if timestamp <= 0 or len(signatures) == 0:
        raise StripeBillingError("Invalid Stripe-Signature header", status_code=400)

    tolerance = max(10, STRIPE_WEBHOOK_TOLERANCE_SECONDS)
    now = int(time.time())
    if abs(now - timestamp) > tolerance:
        raise StripeBillingError("Stripe webhook signature timestamp is out of tolerance", status_code=400)

    secret = _require_webhook_secret()
    try:
        payload_text = payload.decode("utf-8")
    except Exception as exc:
        raise StripeBillingError("Invalid Stripe webhook payload encoding", status_code=400) from exc

    signed_payload = f"{timestamp}.{payload_text}".encode("utf-8")
    expected_signature = hmac.new(
        secret.encode("utf-8"),
        signed_payload,
        hashlib.sha256,
    ).hexdigest()

    if not any(hmac.compare_digest(expected_signature, candidate) for candidate in signatures):
        raise StripeBillingError("Invalid Stripe webhook signature", status_code=400)

    try:
        event = json.loads(payload_text)
    except Exception as exc:
        raise StripeBillingError("Invalid Stripe webhook payload", status_code=400) from exc

    if not isinstance(event, dict):
        raise StripeBillingError("Invalid Stripe webhook object", status_code=400)
    return event


async def create_checkout_session(
    *,
    user_id: str,
    user_email: str | None,
    token_amount: int,
    success_url: str,
    cancel_url: str,
) -> dict[str, Any]:
    normalized_tokens = int(token_amount)
    if normalized_tokens <= 0:
        raise StripeBillingError("tokenAmount must be greater than 0", status_code=400)

    unit_amount = max(1, STRIPE_TOKEN_PRICE_CENTS)
    amount_cents = unit_amount * normalized_tokens

    form_data: dict[str, str] = {
        "mode": "payment",
        "success_url": success_url,
        "cancel_url": cancel_url,
        "client_reference_id": user_id,
        "metadata[user_id]": user_id,
        "metadata[token_amount]": str(normalized_tokens),
        "metadata[checkout_type]": "token_topup",
        "line_items[0][price_data][currency]": "usd",
        "line_items[0][price_data][unit_amount]": str(unit_amount),
        "line_items[0][price_data][product_data][name]": "AI Tokens",
        "line_items[0][quantity]": str(normalized_tokens),
    }
    if user_email:
        form_data["customer_email"] = user_email.strip()

    payload = await _stripe_request("POST", "/checkout/sessions", form_data=form_data)
    session_id = str(payload.get("id") or "").strip()
    checkout_url = str(payload.get("url") or "").strip()
    if not session_id or not checkout_url:
        raise StripeBillingError(
            "Stripe checkout session response is incomplete",
            status_code=502,
            upstream=payload,
        )
    return {
        "id": session_id,
        "url": checkout_url,
        "tokenAmount": normalized_tokens,
        "amountCents": amount_cents,
        "currency": "usd",
        "raw": payload,
    }
