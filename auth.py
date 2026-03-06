from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
from typing import Any

import httpx

from .db import db
from .settings import GOOGLE_CLIENT_ID, JWT_EXPIRES_HOURS, JWT_SECRET


class AuthError(Exception):
    pass


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _b64url_decode(raw: str) -> bytes:
    padding = "=" * ((4 - len(raw) % 4) % 4)
    return base64.urlsafe_b64decode((raw + padding).encode("utf-8"))


def hash_password(password: str) -> str:
    if not password:
        raise AuthError("Password is required")
    iterations = 210_000
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2_sha256${iterations}${_b64url_encode(salt)}${_b64url_encode(digest)}"


def verify_password(password: str, encoded: str) -> bool:
    try:
        scheme, iterations_raw, salt_raw, digest_raw = encoded.split("$", 3)
        if scheme != "pbkdf2_sha256":
            return False
        iterations = int(iterations_raw)
        salt = _b64url_decode(salt_raw)
        expected_digest = _b64url_decode(digest_raw)
        actual_digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
        return hmac.compare_digest(actual_digest, expected_digest)
    except Exception:
        return False


def _jwt_sign(message: bytes) -> str:
    if not JWT_SECRET:
        raise AuthError("JWT secret is not configured")
    signature = hmac.new(JWT_SECRET.encode("utf-8"), message, hashlib.sha256).digest()
    return _b64url_encode(signature)


def create_access_token(payload: dict[str, Any]) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    message = f"{_b64url_encode(json.dumps(header, separators=(',', ':')).encode('utf-8'))}.{_b64url_encode(json.dumps(payload, separators=(',', ':')).encode('utf-8'))}".encode(
        "utf-8"
    )
    signature = _jwt_sign(message)
    return f"{message.decode('utf-8')}.{signature}"


def parse_access_token(token: str) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) != 3:
        raise AuthError("Invalid token format")
    message = f"{parts[0]}.{parts[1]}".encode("utf-8")
    expected = _jwt_sign(message)
    if not hmac.compare_digest(expected, parts[2]):
        raise AuthError("Invalid token signature")
    payload_raw = _b64url_decode(parts[1])
    payload = json.loads(payload_raw.decode("utf-8"))
    if not isinstance(payload, dict):
        raise AuthError("Invalid token payload")
    exp = int(payload.get("exp") or 0)
    if exp <= int(time.time()):
        raise AuthError("Token expired")
    return payload


def sanitize_user(user: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": user.get("id"),
        "email": user.get("email"),
        "name": user.get("name"),
        "provider": user.get("provider"),
        "createdAt": user.get("createdAt"),
    }


def issue_token_for_user(user: dict[str, Any]) -> str:
    now = int(time.time())
    payload = {
        "sub": user.get("id"),
        "email": user.get("email"),
        "iat": now,
        "exp": now + max(1, JWT_EXPIRES_HOURS) * 3600,
    }
    return create_access_token(payload)


def get_user_by_access_token(token: str) -> dict[str, Any]:
    payload = parse_access_token(token)
    user_id = str(payload.get("sub") or "").strip()
    if not user_id:
        raise AuthError("Invalid token subject")
    user = db.get_user_by_id(user_id)
    if not user:
        raise AuthError("User not found")
    return user


async def verify_google_id_token(id_token: str) -> dict[str, str]:
    if not GOOGLE_CLIENT_ID:
        raise AuthError("GOOGLE_CLIENT_ID is not configured")

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(
                "https://oauth2.googleapis.com/tokeninfo",
                params={"id_token": id_token},
            )
    except Exception as exc:
        raise AuthError(f"Failed to verify Google token: {exc}") from exc

    if response.status_code >= 400:
        raise AuthError("Invalid Google token")

    payload: Any
    try:
        payload = response.json()
    except Exception as exc:
        raise AuthError("Invalid Google verify response") from exc

    if not isinstance(payload, dict):
        raise AuthError("Invalid Google payload")

    aud = str(payload.get("aud") or "")
    if aud != GOOGLE_CLIENT_ID:
        raise AuthError("Google token audience mismatch")

    email_verified = payload.get("email_verified")
    if email_verified not in (True, "true", "True", "1", 1):
        raise AuthError("Google email is not verified")

    sub = str(payload.get("sub") or "").strip()
    email = str(payload.get("email") or "").strip().lower()
    name = str(payload.get("name") or "").strip()
    picture = str(payload.get("picture") or "").strip()

    if not sub or not email:
        raise AuthError("Google token missing required fields")

    return {
        "sub": sub,
        "email": email,
        "name": name,
        "picture": picture,
    }
