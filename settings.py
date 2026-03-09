from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parent
ROOT_DIR = BACKEND_DIR.parent

# Single source of env config.
load_dotenv(BACKEND_DIR / ".env", override=False)

DATA_DIR = BACKEND_DIR / "data"
LEGACY_DATA_DIR = ROOT_DIR / "data"
PUBLIC_DIR = BACKEND_DIR / "public"


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except Exception:
        return default


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw.strip())
    except Exception:
        return default


def _package_price_map_env(
    name: str,
    default: str = "200:1900,320:3000,540:5000",
) -> dict[int, int]:
    raw = os.getenv(name, default)
    result: dict[int, int] = {}
    for chunk in raw.split(","):
        part = chunk.strip()
        if not part:
            continue
        token_part, sep, cents_part = part.partition(":")
        if not sep:
            continue
        try:
            token_amount = int(token_part.strip())
            amount_cents = int(cents_part.strip())
        except Exception:
            continue
        if token_amount > 0 and amount_cents > 0:
            result[token_amount] = amount_cents
    return result

UPLOAD_API_BASE_URL = os.getenv("UPLOAD_API_BASE_URL", "https://stage.neuro-x.online/api").rstrip("/")
UPLOAD_POST_API_BASE_URL = os.getenv(
    "UPLOAD_POST_API_BASE_URL",
    "https://api.upload-post.com/api",
).rstrip("/")
UPLOAD_POST_API_KEY = os.getenv("UPLOAD_POST_API_KEY", "").strip()

NANO_BANANO_API_KEY = os.getenv("NANO_BANANO_API_KEY", "")
NANO_BANANO_BASE_URL = os.getenv("NANO_BANANO_BASE_URL", "https://api.kie.ai/api/v1").rstrip("/")
NANO_BANANO_VEO_BASE_URL = os.getenv("NANO_BANANO_VEO_BASE_URL", "https://api.kie.ai").rstrip("/")
KIE_FILE_UPLOAD_BASE_URL = os.getenv("KIE_FILE_UPLOAD_BASE_URL", "https://kieai.redpandaai.co").rstrip("/")
NANO_BANANO_CALLBACK_URL = os.getenv("NANO_BANANO_CALLBACK_URL")

PUBLIC_APP_URL = (
    os.getenv("NANO_BANANO_PUBLIC_BASE_URL")
    or os.getenv("PUBLIC_APP_URL")
    or os.getenv("NEXT_PUBLIC_APP_URL")
    or "http://localhost:8000"
)

FRONTEND_ORIGINS = [
    item.strip()
    for item in os.getenv(
        "FRONTEND_ORIGINS",
        "http://localhost:5173,http://127.0.0.1:5173",
    ).split(",")
    if item.strip()
]
FRONTEND_APP_URL = (
    os.getenv("FRONTEND_APP_URL", "").strip().rstrip("/")
    or (FRONTEND_ORIGINS[0].rstrip("/") if FRONTEND_ORIGINS else "http://localhost:5173")
)

POLL_INTERVAL_SECONDS = 3
POLL_MAX_ATTEMPTS = 100
NETWORK_RETRY_ATTEMPTS = 3
NETWORK_RETRY_DELAY_SECONDS = 1

JWT_SECRET = os.getenv("JWT_SECRET", "dev-change-me")
JWT_EXPIRES_HOURS = int(os.getenv("JWT_EXPIRES_HOURS", "24"))
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "").strip()

TOKEN_INITIAL_BALANCE = _int_env("TOKEN_INITIAL_BALANCE", 0)
TOKEN_COST_PHOTO = _int_env("TOKEN_COST_PHOTO", 1)
TOKEN_COST_VIDEO = _int_env("TOKEN_COST_VIDEO", 5)

STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "").strip()
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "").strip()
STRIPE_TOKEN_PRICE_CENTS = _int_env("STRIPE_TOKEN_PRICE_CENTS", 10)
STRIPE_TOKEN_PRICE_USD = _float_env(
    "STRIPE_TOKEN_PRICE_USD",
    max(1, STRIPE_TOKEN_PRICE_CENTS) / 100,
)
STRIPE_PACKAGE_PRICE_CENTS = _package_price_map_env("STRIPE_PACKAGE_PRICE_CENTS")
STRIPE_CHECKOUT_SUCCESS_URL = os.getenv("STRIPE_CHECKOUT_SUCCESS_URL", "").strip()
STRIPE_CHECKOUT_CANCEL_URL = os.getenv("STRIPE_CHECKOUT_CANCEL_URL", "").strip()
STRIPE_WEBHOOK_TOLERANCE_SECONDS = _int_env("STRIPE_WEBHOOK_TOLERANCE_SECONDS", 300)
