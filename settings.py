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

POLL_INTERVAL_SECONDS = 3
POLL_MAX_ATTEMPTS = 100
NETWORK_RETRY_ATTEMPTS = 3
NETWORK_RETRY_DELAY_SECONDS = 1

JWT_SECRET = os.getenv("JWT_SECRET", "dev-change-me")
JWT_EXPIRES_HOURS = int(os.getenv("JWT_EXPIRES_HOURS", "24"))
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "").strip()
