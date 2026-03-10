from __future__ import annotations

import json
import math
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal
from urllib.parse import parse_qs, quote, urlencode, urlparse

import httpx
from fastapi import FastAPI, File, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .auth import (
    AuthError,
    get_user_by_access_token,
    hash_password,
    issue_token_for_user,
    sanitize_user,
    verify_google_id_token,
    verify_password,
)
from .db import db, nanoid
from .nano_banano import (
    create_character,
    create_character_with_reference,
    generate_video,
    get_video_status,
)
from .settings import (
    FRONTEND_APP_URL,
    FRONTEND_ORIGINS,
    PUBLIC_DIR,
    STRIPE_CHECKOUT_CANCEL_URL,
    STRIPE_CHECKOUT_SUCCESS_URL,
    STRIPE_PACKAGE_PRICE_CENTS,
    STRIPE_TOKEN_PRICE_USD,
    TOKEN_COST_PHOTO,
    TOKEN_COST_VIDEO,
    UPLOAD_API_BASE_URL,
)
from .stripe_billing import (
    StripeBillingError,
    create_checkout_session,
    upsert_payment_entities_metadata,
    verify_and_parse_webhook,
)
from .upload_post import (
    UploadPostError,
    build_upload_post_username,
    delete_user_profile,
    ensure_user_profile,
    generate_connect_url,
    publish_photo_urls,
    get_publish_status,
    get_user_profile_or_none,
    publish_video_url,
    verify_api_key,
)

app = FastAPI(title="AI Influencer Backend", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

uploads_dir = PUBLIC_DIR / "uploads"
uploads_dir.mkdir(parents=True, exist_ok=True)
app.mount("/uploads", StaticFiles(directory=str(uploads_dir)), name="uploads")


def api_error(message: str, status: int = 400, **extra: Any) -> JSONResponse:
    payload: dict[str, Any] = {"error": message}
    payload.update(extra)
    return JSONResponse(payload, status_code=status)


def as_record(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def request_origin(request: Request) -> str:
    return str(request.base_url).rstrip("/")


def with_public_upload_urls(value: Any, origin: str) -> Any:
    if isinstance(value, dict):
        return {key: with_public_upload_urls(item, origin) for key, item in value.items()}
    if isinstance(value, list):
        return [with_public_upload_urls(item, origin) for item in value]
    if isinstance(value, str) and value.startswith("/uploads/"):
        return f"{origin}{value}"
    return value


def extract_uploaded_url(payload: Any) -> str | None:
    data = as_record(payload)
    candidates: list[Any] = [
        data.get("url"),
        data.get("fileUrl"),
        as_record(data.get("file")).get("url"),
        as_record(data.get("data")).get("url"),
        as_record(as_record(data.get("data")).get("file")).get("url"),
    ]
    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()

    # Fallback: return first http(s) URL from payload snapshot.
    snapshot = json.dumps(payload, ensure_ascii=False)
    match = re.search(r"https?://[^\s\"']+", snapshot)
    return match.group(0) if match else None


def is_valid_reference_link(value: str) -> bool:
    if not value.strip():
        return False
    if value.startswith("/"):
        return True
    return value.startswith("http://") or value.startswith("https://")


def normalize_email(value: str) -> str:
    return value.strip().lower()


def is_valid_email(value: str) -> bool:
    return bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", value))


def validate_password_rules(value: str) -> str | None:
    if len(value) < 8:
        return "Пароль должен содержать минимум 8 символов"
    if len(value) > 128:
        return "Пароль должен содержать максимум 128 символов"
    if re.search(r"\s", value):
        return "Пароль не должен содержать пробелы"
    if not re.search(r"[a-z]", value):
        return "Пароль должен содержать хотя бы одну строчную букву"
    if not re.search(r"[A-Z]", value):
        return "Пароль должен содержать хотя бы одну заглавную букву"
    if not re.search(r"\d", value):
        return "Пароль должен содержать хотя бы одну цифру"
    if not re.search(r"[^A-Za-z0-9]", value):
        return "Пароль должен содержать хотя бы один спецсимвол"
    return None


def extract_bearer_token(authorization_header: str | None) -> str | None:
    if not authorization_header:
        return None
    kind, _, token = authorization_header.partition(" ")
    if kind.lower() != "bearer":
        return None
    clean_token = token.strip()
    return clean_token or None


def unauthorized_response(message: str = "Unauthorized") -> JSONResponse:
    return api_error(message, status=401)


def current_user(request: Request) -> dict[str, Any]:
    return as_record(getattr(request.state, "user", {}))


def current_user_id(request: Request) -> str:
    user = current_user(request)
    return str(user.get("id") or "").strip()


def upload_post_username_for_user(user: dict[str, Any]) -> str:
    return build_upload_post_username(
        user_id=str(user.get("id") or ""),
        email=str(user.get("email") or ""),
    )


def as_positive_int(value: Any, fallback: int = 0) -> int:
    try:
        parsed = int(value)
        return parsed if parsed > 0 else fallback
    except Exception:
        return fallback


def normalize_scheduled_date(value: str | None) -> str | None:
    trimmed = (value or "").strip()
    if not trimmed:
        return None

    normalized = trimmed.replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            parsed = datetime.strptime(normalized, fmt)
            return parsed.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return None


def to_absolute_media_url(value: str, origin: str) -> str:
    trimmed = value.strip()
    if not trimmed:
        return ""
    if trimmed.startswith("http://") or trimmed.startswith("https://"):
        return trimmed
    if trimmed.startswith("/"):
        return f"{origin.rstrip('/')}{trimmed}"
    return trimmed


def resolve_blogger_image_for_publish(
    blogger: dict[str, Any],
    *,
    category: Literal["view", "clothes", "home", "cars", "relatives"],
    image_id: str,
) -> tuple[str, str] | None:
    normalized_id = image_id.strip()
    if not normalized_id:
        return None

    if category == "view":
        if normalized_id == "base":
            image_ref = str(blogger.get("baseImage") or "").strip()
            return (image_ref, "Базовый образ") if image_ref else None

        for item in blogger.get("looks") or []:
            record = as_record(item)
            if str(record.get("id") or "").strip() != normalized_id:
                continue
            image_ref = str(record.get("imageRef") or "").strip()
            image_name = str(record.get("name") or "Образ").strip() or "Образ"
            return (image_ref, image_name) if image_ref else None
        return None

    collection = blogger.get(category)
    if not isinstance(collection, list):
        return None
    for item in collection:
        record = as_record(item)
        if str(record.get("id") or "").strip() != normalized_id:
            continue
        image_ref = str(record.get("imageRef") or "").strip()
        image_name = str(record.get("name") or "Изображение").strip() or "Изображение"
        return (image_ref, image_name) if image_ref else None
    return None


def user_token_balance(user: dict[str, Any] | None) -> int:
    if not isinstance(user, dict):
        return 0
    return as_positive_int(user.get("tokenBalance"), fallback=0)


def token_settings_payload() -> dict[str, Any]:
    normalized_usd_price = max(0.001, float(STRIPE_TOKEN_PRICE_USD))
    return {
        "photoGenerationCost": max(1, TOKEN_COST_PHOTO),
        "videoGenerationCost": max(1, TOKEN_COST_VIDEO),
        "stripeTokenPriceCents": normalized_usd_price * 100,
        "stripeTokenPriceUsd": normalized_usd_price,
    }


def normalize_http_origin(value: str | None) -> str:
    raw = (value or "").strip().rstrip("/")
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
    except Exception:
        return ""
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    return f"{parsed.scheme}://{parsed.netloc}"


def checkout_frontend_root(request: Request) -> str:
    allowed_origins = {
        normalize_http_origin(item)
        for item in [*FRONTEND_ORIGINS, FRONTEND_APP_URL]
        if normalize_http_origin(item)
    }
    request_origins = [
        normalize_http_origin(request.headers.get("origin")),
        normalize_http_origin(request.headers.get("referer")),
    ]
    for origin in request_origins:
        if origin and (len(allowed_origins) == 0 or origin in allowed_origins):
            return origin
    return normalize_http_origin(FRONTEND_APP_URL) or request_origin(request)


def is_allowed_checkout_redirect(url: str, request: Request) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False
    redirect_origin = f"{parsed.scheme}://{parsed.netloc}"
    allowed_origins = {
        normalize_http_origin(item)
        for item in [*FRONTEND_ORIGINS, FRONTEND_APP_URL, request_origin(request)]
        if normalize_http_origin(item)
    }
    return redirect_origin in allowed_origins


def default_checkout_success_url(request: Request) -> str:
    root = checkout_frontend_root(request)
    return (
        f"{root.rstrip('/')}/billing?checkout=success&session_id={{CHECKOUT_SESSION_ID}}"
    )


def default_checkout_cancel_url(request: Request) -> str:
    root = checkout_frontend_root(request)
    return f"{root.rstrip('/')}/billing?checkout=cancel"


def checkout_referer_path(request: Request) -> str:
    referer = (request.headers.get("referer") or "").strip()
    if not referer:
        return ""
    try:
        parsed = urlparse(referer)
    except Exception:
        return ""
    return parsed.path.rstrip("/")


def is_package_checkout_flow(request: Request) -> bool:
    return checkout_referer_path(request) in {"/onboarding/step-4", "/payment"}


def inferred_onboarding_checkout_success_url(request: Request) -> str:
    referer = (request.headers.get("referer") or "").strip()
    if not referer:
        return ""
    try:
        parsed = urlparse(referer)
    except Exception:
        return ""

    if parsed.path.rstrip("/") != "/payment":
        return ""

    query = parse_qs(parsed.query, keep_blank_values=False)
    blogger_id = (query.get("bloggerId") or [""])[0].strip()
    if not blogger_id:
        return ""

    root = checkout_frontend_root(request)
    return f"{root.rstrip('/')}/blogger/{quote(blogger_id, safe='')}?tab=looks"


def inferred_payment_checkout_cancel_url(request: Request) -> str:
    referer = (request.headers.get("referer") or "").strip()
    if not referer:
        return ""
    try:
        parsed = urlparse(referer)
    except Exception:
        return ""

    if parsed.path.rstrip("/") != "/payment":
        return ""

    query = parse_qs(parsed.query, keep_blank_values=False)
    query.pop("session_id", None)
    query["checkout"] = ["cancel"]
    encoded_query = urlencode(query, doseq=True)
    root = checkout_frontend_root(request)
    return (
        f"{root.rstrip('/')}/payment?{encoded_query}"
        if encoded_query
        else f"{root.rstrip('/')}/payment?checkout=cancel"
    )


@dataclass
class ReservedTokenSpend:
    user_id: str
    amount: int
    reason: str
    metadata: dict[str, Any]


def reserve_tokens_for_generation(
    *,
    user_id: str,
    amount: int,
    reason: str,
    metadata: dict[str, Any],
) -> tuple[ReservedTokenSpend | None, JSONResponse | None]:
    result = db.spend_user_tokens(
        user_id=user_id,
        amount=amount,
        reason=reason,
        metadata=metadata,
    )
    if not result.get("success"):
        error = str(result.get("error") or "")
        if error == "insufficient_tokens":
            available = int(result.get("balance") or 0)
            return None, api_error(
                f"Недостаточно токенов. Нужно {amount}, доступно {available}",
                status=402,
                code="INSUFFICIENT_TOKENS",
                required=amount,
                available=available,
            )
        if error == "user_not_found":
            return None, unauthorized_response("User not found")
        return None, api_error("Не удалось списать токены", status=500)

    return (
        ReservedTokenSpend(
            user_id=user_id,
            amount=amount,
            reason=reason,
            metadata=metadata,
        ),
        None,
    )


def refund_reserved_tokens(reserved: ReservedTokenSpend, *, reason: str) -> None:
    db.credit_user_tokens(
        user_id=reserved.user_id,
        amount=reserved.amount,
        reason=reason,
        metadata={
            **reserved.metadata,
            "refundForReason": reserved.reason,
        },
    )


def is_owned_by_user(record: dict[str, Any] | None, user_id: str) -> bool:
    if not isinstance(record, dict):
        return False
    return str(record.get("ownerUserId") or "").strip() == user_id


class CreateBloggerRequest(BaseModel):
    name: str = Field(min_length=1)
    prompt: str = Field(min_length=1)
    baseImage: str | None = None


class CreateInNanoRequest(BaseModel):
    referenceImage: str | None = None


class CreateLookRequest(BaseModel):
    mode: Literal["manual", "clone", "constructor"] | None = None
    name: str = Field(min_length=1)
    prompt: str = Field(min_length=1)
    referenceImage: str | None = None
    referenceImages: list[str] | None = None
    includePrimaryReferenceImage: bool = True


class CreateAssetRequest(BaseModel):
    category: Literal["clothes", "home", "cars", "relatives"]
    action: Literal["upload", "generate"]
    name: str = Field(min_length=1)
    imageUrl: str | None = None
    prompt: str | None = None


class CreateVideoRequest(BaseModel):
    bloggerId: str = Field(min_length=1)
    type: Literal["motion_control", "ugc", "custom"]
    prompt: str | None = None
    lookId: str | None = None
    referenceImage: str | None = None
    imageUrls: list[str] | None = None
    videoUrls: list[str] | None = None
    motionDurationSeconds: float | None = None
    motionOrientation: Literal["video", "image"] | None = None
    motionMode: Literal["720p", "1080p"] | None = None
    aspectRatio: Literal["16:9", "9:16", "Auto"] | None = None


class RegisterRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=8, max_length=128)
    name: str | None = None


class LoginRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=1, max_length=200)


class GoogleAuthRequest(BaseModel):
    idToken: str = Field(min_length=1)


ConnectSocialPlatform = Literal[
    "tiktok",
    "instagram",
    "linkedin",
    "youtube",
    "facebook",
    "x",
    "threads",
]

PublishSocialPlatform = Literal[
    "tiktok",
    "instagram",
    "linkedin",
    "youtube",
    "facebook",
    "x",
    "threads",
    "pinterest",
    "bluesky",
    "reddit",
]


class SocialConnectUrlRequest(BaseModel):
    redirectUrl: str | None = None
    redirectButtonText: str | None = None
    connectTitle: str | None = None
    connectDescription: str | None = None
    platforms: list[ConnectSocialPlatform] | None = None
    showCalendar: bool | None = None


class SocialPublishVideoRequest(BaseModel):
    videoId: str = Field(min_length=1)
    platforms: list[PublishSocialPlatform]
    title: str | None = None
    description: str | None = None
    scheduledDate: str | None = None
    timezone: str | None = None
    asyncUpload: bool = True


class SocialPublishImageRequest(BaseModel):
    bloggerId: str = Field(min_length=1)
    category: Literal["view", "clothes", "home", "cars", "relatives"]
    imageId: str = Field(min_length=1)
    platforms: list[PublishSocialPlatform]
    title: str | None = None
    description: str | None = None
    scheduledDate: str | None = None
    timezone: str | None = None
    asyncUpload: bool = True


class CreateCheckoutSessionRequest(BaseModel):
    tokenAmount: int = Field(ge=1, le=100000)
    amountCents: int | None = Field(default=None, ge=1, le=100000000)
    successUrl: str | None = None
    cancelUrl: str | None = None


PUBLIC_AUTH_PATHS = {
    "/api/auth/register",
    "/api/auth/login",
    "/api/auth/google",
    "/api/webhooks/stripe",
}


@app.middleware("http")
async def auth_middleware(request: Request, call_next: Any) -> Any:
    path = request.url.path
    normalized_path = path.rstrip("/") or "/"

    if request.method == "OPTIONS":
        return await call_next(request)

    if path == "/health" or path.startswith("/uploads/"):
        return await call_next(request)

    if not path.startswith("/api/"):
        return await call_next(request)

    if normalized_path in PUBLIC_AUTH_PATHS:
        return await call_next(request)

    token = extract_bearer_token(request.headers.get("Authorization"))
    if not token:
        return unauthorized_response("Missing bearer token")

    try:
        user = get_user_by_access_token(token)
    except AuthError as exc:
        return unauthorized_response(str(exc))

    request.state.user = sanitize_user(user)
    return await call_next(request)


@app.post("/api/auth/register")
async def auth_register(payload: RegisterRequest) -> Any:
    email = normalize_email(payload.email)
    if not is_valid_email(email):
        return api_error("Invalid email format", status=400)

    password_error = validate_password_rules(payload.password)
    if password_error:
        return api_error(password_error, status=400)

    existing = db.get_user_by_email(email)
    if existing:
        return api_error("User with this email already exists", status=409)

    user = db.create_user(
        {
            "email": email,
            "name": (payload.name or "").strip() or email.split("@")[0],
            "passwordHash": hash_password(payload.password),
            "provider": "local",
        }
    )
    user_id = str(user.get("id") or "").strip()
    if user_id:
        refreshed = db.get_user_by_id(user_id)
        if refreshed:
            user = refreshed
    token = issue_token_for_user(user)
    return {
        "token": token,
        "user": sanitize_user(user),
    }


@app.post("/api/auth/login")
async def auth_login(payload: LoginRequest) -> Any:
    email = normalize_email(payload.email)
    user = db.get_user_by_email(email)
    if not user:
        return api_error("Invalid email or password", status=401)

    password_hash = str(user.get("passwordHash") or "")
    if not password_hash:
        return api_error("Use Google login for this account", status=400)

    if not verify_password(payload.password, password_hash):
        return api_error("Invalid email or password", status=401)

    user_id = str(user.get("id") or "").strip()
    if user_id:
        db.ensure_user_token_balance(user_id)
        refreshed = db.get_user_by_id(user_id)
        if refreshed:
            user = refreshed

    token = issue_token_for_user(user)
    return {
        "token": token,
        "user": sanitize_user(user),
    }


@app.post("/api/auth/google")
async def auth_google(payload: GoogleAuthRequest) -> Any:
    try:
        google_user = await verify_google_id_token(payload.idToken)
    except AuthError as exc:
        return api_error(str(exc), status=401)

    user = db.get_user_by_google_sub(google_user["sub"])
    if not user:
        user = db.get_user_by_email(google_user["email"])

    if user:
        update_payload: dict[str, Any] = {
            "name": google_user["name"] or user.get("name"),
            "googleSub": google_user["sub"],
            "email": google_user["email"],
            "provider": "google",
        }
        user = db.update_user(str(user.get("id")), update_payload) or user
    else:
        user = db.create_user(
            {
                "email": google_user["email"],
                "name": google_user["name"] or google_user["email"].split("@")[0],
                "googleSub": google_user["sub"],
                "provider": "google",
            }
        )

    user_id = str(user.get("id") or "").strip()
    if user_id:
        db.ensure_user_token_balance(user_id)
        refreshed = db.get_user_by_id(user_id)
        if refreshed:
            user = refreshed

    token = issue_token_for_user(user)
    return {
        "token": token,
        "user": sanitize_user(user),
    }


@app.get("/api/auth/me")
async def auth_me(request: Request) -> Any:
    token = extract_bearer_token(request.headers.get("Authorization"))
    if not token:
        return unauthorized_response("Missing bearer token")
    try:
        user = get_user_by_access_token(token)
    except AuthError as exc:
        return unauthorized_response(str(exc))
    user_id = str(user.get("id") or "").strip()
    if user_id:
        db.ensure_user_token_balance(user_id)
        refreshed = db.get_user_by_id(user_id)
        if refreshed:
            user = refreshed
    return {"user": sanitize_user(user)}


@app.get("/api/account/me")
async def account_me(request: Request) -> Any:
    user = current_user(request)
    user_id = str(user.get("id") or "").strip()
    if not user_id:
        return unauthorized_response("User not found")

    db.ensure_user_token_balance(user_id)
    fresh_user = db.get_user_by_id(user_id)
    if not fresh_user:
        return unauthorized_response("User not found")

    return {
        "user": sanitize_user(fresh_user),
        "tokenBalance": user_token_balance(fresh_user),
        "tokenSettings": token_settings_payload(),
        "stripeEnabled": True,
    }


@app.get("/api/account/token-transactions")
async def account_token_transactions(request: Request, limit: int = Query(default=50)) -> Any:
    user = current_user(request)
    user_id = str(user.get("id") or "").strip()
    if not user_id:
        return unauthorized_response("User not found")

    items = db.list_token_transactions(user_id=user_id, limit=limit)
    return {"items": items}


@app.post("/api/billing/create-checkout-session")
async def billing_create_checkout_session(
    request: Request,
    payload: CreateCheckoutSessionRequest,
) -> Any:
    user = current_user(request)
    user_id = str(user.get("id") or "").strip()
    if not user_id:
        return unauthorized_response("User not found")

    requested_success_url = (payload.successUrl or "").strip()
    requested_cancel_url = (payload.cancelUrl or "").strip()
    requested_amount_cents = int(payload.amountCents) if payload.amountCents is not None else None
    if requested_success_url and not is_allowed_checkout_redirect(requested_success_url, request):
        return api_error("Invalid successUrl origin", status=400)
    if requested_cancel_url and not is_allowed_checkout_redirect(requested_cancel_url, request):
        return api_error("Invalid cancelUrl origin", status=400)

    inferred_success_url = inferred_onboarding_checkout_success_url(request)
    inferred_cancel_url = inferred_payment_checkout_cancel_url(request)

    success_url = (
        requested_success_url
        or inferred_success_url
        or STRIPE_CHECKOUT_SUCCESS_URL.strip()
        or default_checkout_success_url(request)
    )
    cancel_url = (
        requested_cancel_url
        or inferred_cancel_url
        or STRIPE_CHECKOUT_CANCEL_URL.strip()
        or default_checkout_cancel_url(request)
    )

    package_price_cents = STRIPE_PACKAGE_PRICE_CENTS.get(int(payload.tokenAmount))
    amount_cents_override: int | None = None
    if requested_amount_cents is not None:
        if package_price_cents is None or requested_amount_cents != package_price_cents:
            return api_error("Invalid package pricing", status=400)
        amount_cents_override = requested_amount_cents
    elif package_price_cents is not None and is_package_checkout_flow(request):
        amount_cents_override = package_price_cents
    else:
        calculated_amount_cents = int(round(float(payload.tokenAmount) * float(STRIPE_TOKEN_PRICE_USD) * 100))
        amount_cents_override = max(1, calculated_amount_cents)

    try:
        session = await create_checkout_session(
            user_id=user_id,
            user_email=str(user.get("email") or ""),
            token_amount=payload.tokenAmount,
            success_url=success_url,
            cancel_url=cancel_url,
            amount_cents_override=amount_cents_override,
        )
        db.create_or_update_checkout_session(
            session_id=str(session.get("id")),
            user_id=user_id,
            token_amount=int(session.get("tokenAmount") or payload.tokenAmount),
            amount_cents=int(session.get("amountCents") or 0),
            currency=str(session.get("currency") or "usd"),
            status="pending",
            metadata={
                "source": "checkout_session_create",
            },
        )
        return {
            "sessionId": session.get("id"),
            "url": session.get("url"),
            "tokenAmount": session.get("tokenAmount"),
            "amountCents": session.get("amountCents"),
            "currency": session.get("currency"),
        }
    except StripeBillingError as exc:
        return api_error(
            str(exc),
            status=exc.status_code,
            upstream=exc.upstream,
        )
    except Exception as exc:
        return api_error(f"Failed to create checkout session: {exc}", status=500)


@app.post("/api/webhooks/stripe")
async def stripe_webhook(request: Request) -> Any:
    payload = await request.body()
    signature = request.headers.get("Stripe-Signature")
    try:
        event = verify_and_parse_webhook(payload, signature)
    except StripeBillingError as exc:
        return api_error(str(exc), status=exc.status_code, upstream=exc.upstream)

    event_type = str(event.get("type") or "")
    event_id = str(event.get("id") or "")
    data = as_record(event.get("data"))
    event_object = as_record(data.get("object"))

    if event_type not in {
        "checkout.session.completed",
        "checkout.session.async_payment_succeeded",
    }:
        return {"received": True, "ignored": True, "eventType": event_type}

    payment_status = str(event_object.get("payment_status") or "").lower()
    if payment_status != "paid":
        return {
            "received": True,
            "ignored": True,
            "eventType": event_type,
            "reason": "payment_not_paid",
        }

    session_id = str(event_object.get("id") or "").strip()
    metadata = as_record(event_object.get("metadata"))
    user_id = str(
        metadata.get("user_id")
        or event_object.get("client_reference_id")
        or ""
    ).strip()
    token_amount = as_positive_int(metadata.get("token_amount"), fallback=0)
    amount_cents = as_positive_int(event_object.get("amount_total"), fallback=0)
    currency = str(event_object.get("currency") or "usd").lower()
    payment_intent = str(event_object.get("payment_intent") or "").strip()

    if not session_id or not user_id or token_amount <= 0:
        return api_error(
            "Stripe webhook payload is missing required checkout metadata",
            status=400,
            eventType=event_type,
            sessionId=session_id,
        )

    applied = db.apply_paid_checkout(
        session_id=session_id,
        user_id=user_id,
        token_amount=token_amount,
        amount_cents=amount_cents,
        currency=currency,
        payment_intent=payment_intent or None,
        event_id=event_id or None,
        metadata={
            "eventType": event_type,
        },
    )

    if payment_intent:
        try:
            await upsert_payment_entities_metadata(payment_intent)
        except Exception:
            # Metadata sync failure should not block successful token crediting.
            pass

    return {"received": True, **applied}


@app.get("/api/social/accounts")
async def social_accounts(request: Request) -> Any:
    try:
        user = current_user(request)
        username = upload_post_username_for_user(user)
        upload_post_account = await verify_api_key()
        profile = await get_user_profile_or_none(username) or {}
        return {
            "username": username,
            "socialAccounts": as_record(profile.get("social_accounts")),
            "profile": profile,
            "profileExists": bool(profile),
            "uploadPostAccount": upload_post_account,
        }
    except UploadPostError as exc:
        return api_error(
            str(exc),
            status=exc.status_code,
            upstream=exc.upstream,
        )
    except Exception as exc:
        return api_error(f"Failed to load social accounts: {exc}", status=500)


@app.delete("/api/social/accounts")
async def social_disconnect(request: Request) -> Any:
    try:
        user = current_user(request)
        username = upload_post_username_for_user(user)
        result = await delete_user_profile(username)
        return {
            "success": True,
            "username": username,
            "removed": True,
            "result": result,
        }
    except UploadPostError as exc:
        if exc.status_code == 404:
            user = current_user(request)
            username = upload_post_username_for_user(user)
            return {
                "success": True,
                "username": username,
                "removed": False,
                "message": "Integration is already removed",
            }
        return api_error(
            str(exc),
            status=exc.status_code,
            upstream=exc.upstream,
        )
    except Exception as exc:
        return api_error(f"Failed to remove social integration: {exc}", status=500)


@app.post("/api/social/connect-url")
async def social_connect_url(
    request: Request,
    payload: SocialConnectUrlRequest | None = None,
) -> Any:
    try:
        user = current_user(request)
        username = upload_post_username_for_user(user)
        connect_payload = payload or SocialConnectUrlRequest()
        generated = await generate_connect_url(
            username,
            redirect_url=(
                connect_payload.redirectUrl.strip()
                if connect_payload.redirectUrl and connect_payload.redirectUrl.strip()
                else request_origin(request)
            ),
            redirect_button_text=connect_payload.redirectButtonText,
            connect_title=connect_payload.connectTitle,
            connect_description=connect_payload.connectDescription,
            platforms=connect_payload.platforms,
            show_calendar=connect_payload.showCalendar,
        )
        access_url = str(generated.get("access_url") or "").strip()
        if not access_url:
            return api_error("Upload-Post access URL is empty", status=502, upstream=generated)
        return {
            "username": username,
            "accessUrl": access_url,
            "duration": generated.get("duration"),
        }
    except UploadPostError as exc:
        return api_error(
            str(exc),
            status=exc.status_code,
            upstream=exc.upstream,
        )
    except Exception as exc:
        return api_error(f"Failed to generate connect URL: {exc}", status=500)


@app.post("/api/social/publish-video")
async def social_publish_video(payload: SocialPublishVideoRequest, request: Request) -> Any:
    if len(payload.platforms) == 0:
        return api_error("Нужно выбрать хотя бы одну платформу", status=400)

    user = current_user(request)
    user_id = str(user.get("id") or "").strip()
    username = upload_post_username_for_user(user)

    video = db.get_video_by_id(payload.videoId)
    if not video or not is_owned_by_user(video, user_id):
        return api_error("Video not found", status=404)

    output_url = str(video.get("outputUrl") or "").strip()
    if not output_url:
        return api_error("Видео еще не готово для публикации", status=400)

    normalized_scheduled_date = normalize_scheduled_date(payload.scheduledDate)
    if (payload.scheduledDate or "").strip() and not normalized_scheduled_date:
        return api_error(
            "Некорректная дата публикации. Используй формат YYYY-MM-DDTHH:MM",
            status=400,
        )
    normalized_timezone = (payload.timezone or "").strip() or None

    try:
        trimmed_title = (payload.title or "").strip()
        default_title = (str(video.get("prompt") or "").strip() or "Generated video")[:220]
        publish_result = await publish_video_url(
            username=username,
            video_url=output_url,
            platforms=payload.platforms,
            title=trimmed_title or default_title,
            description=(payload.description or "").strip() or None,
            scheduled_date=normalized_scheduled_date,
            timezone=normalized_timezone,
            async_upload=payload.asyncUpload,
        )

        request_id = str(publish_result.get("request_id") or "").strip()
        job_id = str(publish_result.get("job_id") or "").strip()
        if normalized_scheduled_date:
            publish_status = "scheduled"
        else:
            publish_status = "completed"
            if request_id:
                publish_status = "processing"
            elif job_id:
                publish_status = "scheduled"

        db.update_video(
            payload.videoId,
            {
                "socialPublish": {
                    "provider": "upload-post",
                    "username": username,
                    "platforms": payload.platforms,
                    "requestId": request_id or None,
                    "jobId": job_id or None,
                    "status": publish_status,
                    "scheduledDate": normalized_scheduled_date,
                    "timezone": normalized_timezone,
                    "response": publish_result,
                    "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }
            },
        )

        return {
            "success": True,
            "videoId": payload.videoId,
            "requestId": request_id or None,
            "jobId": job_id or None,
            "status": publish_status,
            "scheduledDate": normalized_scheduled_date,
            "timezone": normalized_timezone,
            "uploadPost": publish_result,
        }
    except UploadPostError as exc:
        return api_error(
            str(exc),
            status=exc.status_code,
            upstream=exc.upstream,
        )
    except Exception as exc:
        return api_error(f"Failed to publish video: {exc}", status=500)


@app.post("/api/social/publish-image")
async def social_publish_image(payload: SocialPublishImageRequest, request: Request) -> Any:
    if len(payload.platforms) == 0:
        return api_error("Нужно выбрать хотя бы одну платформу", status=400)

    user = current_user(request)
    user_id = str(user.get("id") or "").strip()
    username = upload_post_username_for_user(user)

    blogger = db.get_blogger_by_id(payload.bloggerId)
    if not blogger or not is_owned_by_user(blogger, user_id):
        return api_error("Blogger not found", status=404)

    image_result = resolve_blogger_image_for_publish(
        blogger,
        category=payload.category,
        image_id=payload.imageId,
    )
    if not image_result:
        return api_error("Изображение не найдено", status=404)
    image_ref, image_name = image_result
    image_url = to_absolute_media_url(image_ref, request_origin(request))
    if not image_url:
        return api_error("Изображение недоступно для публикации", status=400)

    normalized_scheduled_date = normalize_scheduled_date(payload.scheduledDate)
    if (payload.scheduledDate or "").strip() and not normalized_scheduled_date:
        return api_error(
            "Некорректная дата публикации. Используй формат YYYY-MM-DDTHH:MM",
            status=400,
        )
    normalized_timezone = (payload.timezone or "").strip() or None

    try:
        publish_result = await publish_photo_urls(
            username=username,
            photo_urls=[image_url],
            platforms=payload.platforms,
            title=(payload.title or "").strip() or image_name,
            description=(payload.description or "").strip() or None,
            scheduled_date=normalized_scheduled_date,
            timezone=normalized_timezone,
            async_upload=payload.asyncUpload,
        )

        request_id = str(publish_result.get("request_id") or "").strip()
        job_id = str(publish_result.get("job_id") or "").strip()
        if normalized_scheduled_date:
            publish_status = "scheduled"
        else:
            publish_status = "completed"
            if request_id:
                publish_status = "processing"
            elif job_id:
                publish_status = "scheduled"

        return {
            "success": True,
            "bloggerId": payload.bloggerId,
            "imageId": payload.imageId,
            "requestId": request_id or None,
            "jobId": job_id or None,
            "status": publish_status,
            "scheduledDate": normalized_scheduled_date,
            "timezone": normalized_timezone,
            "uploadPost": publish_result,
        }
    except UploadPostError as exc:
        return api_error(
            str(exc),
            status=exc.status_code,
            upstream=exc.upstream,
        )
    except Exception as exc:
        return api_error(f"Failed to publish image: {exc}", status=500)


@app.get("/api/social/publish-status")
async def social_publish_status(
    requestId: str | None = Query(default=None),
    jobId: str | None = Query(default=None),
) -> Any:
    try:
        payload = await get_publish_status(request_id=requestId, job_id=jobId)
        return payload
    except UploadPostError as exc:
        return api_error(
            str(exc),
            status=exc.status_code,
            upstream=exc.upstream,
        )
    except Exception as exc:
        return api_error(f"Failed to fetch publish status: {exc}", status=500)


@app.get("/api/bloggers")
async def get_bloggers(request: Request) -> Any:
    try:
        user_id = current_user_id(request)
        owned_bloggers = [
            item
            for item in db.get_all_bloggers()
            if is_owned_by_user(item, user_id)
        ]
        return with_public_upload_urls(owned_bloggers, request_origin(request))
    except Exception as exc:
        return api_error(f"Failed to fetch bloggers: {exc}", status=500)


@app.post("/api/bloggers")
async def create_blogger(payload: CreateBloggerRequest, request: Request) -> Any:
    try:
        user_id = current_user_id(request)
        blogger = db.create_blogger(
            {
                "name": payload.name,
                "prompt": payload.prompt,
                "baseImage": payload.baseImage,
                "ownerUserId": user_id,
                "looks": [],
                "clothes": [],
                "home": [],
                "cars": [],
                "relatives": [],
            }
        )
        return with_public_upload_urls(blogger, request_origin(request))
    except Exception as exc:
        return api_error(f"Failed to create blogger: {exc}", status=500)


@app.get("/api/bloggers/{blogger_id}")
async def get_blogger(blogger_id: str, request: Request) -> Any:
    user_id = current_user_id(request)
    blogger = db.get_blogger_by_id(blogger_id)
    if not blogger or not is_owned_by_user(blogger, user_id):
        return api_error("Blogger not found", status=404)
    return with_public_upload_urls(blogger, request_origin(request))


@app.patch("/api/bloggers/{blogger_id}")
async def patch_blogger(blogger_id: str, body: dict[str, Any], request: Request) -> Any:
    user_id = current_user_id(request)
    blogger = db.get_blogger_by_id(blogger_id)
    if not blogger or not is_owned_by_user(blogger, user_id):
        return api_error("Blogger not found", status=404)

    safe_patch = {key: value for key, value in body.items() if key != "ownerUserId"}
    updated = db.update_blogger(blogger_id, safe_patch)
    return with_public_upload_urls(updated, request_origin(request))


@app.delete("/api/bloggers/{blogger_id}")
async def delete_blogger(blogger_id: str, request: Request) -> Any:
    user_id = current_user_id(request)
    blogger = db.get_blogger_by_id(blogger_id)
    if not blogger or not is_owned_by_user(blogger, user_id):
        return api_error("Blogger not found", status=404)
    db.delete_videos_by_blogger_id(blogger_id)
    deleted = db.delete_blogger(blogger_id)
    if not deleted:
        return api_error("Blogger not found", status=404)
    return {"success": True}


@app.post("/api/bloggers/{blogger_id}/create-in-nano")
async def create_blogger_in_nano(
    blogger_id: str,
    request: Request,
    payload: CreateInNanoRequest | None = None,
) -> Any:
    user_id = current_user_id(request)
    blogger = db.get_blogger_by_id(blogger_id)
    if not blogger or not is_owned_by_user(blogger, user_id):
        return api_error("Blogger not found", status=404)

    reserved, reserve_error = reserve_tokens_for_generation(
        user_id=user_id,
        amount=max(1, TOKEN_COST_PHOTO),
        reason="photo_generation",
        metadata={
            "operation": "create_in_nano",
            "bloggerId": blogger_id,
        },
    )
    if reserve_error:
        return reserve_error

    try:
        reference_image = payload.referenceImage if payload else None

        result = (
            await create_character_with_reference(
                {
                    "prompt": blogger.get("prompt", ""),
                    "referenceImages": [reference_image],
                    "aspectRatio": "3:4",
                    "resolution": "2K",
                    "outputFormat": "png",
                    "googleSearch": False,
                }
            )
            if reference_image
            else await create_character(str(blogger.get("prompt", "")))
        )

        if not result.get("success"):
            if reserved:
                refund_reserved_tokens(reserved, reason="refund_generation_failed")
            return api_error(
                result.get("error") or "Failed to create character in Nano Banano",
                status=500,
            )

        base_image = result.get("imageUrl") or blogger.get("baseImage")
        db.update_blogger(
            blogger_id,
            {
                "nanoBananoId": result.get("id"),
                "baseImage": base_image,
            },
        )

        return with_public_upload_urls(
            {
            "success": True,
            "nanoBananoId": result.get("id"),
            "baseImage": base_image,
            "tokenBalance": db.ensure_user_token_balance(user_id),
            },
            request_origin(request),
        )
    except Exception as exc:
        if reserved:
            refund_reserved_tokens(reserved, reason="refund_generation_failed")
        return api_error(f"Failed to create in Nano Banano: {exc}", status=500)


@app.post("/api/bloggers/{blogger_id}/looks")
async def create_look(blogger_id: str, payload: CreateLookRequest, request: Request) -> Any:
    user_id = current_user_id(request)
    blogger = db.get_blogger_by_id(blogger_id)
    if not blogger or not is_owned_by_user(blogger, user_id):
        return api_error("Blogger not found", status=404)

    try:
        mode = payload.mode or "manual"

        uploaded_reference_images = (
            payload.referenceImages
            if isinstance(payload.referenceImages, list)
            else ([payload.referenceImage] if payload.referenceImage else [])
        )

        for item in uploaded_reference_images:
            if not is_valid_reference_link(item):
                return api_error("Некорректная ссылка референса", status=400)

        primary_reference_image = blogger.get("baseImage") or (
            blogger.get("looks", [{}])[0].get("imageRef") if blogger.get("looks") else None
        )

        if mode == "clone" and len(uploaded_reference_images) == 0:
            return api_error("Для клонирования нужен второй референс", status=400)

        if len(uploaded_reference_images) > 0:
            reference_images = list(dict.fromkeys(uploaded_reference_images))
            if (
                payload.includePrimaryReferenceImage
                and isinstance(primary_reference_image, str)
                and primary_reference_image.strip()
            ):
                reference_images = list(
                    dict.fromkeys([*reference_images, primary_reference_image.strip()])
                )
        else:
            if not primary_reference_image:
                return api_error(
                    "Нужен минимум один существующий образ блоггера для image_input",
                    status=400,
                )
            reference_images = [primary_reference_image]

        reserved, reserve_error = reserve_tokens_for_generation(
            user_id=user_id,
            amount=max(1, TOKEN_COST_PHOTO),
            reason="photo_generation",
            metadata={
                "operation": "create_look",
                "bloggerId": blogger_id,
            },
        )
        if reserve_error:
            return reserve_error

        generated = await create_character_with_reference(
            {
                "prompt": payload.prompt,
                "referenceImages": reference_images,
                "aspectRatio": "auto",
                "googleSearch": False,
                "resolution": "1K",
                "outputFormat": "jpg",
            }
        )

        if not generated.get("success") or not generated.get("imageUrl"):
            if reserved:
                refund_reserved_tokens(reserved, reason="refund_generation_failed")
            return api_error(
                generated.get("error") or "Failed to generate look image",
                status=500,
            )

        next_looks = [
            *(blogger.get("looks") or []),
            {
                "id": f"look_{nanoid(8)}",
                "name": payload.name.strip(),
                "imageRef": generated.get("imageUrl"),
            },
        ]

        updated = db.update_blogger(blogger_id, {"looks": next_looks})
        response_payload = with_public_upload_urls(updated, request_origin(request))
        if isinstance(response_payload, dict):
            response_payload["tokenBalance"] = db.ensure_user_token_balance(user_id)
        return response_payload
    except Exception as exc:
        try:
            if "reserved" in locals() and reserved:
                refund_reserved_tokens(reserved, reason="refund_generation_failed")
        except Exception:
            pass
        return api_error(f"Failed to create look: {exc}", status=500)


@app.post("/api/bloggers/{blogger_id}/assets")
async def create_asset(blogger_id: str, payload: CreateAssetRequest, request: Request) -> Any:
    user_id = current_user_id(request)
    blogger = db.get_blogger_by_id(blogger_id)
    if not blogger or not is_owned_by_user(blogger, user_id):
        return api_error("Blogger not found", status=404)

    if payload.action == "upload" and not payload.imageUrl:
        return api_error("Для загрузки нужен imageUrl", status=400)
    if payload.action == "generate" and not payload.prompt:
        return api_error("Для генерации нужен промпт", status=400)

    reserved: ReservedTokenSpend | None = None
    if payload.action == "generate":
        reserved, reserve_error = reserve_tokens_for_generation(
            user_id=user_id,
            amount=max(1, TOKEN_COST_PHOTO),
            reason="photo_generation",
            metadata={
                "operation": "create_asset_generate",
                "bloggerId": blogger_id,
                "category": payload.category,
            },
        )
        if reserve_error:
            return reserve_error

    try:
        image_ref = (
            str(payload.imageUrl)
            if payload.action == "upload"
            else (await create_character(str(payload.prompt or ""))).get("imageUrl")
        )
        if not image_ref:
            if reserved:
                refund_reserved_tokens(reserved, reason="refund_generation_failed")
            return api_error("Не удалось получить изображение", status=500)

        collection = blogger.get(payload.category)
        if not isinstance(collection, list):
            collection = []
        next_collection = [
            *collection,
            {
                "id": f"asset_{nanoid(8)}",
                "name": payload.name.strip(),
                "imageRef": image_ref,
            },
        ]

        updated = db.update_blogger(blogger_id, {payload.category: next_collection})
        response_payload = with_public_upload_urls(updated, request_origin(request))
        if isinstance(response_payload, dict):
            response_payload["tokenBalance"] = db.ensure_user_token_balance(user_id)
        return response_payload
    except Exception as exc:
        if reserved:
            refund_reserved_tokens(reserved, reason="refund_generation_failed")
        return api_error(f"Failed to create item: {exc}", status=500)


@app.get("/api/videos")
async def get_videos(request: Request, bloggerId: str | None = Query(default=None)) -> Any:
    try:
        user_id = current_user_id(request)
        all_videos = [
            item
            for item in db.get_all_videos()
            if is_owned_by_user(item, user_id)
        ]
        if bloggerId:
            return with_public_upload_urls(
                [item for item in all_videos if item.get("bloggerId") == bloggerId],
                request_origin(request),
            )
        return with_public_upload_urls(all_videos, request_origin(request))
    except Exception as exc:
        return api_error(f"Failed to fetch videos: {exc}", status=500)


@app.post("/api/videos")
async def create_video(payload: CreateVideoRequest, request: Request) -> Any:
    try:
        user_id = current_user_id(request)
        blogger = db.get_blogger_by_id(payload.bloggerId)
        if not blogger or not is_owned_by_user(blogger, user_id):
            return api_error("Blogger not found", status=404)

        resolved_reference_image = payload.referenceImage
        if not resolved_reference_image and payload.lookId:
            for look in blogger.get("looks") or []:
                if look.get("id") == payload.lookId:
                    resolved_reference_image = look.get("imageRef")
                    break
        if not resolved_reference_image:
            resolved_reference_image = blogger.get("baseImage")

        resolved_image_urls = payload.imageUrls or (
            [resolved_reference_image] if resolved_reference_image else None
        )

        trimmed_prompt = payload.prompt.strip() if isinstance(payload.prompt, str) else ""

        if payload.type == "motion_control":
            if not payload.videoUrls or len(payload.videoUrls) == 0:
                return api_error("Для Motion Control нужно передать входящее видео", status=400)
            if not resolved_image_urls or len(resolved_image_urls) == 0:
                return api_error("Для Motion Control нужно передать входящее изображение", status=400)
            motion_duration_seconds = float(payload.motionDurationSeconds or 0)
            if not math.isfinite(motion_duration_seconds) or motion_duration_seconds <= 0:
                return api_error(
                    "Для Motion Control нужно передать корректную длительность видео в секундах",
                    status=400,
                )
            video_token_cost = max(1, int(math.ceil(motion_duration_seconds)))
        else:
            motion_duration_seconds = None
            video_token_cost = max(1, TOKEN_COST_VIDEO)

        reserved, reserve_error = reserve_tokens_for_generation(
            user_id=user_id,
            amount=video_token_cost,
            reason="video_generation",
            metadata={
                "operation": "create_video",
                "bloggerId": payload.bloggerId,
                "videoType": payload.type,
                "motionDurationSeconds": motion_duration_seconds,
                "chargedTokens": video_token_cost,
            },
        )
        if reserve_error:
            return reserve_error

        nb_result = await generate_video(
            {
                "bloggerId": payload.bloggerId,
                "type": payload.type,
                "prompt": trimmed_prompt or None,
                "lookId": payload.lookId,
                "referenceImage": resolved_reference_image,
                "aspectRatio": "Auto" if payload.type == "ugc" else payload.aspectRatio,
                "imageUrls": resolved_image_urls,
                "videoUrls": payload.videoUrls,
                "motionOrientation": payload.motionOrientation,
                "motionMode": payload.motionMode,
            }
        )

        video = db.create_video(
            {
                "externalTaskId": nb_result.get("id"),
                "bloggerId": payload.bloggerId,
                "ownerUserId": user_id,
                "type": payload.type,
                "prompt": trimmed_prompt,
                "lookId": payload.lookId,
                "status": nb_result.get("status"),
                "outputUrl": nb_result.get("outputUrl"),
            }
        )
        if not str(nb_result.get("id") or "").strip() and str(nb_result.get("status") or "").lower() == "failed":
            if reserved:
                refund_reserved_tokens(reserved, reason="refund_generation_failed")
            return api_error(
                str(nb_result.get("error") or "Не удалось создать видео"),
                status=500,
            )

        response_payload = with_public_upload_urls(video, request_origin(request))
        if isinstance(response_payload, dict):
            response_payload["tokenBalance"] = db.ensure_user_token_balance(user_id)
        return response_payload
    except Exception as exc:
        # Any hard failure before job submission should return tokens.
        try:
            if "reserved" in locals() and reserved:
                refund_reserved_tokens(reserved, reason="refund_generation_failed")
        except Exception:
            pass
        return api_error(str(exc), status=500)


@app.get("/api/videos/{video_id}")
async def get_video(video_id: str, request: Request, refresh: str | None = Query(default=None)) -> Any:
    user_id = current_user_id(request)
    video = db.get_video_by_id(video_id)
    if not video or not is_owned_by_user(video, user_id):
        return api_error("Video not found", status=404)

    should_refresh = refresh == "1"
    if should_refresh and video.get("status") == "processing" and video.get("externalTaskId"):
        try:
            status = await get_video_status(str(video.get("externalTaskId")), str(video.get("type")))
            updated = db.update_video(
                video_id,
                {
                    "status": status.get("status"),
                    "outputUrl": status.get("outputUrl") or video.get("outputUrl"),
                    "error": status.get("error"),
                },
            )
            if updated:
                video = updated
        except Exception as exc:
            return api_error(f"Failed to refresh video status: {exc}", status=500)

    return with_public_upload_urls(video, request_origin(request))


@app.delete("/api/videos/{video_id}")
async def delete_video(video_id: str, request: Request) -> Any:
    try:
        user_id = current_user_id(request)
        video = db.get_video_by_id(video_id)
        if not video or not is_owned_by_user(video, user_id):
            return api_error("Video not found", status=404)
        deleted = db.delete_video(video_id)
        if not deleted:
            return api_error("Video not found", status=404)
        return {"success": True}
    except Exception as exc:
        return api_error(f"Failed to delete video: {exc}", status=500)


ALLOWED_VIDEO_TYPES = {
    "video/mp4",
    "video/quicktime",
    "video/webm",
    "video/x-m4v",
}


@app.get("/api/trend-videos")
async def get_trend_videos(request: Request) -> list[dict[str, str]]:
    trends_dir = PUBLIC_DIR / "uploads" / "trends"
    if not trends_dir.exists():
        return []

    allowed_ext = {".mp4", ".mov", ".webm", ".m4v"}
    files = [
        item
        for item in trends_dir.iterdir()
        if item.is_file() and item.suffix.lower() in allowed_ext
    ]
    files.sort(key=lambda item: item.name.lower())

    origin = request_origin(request)
    return [
        {
            "id": f"trend-{item.stem}",
            "url": f"{origin}/uploads/trends/{item.name}",
        }
        for item in files
    ]


def extension_from_mime_type(mime_type: str) -> str:
    if mime_type == "video/mp4":
        return "mp4"
    if mime_type == "video/quicktime":
        return "mov"
    if mime_type == "video/webm":
        return "webm"
    if mime_type == "video/x-m4v":
        return "m4v"
    return "mp4"


ALLOWED_IMAGE_TYPES = {
    "image/png",
    "image/webp",
    "image/jpg",
    "image/heif",
    "image/avif",
    "image/jpeg",
    "image/gif",
}


@app.post("/api/upload")
async def upload_file(file: UploadFile | None = File(default=None)) -> Any:
    if file is None:
        return api_error("File is required", status=400)
    if file.content_type and file.content_type not in ALLOWED_IMAGE_TYPES:
        return api_error(
            "Unsupported image type. Allowed: image/png, image/webp, image/jpg, image/heif, image/avif, image/jpeg, image/gif",
            status=400,
        )

    try:
        file_content = await file.read()
        filename = file.filename or "upload.bin"
        content_type = file.content_type or "application/octet-stream"

        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.post(
                f"{UPLOAD_API_BASE_URL}/upload",
                files={"file": (filename, file_content, content_type)},
            )

        payload: Any
        try:
            payload = response.json()
        except Exception:
            payload = {"raw": response.text}

        if response.status_code >= 400:
            return api_error(
                str(as_record(payload).get("detail") or as_record(payload).get("error") or f"Upload failed: {response.status_code} {response.reason_phrase}"),
                status=response.status_code,
                upstream=payload,
            )

        url = extract_uploaded_url(payload)
        if not url:
            return api_error(
                "Upload succeeded, but URL is missing",
                status=502,
                upstream=payload,
            )

        return {"url": url, "raw": payload}
    except Exception as exc:
        return api_error(f"Upload failed: {exc}", status=500)


@app.post("/api/upload-video")
async def upload_video(request: Request, file: UploadFile | None = File(default=None)) -> Any:
    if file is None:
        return api_error("File is required", status=400)

    if file.content_type not in ALLOWED_VIDEO_TYPES:
        return api_error(
            "Unsupported video type. Allowed: video/mp4, video/quicktime, video/webm, video/x-m4v",
            status=400,
        )

    try:
        uploads_motion_dir = PUBLIC_DIR / "uploads" / "motion"
        uploads_motion_dir.mkdir(parents=True, exist_ok=True)

        ext = extension_from_mime_type(file.content_type or "")
        safe_name = f"{int(time.time() * 1000)}_{nanoid(8)}.{ext}"
        file_path = uploads_motion_dir / safe_name
        file_path.write_bytes(await file.read())

        return {"url": f"{request_origin(request)}/uploads/motion/{safe_name}"}
    except Exception as exc:
        return api_error(f"Upload failed: {exc}", status=500)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
