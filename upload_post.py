from __future__ import annotations

import json
import re
from typing import Any, Iterable
from urllib.parse import quote

import httpx

from .settings import UPLOAD_POST_API_BASE_URL, UPLOAD_POST_API_KEY

CONNECT_PLATFORMS = {
    "tiktok",
    "instagram",
    "linkedin",
    "youtube",
    "facebook",
    "x",
    "threads",
}

PUBLISH_PLATFORMS = {
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
}


class UploadPostError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int = 502,
        upstream: Any | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.upstream = upstream


def _as_record(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _parse_payload(response: httpx.Response) -> Any:
    try:
        return response.json()
    except Exception:
        text = response.text.strip()
        if not text:
            return {}
        try:
            return json.loads(text)
        except Exception:
            return {"raw": text}


def _extract_error_message(value: Any) -> str | None:
    if isinstance(value, str):
        text = value.strip()
        return text or None

    if isinstance(value, list):
        for item in value:
            extracted = _extract_error_message(item)
            if extracted:
                return extracted
        return None

    if isinstance(value, dict):
        for key in ("error", "message", "detail", "msg", "reason"):
            extracted = _extract_error_message(value.get(key))
            if extracted:
                return extracted
        for item in value.values():
            extracted = _extract_error_message(item)
            if extracted:
                return extracted
    return None


def _require_api_key() -> str:
    api_key = UPLOAD_POST_API_KEY.strip()
    if not api_key:
        raise UploadPostError(
            "UPLOAD_POST_API_KEY is not configured",
            status_code=500,
        )
    return api_key


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Apikey {_require_api_key()}",
        "Accept": "application/json",
    }


def _base_url() -> str:
    return UPLOAD_POST_API_BASE_URL.rstrip("/")


async def _request(
    method: str,
    path: str,
    *,
    json_payload: Any | None = None,
    data: Any | None = None,
    files: Any | None = None,
    params: dict[str, Any] | None = None,
) -> Any:
    url = f"{_base_url()}{path}"
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.request(
                method,
                url,
                headers=_headers(),
                json=json_payload,
                data=data,
                files=files,
                params=params,
            )
    except UploadPostError:
        raise
    except Exception as exc:
        raise UploadPostError(f"Upload-Post network error: {exc}") from exc

    payload = _parse_payload(response)
    if response.status_code >= 400:
        message = _extract_error_message(payload) or (
            f"Upload-Post error: {response.status_code} {response.reason_phrase}"
        )
        raise UploadPostError(
            message,
            status_code=response.status_code,
            upstream=payload,
        )
    return payload


def build_upload_post_username(user_id: str, email: str | None = None) -> str:
    source = user_id.strip() or (email or "").strip().split("@")[0]
    normalized = re.sub(r"[^a-zA-Z0-9_-]+", "-", source).strip("-").lower()
    if not normalized:
        normalized = "user"
    return f"ai_{normalized[:48]}"


def _normalize_platforms(
    platforms: Iterable[str],
    *,
    allowed: set[str],
) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in platforms:
        value = str(item).strip().lower()
        if not value or value in seen:
            continue
        if value not in allowed:
            raise UploadPostError(
                f"Unsupported platform: {value}",
                status_code=400,
            )
        seen.add(value)
        result.append(value)
    return result


async def verify_api_key() -> dict[str, Any]:
    payload = await _request("GET", "/uploadposts/me")
    return _as_record(payload)


async def get_user_profile(username: str) -> dict[str, Any]:
    normalized = username.strip()
    if not normalized:
        raise UploadPostError("Upload-Post username is required", status_code=400)
    payload = await _request("GET", f"/uploadposts/users/{quote(normalized, safe='')}")
    return _as_record(_as_record(payload).get("profile"))


async def get_user_profile_or_none(username: str) -> dict[str, Any] | None:
    try:
        return await get_user_profile(username)
    except UploadPostError as exc:
        if exc.status_code == 404:
            return None
        raise


async def ensure_user_profile(username: str) -> dict[str, Any]:
    normalized = username.strip()
    if not normalized:
        raise UploadPostError("Upload-Post username is required", status_code=400)

    existing_profile = await get_user_profile_or_none(normalized)
    if existing_profile is not None:
        return existing_profile

    try:
        payload = await _request(
            "POST",
            "/uploadposts/users",
            json_payload={"username": normalized},
        )
    except UploadPostError as exc:
        if exc.status_code != 409:
            raise
        return await get_user_profile(normalized)

    profile = _as_record(_as_record(payload).get("profile"))
    if profile:
        return profile
    return await get_user_profile(normalized)


async def delete_user_profile(username: str) -> dict[str, Any]:
    normalized = username.strip()
    if not normalized:
        raise UploadPostError("Upload-Post username is required", status_code=400)
    payload = await _request(
        "DELETE",
        "/uploadposts/users",
        json_payload={"username": normalized},
    )
    return _as_record(payload)


async def generate_connect_url(
    username: str,
    *,
    redirect_url: str | None = None,
    redirect_button_text: str | None = None,
    connect_title: str | None = None,
    connect_description: str | None = None,
    platforms: Iterable[str] | None = None,
    show_calendar: bool | None = None,
) -> dict[str, Any]:
    normalized = username.strip()
    if not normalized:
        raise UploadPostError("Upload-Post username is required", status_code=400)

    await ensure_user_profile(normalized)

    body: dict[str, Any] = {"username": normalized}
    if redirect_url and redirect_url.strip():
        body["redirect_url"] = redirect_url.strip()
    if redirect_button_text and redirect_button_text.strip():
        body["redirect_button_text"] = redirect_button_text.strip()
    if connect_title and connect_title.strip():
        body["connect_title"] = connect_title.strip()
    if connect_description and connect_description.strip():
        body["connect_description"] = connect_description.strip()
    if show_calendar is not None:
        body["show_calendar"] = bool(show_calendar)
    if platforms is not None:
        normalized_platforms = _normalize_platforms(
            platforms,
            allowed=CONNECT_PLATFORMS,
        )
        if normalized_platforms:
            body["platforms"] = normalized_platforms

    payload = await _request(
        "POST",
        "/uploadposts/users/generate-jwt",
        json_payload=body,
    )
    record = _as_record(payload)
    access_url = str(record.get("access_url") or "").strip()
    if not access_url:
        raise UploadPostError(
            "Upload-Post returned empty access URL",
            status_code=502,
            upstream=payload,
        )
    return record


async def publish_video_url(
    *,
    username: str,
    video_url: str,
    platforms: Iterable[str],
    title: str | None = None,
    description: str | None = None,
    scheduled_date: str | None = None,
    timezone: str | None = None,
    async_upload: bool = True,
) -> dict[str, Any]:
    normalized_user = username.strip()
    if not normalized_user:
        raise UploadPostError("Upload-Post username is required", status_code=400)

    normalized_video_url = video_url.strip()
    if not normalized_video_url:
        raise UploadPostError("Video URL is required for publication", status_code=400)
    if not (
        normalized_video_url.startswith("http://")
        or normalized_video_url.startswith("https://")
    ):
        raise UploadPostError("Video URL must be absolute http(s) URL", status_code=400)

    normalized_platforms = _normalize_platforms(
        platforms,
        allowed=PUBLISH_PLATFORMS,
    )
    if len(normalized_platforms) == 0:
        raise UploadPostError(
            "At least one platform is required",
            status_code=400,
        )

    await ensure_user_profile(normalized_user)

    multipart_fields: list[tuple[str, tuple[None, str]]] = [
        ("user", (None, normalized_user)),
        ("video", (None, normalized_video_url)),
        ("async_upload", (None, "true" if async_upload else "false")),
    ]
    for platform in normalized_platforms:
        multipart_fields.append(("platform[]", (None, platform)))
    if title and title.strip():
        multipart_fields.append(("title", (None, title.strip())))
    if description and description.strip():
        multipart_fields.append(("description", (None, description.strip())))
    if scheduled_date and scheduled_date.strip():
        multipart_fields.append(("scheduled_date", (None, scheduled_date.strip())))
    if timezone and timezone.strip():
        multipart_fields.append(("timezone", (None, timezone.strip())))

    payload = await _request("POST", "/upload", files=multipart_fields)
    record = _as_record(payload)
    if record.get("success") is False:
        message = _extract_error_message(payload) or "Upload-Post rejected publication"
        raise UploadPostError(
            message,
            status_code=502,
            upstream=payload,
        )
    return record


async def get_publish_status(
    *,
    request_id: str | None = None,
    job_id: str | None = None,
) -> dict[str, Any]:
    normalized_request_id = (request_id or "").strip()
    normalized_job_id = (job_id or "").strip()
    if not normalized_request_id and not normalized_job_id:
        raise UploadPostError(
            "request_id or job_id is required",
            status_code=400,
        )

    params: dict[str, Any] = {}
    if normalized_request_id:
        params["request_id"] = normalized_request_id
    if normalized_job_id:
        params["job_id"] = normalized_job_id

    payload = await _request("GET", "/uploadposts/status", params=params)
    return _as_record(payload)
