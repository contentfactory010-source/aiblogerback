from __future__ import annotations

import asyncio
import json
import mimetypes
import os
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from .db import nanoid
from .settings import (
    KIE_FILE_UPLOAD_BASE_URL,
    NANO_BANANO_API_KEY,
    NANO_BANANO_BASE_URL,
    NANO_BANANO_CALLBACK_URL,
    NANO_BANANO_VEO_BASE_URL,
    NETWORK_RETRY_ATTEMPTS,
    NETWORK_RETRY_DELAY_SECONDS,
    POLL_INTERVAL_SECONDS,
    POLL_MAX_ATTEMPTS,
    PUBLIC_APP_URL,
    PUBLIC_DIR,
)


def _get_api_key() -> str:
    if not NANO_BANANO_API_KEY:
        raise RuntimeError("NANO_BANANO_API_KEY is not set")
    return NANO_BANANO_API_KEY


def _public_origin() -> str:
    try:
        parsed = urlparse(PUBLIC_APP_URL)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
    except Exception:
        pass
    return PUBLIC_APP_URL.rstrip("/")


def _to_public_url(image_ref: str) -> str:
    if image_ref.startswith("http://") or image_ref.startswith("https://"):
        return image_ref
    normalized = image_ref if image_ref.startswith("/") else f"/{image_ref}"
    return f"{_public_origin()}{normalized}"


def _is_localhost(hostname: str) -> bool:
    value = hostname.strip().lower()
    return value in {"localhost", "127.0.0.1", "::1"}


def _to_local_public_path(input_url: str) -> str | None:
    if not input_url:
        return None
    if input_url.startswith("/"):
        return input_url.split("?")[0]

    try:
        parsed = urlparse(input_url)
        if _is_localhost(parsed.hostname or ""):
            return parsed.path.split("?")[0]
        return None
    except Exception:
        if input_url.startswith("http://") or input_url.startswith("https://"):
            return None
        return f"/{input_url.lstrip('/').split('?')[0]}"


def _safe_public_abs_path(local_public_path: str) -> Path:
    public_dir = PUBLIC_DIR.resolve()
    candidate = (PUBLIC_DIR / local_public_path.lstrip("/")).resolve()
    if not str(candidate).startswith(str(public_dir)):
        raise RuntimeError("Invalid local input path")
    return candidate


def _guess_content_type(file_name: str) -> str:
    guessed, _ = mimetypes.guess_type(file_name)
    return guessed or "application/octet-stream"


async def _request_with_retry(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    json_payload: Any | None = None,
    files: Any | None = None,
    data: dict[str, Any] | None = None,
) -> httpx.Response:
    last_error: Exception | None = None
    for attempt in range(1, NETWORK_RETRY_ATTEMPTS + 1):
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                response = await client.request(
                    method,
                    url,
                    headers=headers,
                    json=json_payload,
                    files=files,
                    data=data,
                )
            return response
        except Exception as exc:
            last_error = exc
            if attempt < NETWORK_RETRY_ATTEMPTS:
                await asyncio.sleep(NETWORK_RETRY_DELAY_SECONDS)

    message = str(last_error) if last_error else "unknown network error"
    raise RuntimeError(f"Network request failed for {url}: {message}")


def _parse_payload(response: httpx.Response) -> Any:
    try:
        return response.json()
    except Exception:
        text = response.text.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except Exception:
            return {"raw": text}


def _as_record(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _parse_json_string(value: str) -> Any:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if (text.startswith("{") and text.endswith("}")) or (
        text.startswith("[") and text.endswith("]")
    ):
        try:
            return json.loads(text)
        except Exception:
            return None
    return None


def _collect_http_urls(value: Any, acc: list[str] | None = None) -> list[str]:
    result = acc or []
    if isinstance(value, str):
        parsed = _parse_json_string(value)
        if parsed is not None:
            return _collect_http_urls(parsed, result)
        if value.startswith("http://") or value.startswith("https://"):
            result.append(value)
        return result

    if isinstance(value, list):
        for item in value:
            _collect_http_urls(item, result)
        return result

    if isinstance(value, dict):
        for item in value.values():
            _collect_http_urls(item, result)
        return result

    return result


def _get_by_path(source: Any, path: list[str | int]) -> Any:
    current = source
    for key in path:
        if isinstance(key, int):
            if not isinstance(current, list) or key >= len(current):
                return None
            current = current[key]
            continue
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def _get_string_by_paths(source: Any, paths: list[list[str | int]]) -> str | None:
    for path in paths:
        value = _get_by_path(source, path)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _extract_error_message(payload: Any) -> str | None:
    message = _get_string_by_paths(
        payload,
        [
            ["data", "response", "error"],
            ["data", "response", "message"],
            ["data", "errorMessage"],
            ["data", "reason"],
            ["data", "failReason"],
            ["error"],
            ["message"],
            ["msg"],
            ["data", "error"],
            ["data", "message"],
        ],
    )
    if not message:
        return None
    return None if message.lower() == "success" else message


def _extract_task_id(payload: Any) -> str | None:
    task_id = _get_string_by_paths(
        payload,
        [
            ["taskId"],
            ["task_id"],
            ["jobId"],
            ["id"],
            ["data", "taskId"],
            ["data", "task_id"],
            ["data", "id"],
            ["result", "taskId"],
            ["result", "id"],
            ["response", "taskId"],
            ["response", "id"],
        ],
    )
    if task_id:
        return task_id

    snapshot = json.dumps(payload, ensure_ascii=False)
    match = re.search(r"task_[a-zA-Z0-9_-]+", snapshot)
    return match.group(0) if match else None


def _extract_task_status(payload: Any) -> str | None:
    status = _get_string_by_paths(
        payload,
        [
            ["status"],
            ["taskStatus"],
            ["state"],
            ["data", "status"],
            ["data", "taskStatus"],
            ["data", "state"],
        ],
    )
    return status.lower() if status else None


def _looks_like_video_url(value: str) -> bool:
    return bool(re.search(r"\.(mp4|mov|webm|mkv|avi|m4v)(\?.*)?$", value.lower()))


def _normalize_url_for_compare(url: str) -> str:
    try:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/").lower()
    except Exception:
        return url.rstrip("/").lower()


def _extract_uploaded_url(payload: Any) -> str | None:
    direct = _get_string_by_paths(
        payload,
        [
            ["url"],
            ["fileUrl"],
            ["data", "url"],
            ["data", "fileUrl"],
            ["data", "file", "url"],
            ["result", "url"],
            ["result", "fileUrl"],
        ],
    )
    if direct and (direct.startswith("http://") or direct.startswith("https://")):
        return direct

    for url in _collect_http_urls(payload):
        if url.startswith("http://") or url.startswith("https://"):
            return url
    return None


def _extract_image_url(payload: Any, forbidden_urls: set[str] | None = None) -> str | None:
    forbidden = forbidden_urls or set()
    direct = _get_string_by_paths(
        payload,
        [
            ["result", "imageUrl"],
            ["result", "image"],
            ["result", "url"],
            ["output", "imageUrl"],
            ["output", "url"],
            ["data", "result", "imageUrl"],
            ["data", "result", "url"],
            ["data", "output", "imageUrl"],
            ["data", "output", "url"],
            ["imageUrl"],
            ["resultUrl"],
        ],
    )
    if direct and not _looks_like_video_url(direct):
        normalized = _normalize_url_for_compare(direct)
        if normalized not in forbidden:
            return direct

    for url in _collect_http_urls(payload):
        if _looks_like_video_url(url):
            continue
        normalized = _normalize_url_for_compare(url)
        if normalized in forbidden:
            continue
        return url
    return None


def _extract_video_url(payload: Any) -> str | None:
    direct = _get_string_by_paths(
        payload,
        [
            ["data", "resultUrls", 0],
            ["data", "result", "resultUrls", 0],
            ["data", "result", "videoUrl"],
            ["data", "result", "url"],
            ["data", "response", "resultUrls", 0],
            ["resultUrls", 0],
            ["originUrls", 0],
            ["outputUrl"],
            ["videoUrl"],
            ["url"],
        ],
    )
    if direct and _looks_like_video_url(direct):
        return direct

    for url in _collect_http_urls(payload):
        if _looks_like_video_url(url):
            return url
    return None


def _extract_motion_control_output_url(payload: Any) -> str | None:
    result_json_raw = _get_string_by_paths(
        payload,
        [
            ["data", "resultJson"],
            ["resultJson"],
            ["data", "response", "resultJson"],
        ],
    )
    if not result_json_raw:
        return None

    parsed = _parse_json_string(result_json_raw)
    if parsed is None:
        return None

    direct = _get_string_by_paths(
        parsed,
        [
            ["resultUrls", 0],
            ["originUrls", 0],
            ["outputUrl"],
            ["videoUrl"],
            ["url"],
            ["data", "resultUrls", 0],
            ["data", "outputUrl"],
            ["data", "url"],
        ],
    )
    if direct:
        return direct

    urls = _collect_http_urls(parsed)
    return urls[0] if urls else None


def _assert_public_urls(urls: list[str]) -> None:
    for value in urls:
        parsed = urlparse(value)
        if _is_localhost(parsed.hostname or ""):
            raise RuntimeError(
                "Input URL points to localhost. Set NANO_BANANO_PUBLIC_BASE_URL to a public host."
            )


async def _upload_local_input_to_server(local_public_path: str) -> str:
    api_key = _get_api_key()
    abs_path = _safe_public_abs_path(local_public_path)
    if not abs_path.exists():
        raise RuntimeError(f"Local input file not found: {local_public_path}")

    content = abs_path.read_bytes()
    file_name = abs_path.name or f"reference_{int(time.time() * 1000)}"
    content_type = _guess_content_type(file_name)

    response = await _request_with_retry(
        "POST",
        f"{KIE_FILE_UPLOAD_BASE_URL}/api/file-stream-upload",
        headers={"Authorization": f"Bearer {api_key}"},
        files={"file": (file_name, content, content_type)},
        data={"uploadPath": "motion-control", "fileName": f"{int(time.time() * 1000)}_{file_name}"},
    )
    payload = _parse_payload(response)
    if response.status_code >= 400:
        details = _extract_error_message(payload) or json.dumps(payload, ensure_ascii=False)
        raise RuntimeError(
            f"Local input upload failed: {response.status_code} {response.reason_phrase} - {details}"
        )

    uploaded_url = _extract_uploaded_url(payload)
    if not uploaded_url:
        raise RuntimeError("Local input upload succeeded, but URL is missing")

    return uploaded_url


async def _ensure_public_input_url(input_url: str) -> str:
    local_path = _to_local_public_path(input_url)
    if not local_path:
        return input_url
    return await _upload_local_input_to_server(local_path)


async def _ensure_public_input_urls(urls: list[str]) -> list[str]:
    return [await _ensure_public_input_url(item) for item in urls]


async def _fetch_task_record_info(task_id: str) -> Any:
    api_key = _get_api_key()
    response = await _request_with_retry(
        "GET",
        f"{NANO_BANANO_BASE_URL}/jobs/recordInfo?taskId={task_id}",
        headers={"Authorization": f"Bearer {api_key}"},
    )
    payload = _parse_payload(response)
    if response.status_code >= 400:
        details = _extract_error_message(payload) or json.dumps(payload, ensure_ascii=False)
        raise RuntimeError(
            f"Nano Banano recordInfo error: {response.status_code} {response.reason_phrase} - {details}"
        )
    return payload


async def _poll_task_until_finished(task_id: str) -> Any:
    for _ in range(POLL_MAX_ATTEMPTS):
        payload = await _fetch_task_record_info(task_id)
        status = _extract_task_status(payload)

        if status == "success":
            return payload

        if status in {"fail", "failed"}:
            details = _extract_error_message(payload) or json.dumps(payload, ensure_ascii=False)[:1000]
            raise RuntimeError(f"Nano Banano generation failed: {details}")

        await asyncio.sleep(POLL_INTERVAL_SECONDS)

    raise RuntimeError("Nano Banano generation timeout: task is not completed")


async def _download_image_to_server(image_url: str, task_id: str) -> str:
    response = await _request_with_retry("GET", image_url)
    if response.status_code >= 400:
        raise RuntimeError(
            f"Failed to download generated image: {response.status_code} {response.reason_phrase}"
        )

    content_type = response.headers.get("content-type", "")
    extension = "png"
    if "jpeg" in content_type:
        extension = "jpg"
    elif "webp" in content_type:
        extension = "webp"
    elif "gif" in content_type:
        extension = "gif"

    safe_task_id = re.sub(r"[^a-zA-Z0-9_-]", "", task_id) or "task"
    file_name = f"{safe_task_id}_{int(time.time() * 1000)}.{extension}"
    upload_dir = PUBLIC_DIR / "uploads" / "bloggers"
    upload_dir.mkdir(parents=True, exist_ok=True)
    file_path = upload_dir / file_name
    file_path.write_bytes(response.content)
    return f"/uploads/bloggers/{file_name}"


async def _create_nano_banano_task(
    *,
    prompt: str,
    aspect_ratio: str = "3:4",
    resolution: str = "2K",
    output_format: str = "png",
    model: str = "nano-banana-2",
    image_input: list[str] | None = None,
    google_search: bool | None = None,
) -> str:
    api_key = _get_api_key()

    prepared_images = [_to_public_url(item) for item in image_input] if image_input else []
    if prepared_images:
        _assert_public_urls(prepared_images)

    body: dict[str, Any] = {
        "model": model,
        "input": {
            "prompt": prompt,
            "aspect_ratio": aspect_ratio,
            "resolution": resolution,
            "output_format": output_format,
        },
    }
    if isinstance(google_search, bool):
        body["input"]["google_search"] = google_search
    if prepared_images:
        body["input"]["image_input"] = prepared_images
    if NANO_BANANO_CALLBACK_URL:
        body["callBackUrl"] = NANO_BANANO_CALLBACK_URL

    response = await _request_with_retry(
        "POST",
        f"{NANO_BANANO_BASE_URL}/jobs/createTask",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json_payload=body,
    )
    payload = _parse_payload(response)
    if response.status_code >= 400:
        details = _extract_error_message(payload) or json.dumps(payload, ensure_ascii=False)
        raise RuntimeError(
            f"Nano Banano API error: {response.status_code} {response.reason_phrase} - {details}"
        )

    task_id = _extract_task_id(payload)
    if not task_id:
        raise RuntimeError(
            f"Nano Banano createTask returned empty taskId. payload={json.dumps(payload, ensure_ascii=False)[:1000]}"
        )
    return task_id


async def _create_character_from_options(
    *,
    prompt: str,
    reference_images: list[str] | None = None,
    aspect_ratio: str = "3:4",
    resolution: str = "2K",
    output_format: str = "png",
    google_search: bool | None = None,
) -> dict[str, Any]:
    prepared_reference_images = (
        await _ensure_public_input_urls([_to_public_url(item) for item in reference_images])
        if reference_images
        else []
    )
    if prepared_reference_images:
        _assert_public_urls(prepared_reference_images)

    task_id = await _create_nano_banano_task(
        prompt=prompt,
        aspect_ratio=aspect_ratio,
        resolution=resolution,
        output_format=output_format,
        image_input=prepared_reference_images,
        google_search=google_search,
    )

    record = await _poll_task_until_finished(task_id)
    forbidden = {_normalize_url_for_compare(item) for item in prepared_reference_images}
    image_url = _extract_image_url(record, forbidden_urls=forbidden)
    if not image_url:
        snapshot = json.dumps(record, ensure_ascii=False)[:1200]
        raise RuntimeError(
            f"Nano Banano returned success, but image URL is missing. recordInfo={snapshot}"
        )

    local_image_path = await _download_image_to_server(image_url, task_id)

    return {
        "id": task_id,
        "success": True,
        "imageUrl": local_image_path,
    }


async def _create_veo_task(params: dict[str, Any]) -> str:
    api_key = _get_api_key()

    source_image_urls: list[str] = []
    image_urls = params.get("imageUrls")
    if isinstance(image_urls, list):
        source_image_urls = [str(item) for item in image_urls if isinstance(item, str) and item.strip()]
    if not source_image_urls and isinstance(params.get("referenceImage"), str):
        source_image_urls = [str(params["referenceImage"])]

    prepared_image_urls = await _ensure_public_input_urls([_to_public_url(item) for item in source_image_urls])
    if prepared_image_urls:
        _assert_public_urls(prepared_image_urls)

    aspect_ratio = params.get("aspectRatio") or ("Auto" if params.get("type") == "ugc" else "16:9")

    body: dict[str, Any] = {
        "prompt": str(params.get("prompt") or ""),
        "model": "veo3_fast",
        "aspect_ratio": aspect_ratio,
        "enableTranslation": True,
        "generationType": "REFERENCE_2_VIDEO" if prepared_image_urls else "TEXT_2_VIDEO",
    }
    if prepared_image_urls:
        body["imageUrls"] = prepared_image_urls
    if NANO_BANANO_CALLBACK_URL:
        body["callBackUrl"] = NANO_BANANO_CALLBACK_URL

    response = await _request_with_retry(
        "POST",
        f"{NANO_BANANO_VEO_BASE_URL}/api/v1/veo/generate",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json_payload=body,
    )
    payload = _parse_payload(response)

    if response.status_code >= 400:
        details = _extract_error_message(payload) or json.dumps(payload, ensure_ascii=False)
        raise RuntimeError(
            f"Kie Veo API error: {response.status_code} {response.reason_phrase} - {details}"
        )

    code = _as_record(payload).get("code")
    if isinstance(code, (int, float)) and int(code) != 200:
        details = _extract_error_message(payload) or _get_string_by_paths(payload, [["msg"]]) or json.dumps(payload)
        raise RuntimeError(f"Kie Veo task rejected: {details}")

    external_id = _get_string_by_paths(payload, [["data", "taskId"], ["taskId"], ["data", "id"], ["id"]])
    if not external_id:
        raise RuntimeError(f"Kie Veo response does not contain task id. payload={json.dumps(payload)[:1000]}")

    return external_id


async def _create_motion_control_task(params: dict[str, Any]) -> str:
    api_key = _get_api_key()

    input_sources = [
        _to_public_url(str(item))
        for item in (params.get("imageUrls") or [])
        if isinstance(item, str) and item.strip()
    ]
    video_sources = [
        _to_public_url(str(item))
        for item in (params.get("videoUrls") or [])
        if isinstance(item, str) and item.strip()
    ]

    if not input_sources:
        raise RuntimeError("Motion control requires at least one input image URL")
    if not video_sources:
        raise RuntimeError("Motion control requires at least one input video URL")

    input_urls = await _ensure_public_input_urls(input_sources)
    video_urls = await _ensure_public_input_urls(video_sources)
    _assert_public_urls(input_urls)
    _assert_public_urls(video_urls)

    input_payload: dict[str, Any] = {
        "input_urls": input_urls,
        "video_urls": video_urls,
        "character_orientation": params.get("motionOrientation") or "video",
        "mode": params.get("motionMode") or "720p",
    }
    prompt = str(params.get("prompt") or "").strip()
    if prompt:
        input_payload["prompt"] = prompt

    body: dict[str, Any] = {
        "model": "kling-2.6/motion-control",
        "input": input_payload,
    }
    if NANO_BANANO_CALLBACK_URL:
        body["callBackUrl"] = NANO_BANANO_CALLBACK_URL

    response = await _request_with_retry(
        "POST",
        f"{NANO_BANANO_BASE_URL}/jobs/createTask",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json_payload=body,
    )
    payload = _parse_payload(response)

    if response.status_code >= 400:
        details = _extract_error_message(payload) or json.dumps(payload, ensure_ascii=False)
        raise RuntimeError(
            f"Kie Motion Control API error: {response.status_code} {response.reason_phrase} - {details}"
        )

    code = _as_record(payload).get("code")
    if isinstance(code, (int, float)) and int(code) != 200:
        details = _extract_error_message(payload) or _get_string_by_paths(payload, [["msg"]]) or json.dumps(payload)
        raise RuntimeError(f"Kie Motion Control task rejected: {details}")

    task_id = _extract_task_id(payload)
    if not task_id:
        raise RuntimeError(f"Motion Control response does not contain task id. payload={json.dumps(payload)[:1000]}")
    return task_id


async def create_character(prompt: str) -> dict[str, Any]:
    return await _create_character_from_options(
        prompt=prompt,
        aspect_ratio="3:4",
        resolution="2K",
        output_format="png",
    )


async def create_character_with_reference(options: dict[str, Any]) -> dict[str, Any]:
    return await _create_character_from_options(
        prompt=str(options.get("prompt") or ""),
        reference_images=[str(item) for item in options.get("referenceImages", [])],
        aspect_ratio=str(options.get("aspectRatio") or "3:4"),
        resolution=str(options.get("resolution") or "2K"),
        output_format=str(options.get("outputFormat") or "png"),
        google_search=bool(options.get("googleSearch")) if options.get("googleSearch") is not None else None,
    )


async def generate_video(params: dict[str, Any]) -> dict[str, Any]:
    video_type = str(params.get("type") or "")
    external_id = (
        await _create_motion_control_task(params)
        if video_type == "motion_control"
        else await _create_veo_task(params)
    )

    return {
        "id": external_id or f"nb_video_{int(time.time() * 1000)}",
        "status": "processing",
        "outputUrl": None,
    }


async def get_video_status(external_id: str, video_type: str) -> dict[str, Any]:
    if video_type == "motion_control":
        payload = await _fetch_task_record_info(external_id)
        status = _extract_task_status(payload)
        error = _extract_error_message(payload)
        output_url = _extract_motion_control_output_url(payload) or _extract_video_url(payload)

        if status in {"success", "succeed", "done", "completed"}:
            return {"status": "done", "outputUrl": output_url}
        if status in {"failed", "error", "cancelled", "canceled"}:
            return {"status": "failed", "error": error or "Motion control task failed"}

        data = _as_record(_as_record(payload).get("data"))
        success_flag_raw = data.get("successFlag")
        try:
            success_flag = int(success_flag_raw)
        except Exception:
            success_flag = None

        if success_flag == 1:
            return {"status": "done", "outputUrl": output_url}
        if success_flag in {2, 3}:
            return {"status": "failed", "error": error or "Motion control task failed"}

        return {"status": "processing"}

    api_key = _get_api_key()
    response = await _request_with_retry(
        "GET",
        f"{NANO_BANANO_VEO_BASE_URL}/api/v1/veo/record-info?taskId={external_id}",
        headers={"Authorization": f"Bearer {api_key}"},
    )
    payload = _parse_payload(response)

    if response.status_code >= 400:
        details = _extract_error_message(payload) or json.dumps(payload, ensure_ascii=False)
        raise RuntimeError(
            f"Kie Veo status API error: {response.status_code} {response.reason_phrase} - {details}"
        )

    code = _as_record(payload).get("code")
    if isinstance(code, (int, float)) and int(code) != 200:
        return {
            "status": "failed",
            "error": _extract_error_message(payload) or _get_string_by_paths(payload, [["msg"]]) or "Veo status request failed",
        }

    data = _as_record(_as_record(payload).get("data"))
    success_flag_raw = data.get("successFlag")
    try:
        success_flag = int(success_flag_raw)
    except Exception:
        success_flag = None

    if success_flag == 1:
        return {
            "status": "done",
            "outputUrl": _extract_video_url(payload),
        }

    if success_flag in {2, 3}:
        return {
            "status": "failed",
            "error": _extract_error_message(payload) or "Veo task failed",
        }

    return {"status": "processing"}
