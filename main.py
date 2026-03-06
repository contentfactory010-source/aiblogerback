from __future__ import annotations

import json
import re
import time
from typing import Any, Literal

import httpx
from fastapi import FastAPI, File, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .db import db, nanoid
from .nano_banano import (
    create_character,
    create_character_with_reference,
    generate_video,
    get_video_status,
)
from .settings import PUBLIC_DIR, UPLOAD_API_BASE_URL

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
    motionOrientation: Literal["video", "image"] | None = None
    motionMode: Literal["720p", "1080p"] | None = None
    aspectRatio: Literal["16:9", "9:16", "Auto"] | None = None


@app.get("/api/bloggers")
async def get_bloggers(request: Request) -> Any:
    try:
        return with_public_upload_urls(db.get_all_bloggers(), request_origin(request))
    except Exception as exc:
        return api_error(f"Failed to fetch bloggers: {exc}", status=500)


@app.post("/api/bloggers")
async def create_blogger(payload: CreateBloggerRequest, request: Request) -> Any:
    try:
        blogger = db.create_blogger(
            {
                "name": payload.name,
                "prompt": payload.prompt,
                "baseImage": payload.baseImage,
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
    blogger = db.get_blogger_by_id(blogger_id)
    if not blogger:
        return api_error("Blogger not found", status=404)
    return with_public_upload_urls(blogger, request_origin(request))


@app.patch("/api/bloggers/{blogger_id}")
async def patch_blogger(blogger_id: str, body: dict[str, Any], request: Request) -> Any:
    blogger = db.get_blogger_by_id(blogger_id)
    if not blogger:
        return api_error("Blogger not found", status=404)

    updated = db.update_blogger(blogger_id, body)
    return with_public_upload_urls(updated, request_origin(request))


@app.delete("/api/bloggers/{blogger_id}")
async def delete_blogger(blogger_id: str) -> Any:
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
    blogger = db.get_blogger_by_id(blogger_id)
    if not blogger:
        return api_error("Blogger not found", status=404)

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
            },
            request_origin(request),
        )
    except Exception as exc:
        return api_error(f"Failed to create in Nano Banano: {exc}", status=500)


@app.post("/api/bloggers/{blogger_id}/looks")
async def create_look(blogger_id: str, payload: CreateLookRequest, request: Request) -> Any:
    blogger = db.get_blogger_by_id(blogger_id)
    if not blogger:
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

        if not primary_reference_image:
            return api_error(
                "Нужен минимум один существующий образ блоггера для image_input",
                status=400,
            )

        if mode == "clone" and len(uploaded_reference_images) == 0:
            return api_error("Для клонирования нужен второй референс", status=400)

        reference_images = [primary_reference_image]
        for reference in uploaded_reference_images:
            if reference != primary_reference_image:
                reference_images.append(reference)

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
        return with_public_upload_urls(updated, request_origin(request))
    except Exception as exc:
        return api_error(f"Failed to create look: {exc}", status=500)


@app.post("/api/bloggers/{blogger_id}/assets")
async def create_asset(blogger_id: str, payload: CreateAssetRequest, request: Request) -> Any:
    blogger = db.get_blogger_by_id(blogger_id)
    if not blogger:
        return api_error("Blogger not found", status=404)

    if payload.action == "upload" and not payload.imageUrl:
        return api_error("Для загрузки нужен imageUrl", status=400)
    if payload.action == "generate" and not payload.prompt:
        return api_error("Для генерации нужен промпт", status=400)

    try:
        image_ref = (
            str(payload.imageUrl)
            if payload.action == "upload"
            else (await create_character(str(payload.prompt or ""))).get("imageUrl")
        )
        if not image_ref:
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
        return with_public_upload_urls(updated, request_origin(request))
    except Exception as exc:
        return api_error(f"Failed to create item: {exc}", status=500)


@app.get("/api/videos")
async def get_videos(request: Request, bloggerId: str | None = Query(default=None)) -> Any:
    try:
        all_videos = db.get_all_videos()
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
        blogger = db.get_blogger_by_id(payload.bloggerId)
        if not blogger:
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
                "type": payload.type,
                "prompt": trimmed_prompt,
                "lookId": payload.lookId,
                "status": nb_result.get("status"),
                "outputUrl": nb_result.get("outputUrl"),
            }
        )

        return with_public_upload_urls(video, request_origin(request))
    except Exception as exc:
        return api_error(str(exc), status=500)


@app.get("/api/videos/{video_id}")
async def get_video(video_id: str, request: Request, refresh: str | None = Query(default=None)) -> Any:
    video = db.get_video_by_id(video_id)
    if not video:
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
async def delete_video(video_id: str) -> Any:
    try:
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


@app.post("/api/upload")
async def upload_file(file: UploadFile | None = File(default=None)) -> Any:
    if file is None:
        return api_error("File is required", status=400)

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
