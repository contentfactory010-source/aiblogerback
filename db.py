from __future__ import annotations

import json
import secrets
import string
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .settings import DATA_DIR, LEGACY_DATA_DIR

BLOGGERS_FILE = DATA_DIR / "bloggers.json"
VIDEOS_FILE = DATA_DIR / "videos.json"

_ID_ALPHABET = string.ascii_letters + string.digits


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def nanoid(size: int = 21) -> str:
    return "".join(secrets.choice(_ID_ALPHABET) for _ in range(size))


def normalize_blogger_record(value: dict[str, Any]) -> dict[str, Any]:
    return {
        **value,
        "looks": value.get("looks") if isinstance(value.get("looks"), list) else [],
        "clothes": value.get("clothes") if isinstance(value.get("clothes"), list) else [],
        "home": value.get("home") if isinstance(value.get("home"), list) else [],
        "cars": value.get("cars") if isinstance(value.get("cars"), list) else [],
        "relatives": value.get("relatives") if isinstance(value.get("relatives"), list) else [],
    }


class JsonDB:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._migrated_legacy_data = False

    def _ensure_data_dir(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        if self._migrated_legacy_data:
            return

        legacy_files = {
            BLOGGERS_FILE: LEGACY_DATA_DIR / "bloggers.json",
            VIDEOS_FILE: LEGACY_DATA_DIR / "videos.json",
        }
        for target_file, legacy_file in legacy_files.items():
            if target_file.exists() or not legacy_file.exists():
                continue
            try:
                target_file.write_bytes(legacy_file.read_bytes())
            except Exception:
                # Keep going; backend can still start with empty storage.
                continue

        self._migrated_legacy_data = True

    def _read_json(self, file_path: Path) -> list[dict[str, Any]]:
        self._ensure_data_dir()
        if not file_path.exists():
            return []
        try:
            raw = file_path.read_text(encoding="utf-8")
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []

    def _write_json(self, file_path: Path, payload: list[dict[str, Any]]) -> None:
        self._ensure_data_dir()
        file_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def get_all_bloggers(self) -> list[dict[str, Any]]:
        with self._lock:
            bloggers = self._read_json(BLOGGERS_FILE)
            return [normalize_blogger_record(item) for item in bloggers if isinstance(item, dict)]

    def get_blogger_by_id(self, blogger_id: str) -> dict[str, Any] | None:
        bloggers = self.get_all_bloggers()
        return next((item for item in bloggers if item.get("id") == blogger_id), None)

    def create_blogger(self, data: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            bloggers = [
                normalize_blogger_record(item)
                for item in self._read_json(BLOGGERS_FILE)
                if isinstance(item, dict)
            ]
            blogger = normalize_blogger_record(
                {
                    **data,
                    "id": nanoid(),
                    "createdAt": now_iso(),
                }
            )
            bloggers.append(blogger)
            self._write_json(BLOGGERS_FILE, bloggers)
            return blogger

    def update_blogger(self, blogger_id: str, data: dict[str, Any]) -> dict[str, Any] | None:
        with self._lock:
            bloggers = [
                normalize_blogger_record(item)
                for item in self._read_json(BLOGGERS_FILE)
                if isinstance(item, dict)
            ]
            for index, blogger in enumerate(bloggers):
                if blogger.get("id") != blogger_id:
                    continue
                updated = normalize_blogger_record({**blogger, **data})
                bloggers[index] = updated
                self._write_json(BLOGGERS_FILE, bloggers)
                return updated
            return None

    def delete_blogger(self, blogger_id: str) -> bool:
        with self._lock:
            bloggers = [
                normalize_blogger_record(item)
                for item in self._read_json(BLOGGERS_FILE)
                if isinstance(item, dict)
            ]
            filtered = [item for item in bloggers if item.get("id") != blogger_id]
            if len(filtered) == len(bloggers):
                return False
            self._write_json(BLOGGERS_FILE, filtered)
            return True

    def get_all_videos(self) -> list[dict[str, Any]]:
        with self._lock:
            videos = self._read_json(VIDEOS_FILE)
            return [item for item in videos if isinstance(item, dict)]

    def get_video_by_id(self, video_id: str) -> dict[str, Any] | None:
        videos = self.get_all_videos()
        return next((item for item in videos if item.get("id") == video_id), None)

    def create_video(self, data: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            videos = [item for item in self._read_json(VIDEOS_FILE) if isinstance(item, dict)]
            now = now_iso()
            video = {
                **data,
                "id": nanoid(),
                "createdAt": now,
                "updatedAt": now,
            }
            videos.append(video)
            self._write_json(VIDEOS_FILE, videos)
            return video

    def update_video(self, video_id: str, data: dict[str, Any]) -> dict[str, Any] | None:
        with self._lock:
            videos = [item for item in self._read_json(VIDEOS_FILE) if isinstance(item, dict)]
            for index, video in enumerate(videos):
                if video.get("id") != video_id:
                    continue
                updated = {
                    **video,
                    **data,
                    "updatedAt": now_iso(),
                }
                videos[index] = updated
                self._write_json(VIDEOS_FILE, videos)
                return updated
            return None

    def delete_video(self, video_id: str) -> bool:
        with self._lock:
            videos = [item for item in self._read_json(VIDEOS_FILE) if isinstance(item, dict)]
            filtered = [item for item in videos if item.get("id") != video_id]
            if len(filtered) == len(videos):
                return False
            self._write_json(VIDEOS_FILE, filtered)
            return True

    def delete_videos_by_blogger_id(self, blogger_id: str) -> int:
        with self._lock:
            videos = [item for item in self._read_json(VIDEOS_FILE) if isinstance(item, dict)]
            filtered = [item for item in videos if item.get("bloggerId") != blogger_id]
            deleted_count = len(videos) - len(filtered)
            if deleted_count > 0:
                self._write_json(VIDEOS_FILE, filtered)
            return deleted_count


db = JsonDB()
