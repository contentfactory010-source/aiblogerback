from __future__ import annotations

import json
import secrets
import sqlite3
import string
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .settings import DATA_DIR, LEGACY_DATA_DIR, TOKEN_INITIAL_BALANCE

DB_FILE = DATA_DIR / "app.db"
BLOGGERS_FILE = DATA_DIR / "bloggers.json"
VIDEOS_FILE = DATA_DIR / "videos.json"
USERS_FILE = DATA_DIR / "users.json"

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


def normalize_token_balance(value: Any) -> int:
    if isinstance(value, bool):
        return TOKEN_INITIAL_BALANCE
    if isinstance(value, int):
        return max(0, value)
    if isinstance(value, float):
        return max(0, int(value))
    if isinstance(value, str):
        try:
            return max(0, int(value.strip()))
        except Exception:
            return TOKEN_INITIAL_BALANCE
    return TOKEN_INITIAL_BALANCE


def _read_legacy_json(file_path: Path) -> list[dict[str, Any]]:
    if not file_path.exists():
        return []
    try:
        raw = file_path.read_text(encoding="utf-8")
        parsed = json.loads(raw)
        if not isinstance(parsed, list):
            return []
        return [item for item in parsed if isinstance(item, dict)]
    except Exception:
        return []


class SQLiteDB:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._conn: sqlite3.Connection | None = None
        self._ready = False

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(str(DB_FILE), check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    def _serialize(self, payload: dict[str, Any]) -> str:
        return json.dumps(payload, ensure_ascii=False)

    def _deserialize(self, payload: str) -> dict[str, Any]:
        try:
            parsed = json.loads(payload)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    def _ensure_schema_and_migration(self) -> None:
        if self._ready:
            return

        conn = self._connect()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bloggers (
                id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS videos (
                id TEXT PRIMARY KEY,
                blogger_id TEXT,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_blogger_id ON videos(blogger_id)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                email TEXT,
                google_sub TEXT,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON users(email)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_google_sub ON users(google_sub)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS token_transactions (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                amount INTEGER NOT NULL,
                balance_after INTEGER NOT NULL,
                kind TEXT NOT NULL,
                reason TEXT NOT NULL,
                metadata TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_token_transactions_user_id ON token_transactions(user_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_token_transactions_created_at ON token_transactions(created_at)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stripe_checkouts (
                session_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                token_amount INTEGER NOT NULL,
                amount_cents INTEGER NOT NULL,
                currency TEXT NOT NULL,
                status TEXT NOT NULL,
                payment_intent TEXT,
                event_id TEXT,
                metadata TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_stripe_checkouts_user_id ON stripe_checkouts(user_id)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS generation_jobs (
                id TEXT PRIMARY KEY,
                owner_user_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                status TEXT NOT NULL,
                target_id TEXT,
                target_type TEXT,
                external_task_id TEXT,
                payload TEXT NOT NULL,
                error TEXT,
                result_url TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_generation_jobs_owner_user_id ON generation_jobs(owner_user_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_generation_jobs_status_created_at ON generation_jobs(status, created_at)"
        )

        self._migrate_from_legacy_json(conn)
        conn.commit()
        self._ready = True

    def _table_is_empty(self, conn: sqlite3.Connection, table: str) -> bool:
        row = conn.execute(f"SELECT COUNT(1) AS cnt FROM {table}").fetchone()
        return int(row["cnt"] if row and row["cnt"] is not None else 0) == 0

    def _pick_legacy_file(self, primary: Path, secondary: Path) -> Path | None:
        if primary.exists():
            return primary
        if secondary.exists():
            return secondary
        return None

    def _migrate_from_legacy_json(self, conn: sqlite3.Connection) -> None:
        bloggers_legacy = self._pick_legacy_file(BLOGGERS_FILE, LEGACY_DATA_DIR / "bloggers.json")
        videos_legacy = self._pick_legacy_file(VIDEOS_FILE, LEGACY_DATA_DIR / "videos.json")
        users_legacy = self._pick_legacy_file(USERS_FILE, LEGACY_DATA_DIR / "users.json")

        if self._table_is_empty(conn, "bloggers") and bloggers_legacy:
            for item in _read_legacy_json(bloggers_legacy):
                blogger = normalize_blogger_record(item)
                blogger_id = str(blogger.get("id") or nanoid())
                blogger = {**blogger, "id": blogger_id}
                created_at = str(blogger.get("createdAt") or now_iso())
                conn.execute(
                    "INSERT OR REPLACE INTO bloggers (id, payload, created_at) VALUES (?, ?, ?)",
                    (blogger_id, self._serialize(blogger), created_at),
                )

        if self._table_is_empty(conn, "videos") and videos_legacy:
            for item in _read_legacy_json(videos_legacy):
                video_id = str(item.get("id") or nanoid())
                video = {**item, "id": video_id}
                created_at = str(video.get("createdAt") or now_iso())
                blogger_id = str(video.get("bloggerId") or "") or None
                conn.execute(
                    "INSERT OR REPLACE INTO videos (id, blogger_id, payload, created_at) VALUES (?, ?, ?, ?)",
                    (video_id, blogger_id, self._serialize(video), created_at),
                )

        if self._table_is_empty(conn, "users") and users_legacy:
            for item in _read_legacy_json(users_legacy):
                user_id = str(item.get("id") or nanoid())
                user = {**item, "id": user_id}
                created_at = str(user.get("createdAt") or now_iso())
                email = str(user.get("email") or "").strip().lower() or None
                google_sub = str(user.get("googleSub") or "").strip() or None
                conn.execute(
                    "INSERT OR REPLACE INTO users (id, email, google_sub, payload, created_at) VALUES (?, ?, ?, ?, ?)",
                    (user_id, email, google_sub, self._serialize(user), created_at),
                )

    def _row_payload(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return self._deserialize(str(row["payload"]))

    def get_all_bloggers(self) -> list[dict[str, Any]]:
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            rows = conn.execute("SELECT payload FROM bloggers ORDER BY created_at ASC").fetchall()
            return [normalize_blogger_record(self._deserialize(str(row["payload"]))) for row in rows]

    def get_blogger_by_id(self, blogger_id: str) -> dict[str, Any] | None:
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            row = conn.execute("SELECT payload FROM bloggers WHERE id = ?", (blogger_id,)).fetchone()
            payload = self._row_payload(row)
            return normalize_blogger_record(payload) if isinstance(payload, dict) else None

    def create_blogger(self, data: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            blogger = normalize_blogger_record(
                {
                    **data,
                    "id": nanoid(),
                    "createdAt": now_iso(),
                }
            )
            conn.execute(
                "INSERT INTO bloggers (id, payload, created_at) VALUES (?, ?, ?)",
                (str(blogger["id"]), self._serialize(blogger), str(blogger["createdAt"])),
            )
            conn.commit()
            return blogger

    def update_blogger(self, blogger_id: str, data: dict[str, Any]) -> dict[str, Any] | None:
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            row = conn.execute("SELECT payload FROM bloggers WHERE id = ?", (blogger_id,)).fetchone()
            current = self._row_payload(row)
            if not isinstance(current, dict):
                return None

            updated = normalize_blogger_record({**current, **data, "id": blogger_id})
            created_at = str(updated.get("createdAt") or current.get("createdAt") or now_iso())
            conn.execute(
                "UPDATE bloggers SET payload = ?, created_at = ? WHERE id = ?",
                (self._serialize(updated), created_at, blogger_id),
            )
            conn.commit()
            return updated

    def delete_blogger(self, blogger_id: str) -> bool:
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            cur = conn.execute("DELETE FROM bloggers WHERE id = ?", (blogger_id,))
            conn.commit()
            return cur.rowcount > 0

    def get_all_videos(self) -> list[dict[str, Any]]:
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            rows = conn.execute("SELECT payload FROM videos ORDER BY created_at ASC").fetchall()
            return [self._deserialize(str(row["payload"])) for row in rows]

    def get_video_by_id(self, video_id: str) -> dict[str, Any] | None:
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            row = conn.execute("SELECT payload FROM videos WHERE id = ?", (video_id,)).fetchone()
            return self._row_payload(row)

    def create_video(self, data: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            now = now_iso()
            video = {
                **data,
                "id": nanoid(),
                "createdAt": now,
                "updatedAt": now,
            }
            conn.execute(
                "INSERT INTO videos (id, blogger_id, payload, created_at) VALUES (?, ?, ?, ?)",
                (
                    str(video["id"]),
                    str(video.get("bloggerId") or "") or None,
                    self._serialize(video),
                    str(video["createdAt"]),
                ),
            )
            conn.commit()
            return video

    def update_video(self, video_id: str, data: dict[str, Any]) -> dict[str, Any] | None:
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            row = conn.execute("SELECT payload FROM videos WHERE id = ?", (video_id,)).fetchone()
            current = self._row_payload(row)
            if not isinstance(current, dict):
                return None

            updated = {
                **current,
                **data,
                "id": video_id,
                "updatedAt": now_iso(),
            }
            created_at = str(updated.get("createdAt") or current.get("createdAt") or now_iso())
            conn.execute(
                "UPDATE videos SET blogger_id = ?, payload = ?, created_at = ? WHERE id = ?",
                (
                    str(updated.get("bloggerId") or "") or None,
                    self._serialize(updated),
                    created_at,
                    video_id,
                ),
            )
            conn.commit()
            return updated

    def delete_video(self, video_id: str) -> bool:
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            cur = conn.execute("DELETE FROM videos WHERE id = ?", (video_id,))
            conn.commit()
            return cur.rowcount > 0

    def delete_videos_by_blogger_id(self, blogger_id: str) -> int:
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            cur = conn.execute("DELETE FROM videos WHERE blogger_id = ?", (blogger_id,))
            conn.commit()
            return cur.rowcount

    def get_all_users(self) -> list[dict[str, Any]]:
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            rows = conn.execute("SELECT payload FROM users ORDER BY created_at ASC").fetchall()
            return [self._deserialize(str(row["payload"])) for row in rows]

    def get_user_by_id(self, user_id: str) -> dict[str, Any] | None:
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            row = conn.execute("SELECT payload FROM users WHERE id = ?", (user_id,)).fetchone()
            return self._row_payload(row)

    def get_user_by_email(self, email: str) -> dict[str, Any] | None:
        normalized_email = str(email).strip().lower()
        if not normalized_email:
            return None
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            row = conn.execute("SELECT payload FROM users WHERE email = ?", (normalized_email,)).fetchone()
            return self._row_payload(row)

    def get_user_by_google_sub(self, google_sub: str) -> dict[str, Any] | None:
        normalized_sub = str(google_sub).strip()
        if not normalized_sub:
            return None
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            row = conn.execute("SELECT payload FROM users WHERE google_sub = ?", (normalized_sub,)).fetchone()
            return self._row_payload(row)

    def create_user(self, data: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            now = now_iso()
            user = {
                **data,
                "id": nanoid(),
                "createdAt": now,
                "updatedAt": now,
            }
            if "tokenBalance" not in user:
                user["tokenBalance"] = TOKEN_INITIAL_BALANCE
            user["tokenBalance"] = normalize_token_balance(user.get("tokenBalance"))
            email = str(user.get("email") or "").strip().lower() or None
            google_sub = str(user.get("googleSub") or "").strip() or None
            conn.execute(
                "INSERT INTO users (id, email, google_sub, payload, created_at) VALUES (?, ?, ?, ?, ?)",
                (str(user["id"]), email, google_sub, self._serialize(user), str(user["createdAt"])),
            )
            conn.commit()
            return user

    def update_user(self, user_id: str, data: dict[str, Any]) -> dict[str, Any] | None:
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            row = conn.execute("SELECT payload FROM users WHERE id = ?", (user_id,)).fetchone()
            current = self._row_payload(row)
            if not isinstance(current, dict):
                return None

            updated = {
                **current,
                **data,
                "id": user_id,
                "updatedAt": now_iso(),
            }
            updated["tokenBalance"] = normalize_token_balance(updated.get("tokenBalance"))
            created_at = str(updated.get("createdAt") or current.get("createdAt") or now_iso())
            email = str(updated.get("email") or "").strip().lower() or None
            google_sub = str(updated.get("googleSub") or "").strip() or None
            conn.execute(
                "UPDATE users SET email = ?, google_sub = ?, payload = ?, created_at = ? WHERE id = ?",
                (email, google_sub, self._serialize(updated), created_at, user_id),
            )
            conn.commit()
            return updated

    def ensure_user_token_balance(self, user_id: str) -> int | None:
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            row = conn.execute("SELECT payload FROM users WHERE id = ?", (user_id,)).fetchone()
            current = self._row_payload(row)
            if not isinstance(current, dict):
                return None

            balance = normalize_token_balance(current.get("tokenBalance"))
            if current.get("tokenBalance") != balance:
                updated = {
                    **current,
                    "id": user_id,
                    "tokenBalance": balance,
                    "updatedAt": now_iso(),
                }
                email = str(updated.get("email") or "").strip().lower() or None
                google_sub = str(updated.get("googleSub") or "").strip() or None
                created_at = str(updated.get("createdAt") or current.get("createdAt") or now_iso())
                conn.execute(
                    "UPDATE users SET email = ?, google_sub = ?, payload = ?, created_at = ? WHERE id = ?",
                    (email, google_sub, self._serialize(updated), created_at, user_id),
                )
                conn.commit()
            return balance

    def list_token_transactions(self, user_id: str, limit: int = 50) -> list[dict[str, Any]]:
        normalized_limit = max(1, min(int(limit), 500))
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            rows = conn.execute(
                """
                SELECT id, user_id, amount, balance_after, kind, reason, metadata, created_at
                FROM token_transactions
                WHERE user_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (user_id, normalized_limit),
            ).fetchall()

            result: list[dict[str, Any]] = []
            for row in rows:
                metadata = self._deserialize(str(row["metadata"])) if row["metadata"] else {}
                result.append(
                    {
                        "id": str(row["id"]),
                        "userId": str(row["user_id"]),
                        "amount": int(row["amount"] or 0),
                        "balanceAfter": int(row["balance_after"] or 0),
                        "kind": str(row["kind"] or ""),
                        "reason": str(row["reason"] or ""),
                        "metadata": metadata,
                        "createdAt": str(row["created_at"] or ""),
                    }
                )
            return result

    def create_or_update_checkout_session(
        self,
        *,
        session_id: str,
        user_id: str,
        token_amount: int,
        amount_cents: int,
        currency: str,
        status: str = "pending",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            now = now_iso()
            conn.execute(
                """
                INSERT INTO stripe_checkouts (
                    session_id,
                    user_id,
                    token_amount,
                    amount_cents,
                    currency,
                    status,
                    metadata,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    user_id = excluded.user_id,
                    token_amount = excluded.token_amount,
                    amount_cents = excluded.amount_cents,
                    currency = excluded.currency,
                    status = excluded.status,
                    metadata = excluded.metadata,
                    updated_at = excluded.updated_at
                """,
                (
                    session_id,
                    user_id,
                    int(token_amount),
                    int(amount_cents),
                    str(currency).lower(),
                    status,
                    self._serialize(metadata or {}),
                    now,
                    now,
                ),
            )
            conn.commit()
            return {
                "sessionId": session_id,
                "userId": user_id,
                "tokenAmount": int(token_amount),
                "amountCents": int(amount_cents),
                "currency": str(currency).lower(),
                "status": status,
                "updatedAt": now,
            }

    def spend_user_tokens(
        self,
        *,
        user_id: str,
        amount: int,
        reason: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_amount = int(amount)
        if normalized_amount <= 0:
            return {
                "success": False,
                "error": "invalid_amount",
            }

        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            row = conn.execute("SELECT payload FROM users WHERE id = ?", (user_id,)).fetchone()
            current = self._row_payload(row)
            if not isinstance(current, dict):
                return {"success": False, "error": "user_not_found"}

            current_balance = normalize_token_balance(current.get("tokenBalance"))
            if current_balance < normalized_amount:
                return {
                    "success": False,
                    "error": "insufficient_tokens",
                    "balance": current_balance,
                }

            next_balance = current_balance - normalized_amount
            updated = {
                **current,
                "id": user_id,
                "tokenBalance": next_balance,
                "updatedAt": now_iso(),
            }
            email = str(updated.get("email") or "").strip().lower() or None
            google_sub = str(updated.get("googleSub") or "").strip() or None
            created_at = str(updated.get("createdAt") or current.get("createdAt") or now_iso())

            conn.execute(
                "UPDATE users SET email = ?, google_sub = ?, payload = ?, created_at = ? WHERE id = ?",
                (email, google_sub, self._serialize(updated), created_at, user_id),
            )

            transaction_id = nanoid()
            tx_created = now_iso()
            conn.execute(
                """
                INSERT INTO token_transactions (
                    id,
                    user_id,
                    amount,
                    balance_after,
                    kind,
                    reason,
                    metadata,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    transaction_id,
                    user_id,
                    -normalized_amount,
                    next_balance,
                    "debit",
                    reason,
                    self._serialize(metadata or {}),
                    tx_created,
                ),
            )
            conn.commit()
            return {
                "success": True,
                "balance": next_balance,
                "transactionId": transaction_id,
                "amount": -normalized_amount,
            }

    def credit_user_tokens(
        self,
        *,
        user_id: str,
        amount: int,
        reason: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_amount = int(amount)
        if normalized_amount <= 0:
            return {
                "success": False,
                "error": "invalid_amount",
            }

        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            row = conn.execute("SELECT payload FROM users WHERE id = ?", (user_id,)).fetchone()
            current = self._row_payload(row)
            if not isinstance(current, dict):
                return {"success": False, "error": "user_not_found"}

            current_balance = normalize_token_balance(current.get("tokenBalance"))
            next_balance = current_balance + normalized_amount
            updated = {
                **current,
                "id": user_id,
                "tokenBalance": next_balance,
                "updatedAt": now_iso(),
            }
            email = str(updated.get("email") or "").strip().lower() or None
            google_sub = str(updated.get("googleSub") or "").strip() or None
            created_at = str(updated.get("createdAt") or current.get("createdAt") or now_iso())
            conn.execute(
                "UPDATE users SET email = ?, google_sub = ?, payload = ?, created_at = ? WHERE id = ?",
                (email, google_sub, self._serialize(updated), created_at, user_id),
            )

            transaction_id = nanoid()
            tx_created = now_iso()
            conn.execute(
                """
                INSERT INTO token_transactions (
                    id,
                    user_id,
                    amount,
                    balance_after,
                    kind,
                    reason,
                    metadata,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    transaction_id,
                    user_id,
                    normalized_amount,
                    next_balance,
                    "credit",
                    reason,
                    self._serialize(metadata or {}),
                    tx_created,
                ),
            )
            conn.commit()
            return {
                "success": True,
                "balance": next_balance,
                "transactionId": transaction_id,
                "amount": normalized_amount,
            }

    def apply_paid_checkout(
        self,
        *,
        session_id: str,
        user_id: str,
        token_amount: int,
        amount_cents: int,
        currency: str,
        payment_intent: str | None = None,
        event_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_tokens = int(token_amount)
        if normalized_tokens <= 0:
            return {"applied": False, "error": "invalid_token_amount"}

        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()

            row = conn.execute(
                "SELECT session_id, status FROM stripe_checkouts WHERE session_id = ?",
                (session_id,),
            ).fetchone()
            if row is not None and str(row["status"] or "") == "paid":
                return {"applied": False, "status": "already_paid"}

            user_row = conn.execute("SELECT payload FROM users WHERE id = ?", (user_id,)).fetchone()
            current_user = self._row_payload(user_row)
            if not isinstance(current_user, dict):
                return {"applied": False, "error": "user_not_found"}

            current_balance = normalize_token_balance(current_user.get("tokenBalance"))
            next_balance = current_balance + normalized_tokens
            updated_user = {
                **current_user,
                "id": user_id,
                "tokenBalance": next_balance,
                "updatedAt": now_iso(),
            }
            user_email = str(updated_user.get("email") or "").strip().lower() or None
            user_google_sub = str(updated_user.get("googleSub") or "").strip() or None
            user_created_at = str(
                updated_user.get("createdAt") or current_user.get("createdAt") or now_iso()
            )
            conn.execute(
                "UPDATE users SET email = ?, google_sub = ?, payload = ?, created_at = ? WHERE id = ?",
                (
                    user_email,
                    user_google_sub,
                    self._serialize(updated_user),
                    user_created_at,
                    user_id,
                ),
            )

            now = now_iso()
            conn.execute(
                """
                INSERT INTO stripe_checkouts (
                    session_id,
                    user_id,
                    token_amount,
                    amount_cents,
                    currency,
                    status,
                    payment_intent,
                    event_id,
                    metadata,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    user_id = excluded.user_id,
                    token_amount = excluded.token_amount,
                    amount_cents = excluded.amount_cents,
                    currency = excluded.currency,
                    status = excluded.status,
                    payment_intent = excluded.payment_intent,
                    event_id = excluded.event_id,
                    metadata = excluded.metadata,
                    updated_at = excluded.updated_at
                """,
                (
                    session_id,
                    user_id,
                    normalized_tokens,
                    int(amount_cents),
                    str(currency).lower(),
                    "paid",
                    payment_intent or None,
                    event_id or None,
                    self._serialize(metadata or {}),
                    now,
                    now,
                ),
            )

            transaction_id = nanoid()
            conn.execute(
                """
                INSERT INTO token_transactions (
                    id,
                    user_id,
                    amount,
                    balance_after,
                    kind,
                    reason,
                    metadata,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    transaction_id,
                    user_id,
                    normalized_tokens,
                    next_balance,
                    "credit",
                    "stripe_topup",
                    self._serialize(
                        {
                            "sessionId": session_id,
                            "amountCents": int(amount_cents),
                            "currency": str(currency).lower(),
                            "paymentIntent": payment_intent or "",
                            "eventId": event_id or "",
                            **(metadata or {}),
                        }
                    ),
                    now,
                ),
            )
            conn.commit()
            return {
                "applied": True,
                "balance": next_balance,
                "credited": normalized_tokens,
                "transactionId": transaction_id,
            }

    def create_generation_job(self, data: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            now = now_iso()
            job = {
                **data,
                "id": nanoid(),
                "status": str(data.get("status") or "queued"),
                "createdAt": now,
                "updatedAt": now,
            }
            conn.execute(
                """
                INSERT INTO generation_jobs (
                    id,
                    owner_user_id,
                    kind,
                    status,
                    target_id,
                    target_type,
                    external_task_id,
                    payload,
                    error,
                    result_url,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(job["id"]),
                    str(job.get("ownerUserId") or ""),
                    str(job.get("kind") or ""),
                    str(job.get("status") or "queued"),
                    str(job.get("targetId") or "") or None,
                    str(job.get("targetType") or "") or None,
                    str(job.get("externalTaskId") or "") or None,
                    self._serialize(job.get("payload") if isinstance(job.get("payload"), dict) else {}),
                    str(job.get("error") or "") or None,
                    str(job.get("resultUrl") or "") or None,
                    str(job["createdAt"]),
                ),
            )
            conn.commit()
            return job

    def get_generation_job_by_id(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            row = conn.execute(
                """
                SELECT
                    id,
                    owner_user_id,
                    kind,
                    status,
                    target_id,
                    target_type,
                    external_task_id,
                    payload,
                    error,
                    result_url,
                    created_at
                FROM generation_jobs
                WHERE id = ?
                """,
                (job_id,),
            ).fetchone()
            if row is None:
                return None
            payload = self._deserialize(str(row["payload"] or "{}"))
            created_at = str(row["created_at"] or now_iso())
            return {
                "id": str(row["id"] or ""),
                "ownerUserId": str(row["owner_user_id"] or ""),
                "kind": str(row["kind"] or ""),
                "status": str(row["status"] or ""),
                "targetId": str(row["target_id"] or ""),
                "targetType": str(row["target_type"] or ""),
                "externalTaskId": str(row["external_task_id"] or ""),
                "payload": payload,
                "error": str(row["error"] or ""),
                "resultUrl": str(row["result_url"] or ""),
                "createdAt": created_at,
                "updatedAt": str(payload.get("updatedAt") or created_at),
            }

    def list_generation_jobs(
        self,
        *,
        statuses: list[str] | None = None,
        owner_user_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        normalized_limit = max(1, min(int(limit), 1000))
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()

            where_clauses: list[str] = []
            params: list[Any] = []

            if statuses:
                normalized_statuses = [str(item).strip() for item in statuses if str(item).strip()]
                if normalized_statuses:
                    placeholders = ", ".join(["?"] * len(normalized_statuses))
                    where_clauses.append(f"status IN ({placeholders})")
                    params.extend(normalized_statuses)

            if owner_user_id and owner_user_id.strip():
                where_clauses.append("owner_user_id = ?")
                params.append(owner_user_id.strip())

            where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            rows = conn.execute(
                f"""
                SELECT
                    id,
                    owner_user_id,
                    kind,
                    status,
                    target_id,
                    target_type,
                    external_task_id,
                    payload,
                    error,
                    result_url,
                    created_at
                FROM generation_jobs
                {where_sql}
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (*params, normalized_limit),
            ).fetchall()

            result: list[dict[str, Any]] = []
            for row in rows:
                payload = self._deserialize(str(row["payload"] or "{}"))
                created_at = str(row["created_at"] or now_iso())
                result.append(
                    {
                        "id": str(row["id"] or ""),
                        "ownerUserId": str(row["owner_user_id"] or ""),
                        "kind": str(row["kind"] or ""),
                        "status": str(row["status"] or ""),
                        "targetId": str(row["target_id"] or ""),
                        "targetType": str(row["target_type"] or ""),
                        "externalTaskId": str(row["external_task_id"] or ""),
                        "payload": payload,
                        "error": str(row["error"] or ""),
                        "resultUrl": str(row["result_url"] or ""),
                        "createdAt": created_at,
                        "updatedAt": str(payload.get("updatedAt") or created_at),
                    }
                )
            return result

    def update_generation_job(self, job_id: str, data: dict[str, Any]) -> dict[str, Any] | None:
        with self._lock:
            self._ensure_schema_and_migration()
            conn = self._connect()
            row = conn.execute(
                """
                SELECT
                    id,
                    owner_user_id,
                    kind,
                    status,
                    target_id,
                    target_type,
                    external_task_id,
                    payload,
                    error,
                    result_url,
                    created_at
                FROM generation_jobs
                WHERE id = ?
                """,
                (job_id,),
            ).fetchone()
            if row is None:
                return None

            current_payload = self._deserialize(str(row["payload"] or "{}"))
            next_payload = (
                data["payload"]
                if isinstance(data.get("payload"), dict)
                else current_payload
            )
            if not isinstance(next_payload, dict):
                next_payload = {}
            next_payload = {
                **next_payload,
                "updatedAt": now_iso(),
            }

            next_status = str(data.get("status") or row["status"] or "queued")
            next_target_id = (
                str(data.get("targetId"))
                if "targetId" in data
                else str(row["target_id"] or "")
            )
            next_target_type = (
                str(data.get("targetType"))
                if "targetType" in data
                else str(row["target_type"] or "")
            )
            next_external_task_id = (
                str(data.get("externalTaskId"))
                if "externalTaskId" in data
                else str(row["external_task_id"] or "")
            )
            next_error = (
                str(data.get("error"))
                if "error" in data
                else str(row["error"] or "")
            )
            next_result_url = (
                str(data.get("resultUrl"))
                if "resultUrl" in data
                else str(row["result_url"] or "")
            )

            conn.execute(
                """
                UPDATE generation_jobs
                SET
                    status = ?,
                    target_id = ?,
                    target_type = ?,
                    external_task_id = ?,
                    payload = ?,
                    error = ?,
                    result_url = ?
                WHERE id = ?
                """,
                (
                    next_status,
                    next_target_id or None,
                    next_target_type or None,
                    next_external_task_id or None,
                    self._serialize(next_payload),
                    next_error or None,
                    next_result_url or None,
                    job_id,
                ),
            )
            conn.commit()
            return {
                "id": str(row["id"] or ""),
                "ownerUserId": str(row["owner_user_id"] or ""),
                "kind": str(row["kind"] or ""),
                "status": next_status,
                "targetId": next_target_id,
                "targetType": next_target_type,
                "externalTaskId": next_external_task_id,
                "payload": next_payload,
                "error": next_error,
                "resultUrl": next_result_url,
                "createdAt": str(row["created_at"] or now_iso()),
                "updatedAt": str(next_payload.get("updatedAt") or now_iso()),
            }


db = SQLiteDB()
