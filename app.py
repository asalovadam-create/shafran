"""
SHAFRAN — бэкенд.
Python 3.11+, FastAPI, PostgreSQL (Neon) через asyncpg, WebSocket для
мгновенной синхронизации таймера.

Единая система входа (без отдельных "регистрация"/"вход" разделов):
  1. Человек вводит логин (номер телефона).
  2. POST /api/check-login — сервер смотрит, есть ли такой логин в базе.
  3. Если есть — просим только пароль → POST /api/login.
     Если нет — просим имя и пароль → POST /api/register.

Админ — обычный пользователь с флагом is_admin=true в той же таблице.
Управление таймером (/api/admin/*) теперь защищено токеном админа,
а не отдельным статическим ключом — то есть система входа действительно одна.

Честность очереди номеров:
- Выдача номера идёт ТОЛЬКО через POST /api/claim.
- Внутри claim() стоит asyncio.Lock — пока один запрос не завершится
  (не запишется в БД), следующий не начнёт выполняться. Поэтому даже
  если 50 человек нажмут кнопку одновременно, номера уйдут строго по
  одному, без повторов.
- Запускать процесс нужно ОДНИМ воркером (uvicorn ... --workers 1).
"""

import asyncio
import hashlib
import os
import secrets
from datetime import datetime, timezone
from typing import Optional

import asyncpg
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from urllib.parse import urlparse

DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    raise RuntimeError(
        "Не задана переменная окружения DATABASE_URL "
        "(строка подключения к PostgreSQL / Neon)."
    )

# asyncpg не понимает параметр channel_binding из строки подключения Neon —
# убираем query-параметры и подключаемся с ssl отдельно.
_DB_DSN = DATABASE_URL.split("?", 1)[0]
_parsed_host = urlparse(DATABASE_URL).hostname or ""
_USE_SSL = _parsed_host not in ("localhost", "127.0.0.1")

ADMIN_SEED_LOGIN = os.environ.get("SHAFRAN_ADMIN_LOGIN", "admin")
ADMIN_SEED_PASSWORD = os.environ.get("SHAFRAN_ADMIN_PASSWORD", "123")
ADMIN_SEED_NAME = "Администратор"

app = FastAPI(title="SHAFRAN")

claim_lock = asyncio.Lock()
ws_clients: set[WebSocket] = set()
pool: Optional[asyncpg.Pool] = None


# ---------------------------------------------------------------- пароли

def hash_password(password: str, salt: Optional[str] = None) -> tuple[str, str]:
    if salt is None:
        salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), bytes.fromhex(salt), 200_000
    ).hex()
    return digest, salt


def verify_password(password: str, salt: str, expected_hash: str) -> bool:
    digest, _ = hash_password(password, salt)
    return secrets.compare_digest(digest, expected_hash)


# ---------------------------------------------------------------- database

async def get_pool() -> asyncpg.Pool:
    global pool
    if pool is None:
        pool = await asyncpg.create_pool(dsn=_DB_DSN, ssl=_USE_SSL, min_size=1, max_size=8)
    return pool


async def init_db():
    p = await get_pool()
    async with p.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                login TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                password_salt TEXT NOT NULL,
                token TEXT UNIQUE NOT NULL,
                is_admin BOOLEAN NOT NULL DEFAULT FALSE,
                queue_number INTEGER UNIQUE,
                claimed_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS event_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                timer_end TIMESTAMPTZ,
                timer_running BOOLEAN NOT NULL DEFAULT FALSE,
                next_number INTEGER NOT NULL DEFAULT 1
            );
        """)
        await conn.execute("""
            INSERT INTO event_state (id, timer_end, timer_running, next_number)
            VALUES (1, NULL, FALSE, 1)
            ON CONFLICT (id) DO NOTHING;
        """)

        # сеем админ-аккаунт, если его ещё нет
        existing_admin = await conn.fetchrow(
            "SELECT id FROM users WHERE login = $1", ADMIN_SEED_LOGIN
        )
        if not existing_admin:
            digest, salt = hash_password(ADMIN_SEED_PASSWORD)
            await conn.execute(
                """INSERT INTO users (login, name, password_hash, password_salt, token, is_admin)
                   VALUES ($1, $2, $3, $4, $5, TRUE)""",
                ADMIN_SEED_LOGIN, ADMIN_SEED_NAME, digest, salt, secrets.token_urlsafe(24),
            )


@app.on_event("startup")
async def on_startup():
    await init_db()


@app.on_event("shutdown")
async def on_shutdown():
    global pool
    if pool is not None:
        await pool.close()


def queue_is_open(state_row) -> bool:
    if not state_row["timer_running"] or not state_row["timer_end"]:
        return False
    return datetime.now(timezone.utc) >= state_row["timer_end"]


# ------------------------------------------------------------------ models

class CheckLoginPayload(BaseModel):
    login: str


class RegisterPayload(BaseModel):
    login: str
    name: str
    password: str


class LoginPayload(BaseModel):
    login: str
    password: str


class ClaimPayload(BaseModel):
    token: str


class AdminTimerPayload(BaseModel):
    token: str
    seconds: int


class AdminResetPayload(BaseModel):
    token: str
    wipe_users: bool = False


# -------------------------------------------------------------------- ws

async def broadcast(message: dict):
    dead = []
    for ws in ws_clients:
        try:
            await ws.send_json(message)
        except Exception:
            dead.append(ws)
    for ws in dead:
        ws_clients.discard(ws)


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    ws_clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        ws_clients.discard(websocket)


# ------------------------------------------------------------------- api

@app.get("/api/state")
async def api_state():
    p = await get_pool()
    async with p.acquire() as conn:
        s = await conn.fetchrow("SELECT * FROM event_state WHERE id = 1")
        claimed = await conn.fetchval(
            "SELECT COUNT(*) FROM users WHERE queue_number IS NOT NULL"
        )
        return {
            "timer_end": s["timer_end"].isoformat() if s["timer_end"] else None,
            "timer_running": bool(s["timer_running"]),
            "queue_open": queue_is_open(s),
            "claimed_count": claimed,
            "server_time": datetime.now(timezone.utc).isoformat(),
        }


@app.post("/api/check-login")
async def api_check_login(payload: CheckLoginPayload):
    login = payload.login.strip()
    if not login:
        raise HTTPException(400, "Введите номер телефона")
    p = await get_pool()
    async with p.acquire() as conn:
        row = await conn.fetchrow("SELECT id FROM users WHERE login = $1", login)
        return {"exists": row is not None}


@app.post("/api/register")
async def api_register(payload: RegisterPayload):
    login = payload.login.strip()
    name = payload.name.strip()
    password = payload.password

    if not login or not name or not password:
        raise HTTPException(400, "Заполните телефон, имя и пароль")
    if len(password) < 3:
        raise HTTPException(400, "Пароль слишком короткий")

    digest, salt = hash_password(password)
    token = secrets.token_urlsafe(24)

    p = await get_pool()
    async with p.acquire() as conn:
        existing = await conn.fetchrow("SELECT id FROM users WHERE login = $1", login)
        if existing:
            raise HTTPException(409, "Такой номер уже зарегистрирован, введите пароль")

        row = await conn.fetchrow(
            """INSERT INTO users (login, name, password_hash, password_salt, token)
               VALUES ($1, $2, $3, $4, $5)
               RETURNING name, token, is_admin, queue_number""",
            login, name, digest, salt, token,
        )
        return {
            "token": row["token"],
            "name": row["name"],
            "is_admin": row["is_admin"],
            "queue_number": row["queue_number"],
        }


@app.post("/api/login")
async def api_login(payload: LoginPayload):
    login = payload.login.strip()
    password = payload.password

    p = await get_pool()
    async with p.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE login = $1", login)
        if not row or not verify_password(password, row["password_salt"], row["password_hash"]):
            raise HTTPException(401, "Неверный номер или пароль")
        return {
            "token": row["token"],
            "name": row["name"],
            "is_admin": row["is_admin"],
            "queue_number": row["queue_number"],
        }


@app.get("/api/me")
async def api_me(token: str):
    p = await get_pool()
    async with p.acquire() as conn:
        u = await conn.fetchrow("SELECT * FROM users WHERE token = $1", token)
        if not u:
            raise HTTPException(404, "Пользователь не найден")
        return {
            "name": u["name"],
            "queue_number": u["queue_number"],
            "is_admin": u["is_admin"],
        }


@app.post("/api/claim")
async def api_claim(payload: ClaimPayload):
    async with claim_lock:
        p = await get_pool()
        async with p.acquire() as conn:
            u = await conn.fetchrow("SELECT * FROM users WHERE token = $1", payload.token)
            if not u:
                raise HTTPException(404, "Пользователь не найден")

            if u["queue_number"] is not None:
                return {"queue_number": u["queue_number"]}

            state = await conn.fetchrow("SELECT * FROM event_state WHERE id = 1")
            if not queue_is_open(state):
                raise HTTPException(400, "Выдача номеров ещё не началась")

            number = state["next_number"]
            await conn.execute(
                "UPDATE users SET queue_number = $1, claimed_at = $2 WHERE id = $3",
                number, datetime.now(timezone.utc), u["id"],
            )
            await conn.execute(
                "UPDATE event_state SET next_number = $1 WHERE id = 1", number + 1
            )
            claimed = await conn.fetchval(
                "SELECT COUNT(*) FROM users WHERE queue_number IS NOT NULL"
            )

        await broadcast({"type": "claimed_count", "claimed_count": claimed})
        return {"queue_number": number}


# ------------------------------------------------------------- admin api

async def require_admin(token: str) -> None:
    p = await get_pool()
    async with p.acquire() as conn:
        u = await conn.fetchrow("SELECT is_admin FROM users WHERE token = $1", token)
        if not u or not u["is_admin"]:
            raise HTTPException(403, "Доступ только для администратора")


@app.post("/api/admin/timer")
async def admin_start_timer(payload: AdminTimerPayload):
    await require_admin(payload.token)
    if payload.seconds <= 0:
        raise HTTPException(400, "seconds должно быть больше нуля")

    end = datetime.now(timezone.utc).timestamp() + payload.seconds
    end_dt = datetime.fromtimestamp(end, tz=timezone.utc)

    p = await get_pool()
    async with p.acquire() as conn:
        await conn.execute(
            "UPDATE event_state SET timer_end = $1, timer_running = TRUE WHERE id = 1",
            end_dt,
        )

    await broadcast({"type": "timer_start", "timer_end": end_dt.isoformat()})
    return {"timer_end": end_dt.isoformat()}


@app.post("/api/admin/reset")
async def admin_reset(payload: AdminResetPayload):
    await require_admin(payload.token)
    p = await get_pool()
    async with p.acquire() as conn:
        await conn.execute(
            "UPDATE event_state SET timer_end = NULL, timer_running = FALSE, next_number = 1 WHERE id = 1"
        )
        if payload.wipe_users:
            await conn.execute("DELETE FROM users WHERE is_admin = FALSE")
        else:
            await conn.execute("UPDATE users SET queue_number = NULL, claimed_at = NULL")

    await broadcast({"type": "reset"})
    return {"ok": True}


@app.get("/api/admin/stats")
async def admin_stats(token: str):
    await require_admin(token)
    p = await get_pool()
    async with p.acquire() as conn:
        s = await conn.fetchrow("SELECT * FROM event_state WHERE id = 1")
        users = await conn.fetch(
            """SELECT login, name, queue_number, claimed_at FROM users
               WHERE is_admin = FALSE
               ORDER BY queue_number IS NULL, queue_number ASC"""
        )
        return {
            "timer_end": s["timer_end"].isoformat() if s["timer_end"] else None,
            "timer_running": bool(s["timer_running"]),
            "next_number": s["next_number"],
            "users": [
                {
                    "login": u["login"],
                    "name": u["name"],
                    "queue_number": u["queue_number"],
                    "claimed_at": u["claimed_at"].isoformat() if u["claimed_at"] else None,
                }
                for u in users
            ],
        }


# ---------------------------------------------------------------- static

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def root():
    return FileResponse("static/index.html")


@app.get("/admin")
def admin_page():
    return FileResponse("static/admin.html")


@app.get("/manifest.json")
def manifest():
    return FileResponse("static/manifest.json")


@app.get("/sw.js")
def sw():
    return FileResponse("static/sw.js", media_type="application/javascript")
