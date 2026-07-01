"""
SHAFRAN — бэкенд.
Python 3.11+, FastAPI, SQLite (WAL), WebSocket для мгновенной синхронизации таймера.

Важно про честность очереди номеров:
- Выдача номера идёт ТОЛЬКО через POST /api/claim.
- Внутри claim() стоит asyncio.Lock — пока один запрос не завершится
  (не запишется в БД), следующий не начнёт выполняться. Это гарантирует,
  что даже если 50 человек нажмут кнопку одновременно, номера уйдут
  строго по одному, без повторов.
- Дополнительно поле queue_number в таблице users помечено UNIQUE —
  это второй, "аппаратный" уровень защиты на случай программной ошибки.
- Запускать процесс нужно ОДНИМ воркером (uvicorn ... --workers 1).
  Один воркер = один Lock на всех = гарантия отсутствия дублей.
  Render Web Service по умолчанию именно так и работает.
"""

import asyncio
import os
import secrets
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

DB_PATH = os.environ.get("SHAFRAN_DB", "shafran.db")
ADMIN_KEY = os.environ.get("SHAFRAN_ADMIN_KEY", "shafran-admin-2026")

app = FastAPI(title="SHAFRAN")

claim_lock = asyncio.Lock()
ws_clients: set[WebSocket] = set()


# ---------------------------------------------------------------- database

@contextmanager
def db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone TEXT UNIQUE NOT NULL,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                token TEXT UNIQUE NOT NULL,
                queue_number INTEGER UNIQUE,
                claimed_at TEXT,
                created_at TEXT NOT NULL
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS event_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                timer_end TEXT,
                timer_running INTEGER NOT NULL DEFAULT 0,
                next_number INTEGER NOT NULL DEFAULT 1
            );
        """)
        conn.execute("""
            INSERT OR IGNORE INTO event_state (id, timer_end, timer_running, next_number)
            VALUES (1, NULL, 0, 1);
        """)


init_db()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_state_row(conn) -> sqlite3.Row:
    return conn.execute("SELECT * FROM event_state WHERE id = 1").fetchone()


def queue_is_open(state_row) -> bool:
    if not state_row["timer_running"] or not state_row["timer_end"]:
        return False
    end = datetime.fromisoformat(state_row["timer_end"])
    return datetime.now(timezone.utc) >= end


# ------------------------------------------------------------------ models

class RegisterPayload(BaseModel):
    phone: str
    first_name: str
    last_name: str


class ClaimPayload(BaseModel):
    token: str


class AdminTimerPayload(BaseModel):
    admin_key: str
    seconds: int


class AdminResetPayload(BaseModel):
    admin_key: str
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
            # мы не ждём сообщений от клиента, просто держим соединение живым
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        ws_clients.discard(websocket)


# ------------------------------------------------------------------- api

@app.get("/api/state")
def api_state():
    with db() as conn:
        s = get_state_row(conn)
        claimed = conn.execute(
            "SELECT COUNT(*) c FROM users WHERE queue_number IS NOT NULL"
        ).fetchone()["c"]
        return {
            "timer_end": s["timer_end"],
            "timer_running": bool(s["timer_running"]),
            "queue_open": queue_is_open(s),
            "claimed_count": claimed,
            "server_time": now_iso(),
        }


@app.post("/api/register")
def api_register(payload: RegisterPayload):
    phone = payload.phone.strip()
    first_name = payload.first_name.strip()
    last_name = payload.last_name.strip()
    if not phone or not first_name or not last_name:
        raise HTTPException(400, "Заполните телефон, имя и фамилию")

    with db() as conn:
        existing = conn.execute(
            "SELECT * FROM users WHERE phone = ?", (phone,)
        ).fetchone()
        if existing:
            return {
                "token": existing["token"],
                "first_name": existing["first_name"],
                "last_name": existing["last_name"],
                "queue_number": existing["queue_number"],
            }

        token = secrets.token_urlsafe(24)
        conn.execute(
            """INSERT INTO users (phone, first_name, last_name, token, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (phone, first_name, last_name, token, now_iso()),
        )
        return {
            "token": token,
            "first_name": first_name,
            "last_name": last_name,
            "queue_number": None,
        }


@app.get("/api/me")
def api_me(token: str):
    with db() as conn:
        u = conn.execute("SELECT * FROM users WHERE token = ?", (token,)).fetchone()
        if not u:
            raise HTTPException(404, "Пользователь не найден")
        return {
            "first_name": u["first_name"],
            "last_name": u["last_name"],
            "queue_number": u["queue_number"],
        }


@app.post("/api/claim")
async def api_claim(payload: ClaimPayload):
    async with claim_lock:
        with db() as conn:
            u = conn.execute(
                "SELECT * FROM users WHERE token = ?", (payload.token,)
            ).fetchone()
            if not u:
                raise HTTPException(404, "Пользователь не найден")

            if u["queue_number"] is not None:
                # уже получал номер — просто возвращаем его же, без побочных эффектов
                return {"queue_number": u["queue_number"]}

            state = get_state_row(conn)
            if not queue_is_open(state):
                raise HTTPException(400, "Выдача номеров ещё не началась")

            number = state["next_number"]
            conn.execute(
                "UPDATE users SET queue_number = ?, claimed_at = ? WHERE id = ?",
                (number, now_iso(), u["id"]),
            )
            conn.execute(
                "UPDATE event_state SET next_number = ? WHERE id = 1", (number + 1,)
            )
            claimed = conn.execute(
                "SELECT COUNT(*) c FROM users WHERE queue_number IS NOT NULL"
            ).fetchone()["c"]

        await broadcast({"type": "claimed_count", "claimed_count": claimed})
        return {"queue_number": number}


# ------------------------------------------------------------- admin api

def check_admin(key: str):
    if not secrets.compare_digest(key, ADMIN_KEY):
        raise HTTPException(403, "Неверный ключ администратора")


@app.post("/api/admin/timer")
async def admin_start_timer(payload: AdminTimerPayload):
    check_admin(payload.admin_key)
    if payload.seconds <= 0:
        raise HTTPException(400, "seconds должно быть больше нуля")

    end = datetime.now(timezone.utc).timestamp() + payload.seconds
    end_iso = datetime.fromtimestamp(end, tz=timezone.utc).isoformat()

    with db() as conn:
        conn.execute(
            "UPDATE event_state SET timer_end = ?, timer_running = 1 WHERE id = 1",
            (end_iso,),
        )

    await broadcast({"type": "timer_start", "timer_end": end_iso})
    return {"timer_end": end_iso}


@app.post("/api/admin/reset")
async def admin_reset(payload: AdminResetPayload):
    check_admin(payload.admin_key)
    with db() as conn:
        conn.execute(
            "UPDATE event_state SET timer_end = NULL, timer_running = 0, next_number = 1 WHERE id = 1"
        )
        if payload.wipe_users:
            conn.execute("DELETE FROM users")
        else:
            conn.execute("UPDATE users SET queue_number = NULL, claimed_at = NULL")

    await broadcast({"type": "reset"})
    return {"ok": True}


@app.get("/api/admin/stats")
def admin_stats(admin_key: str):
    check_admin(admin_key)
    with db() as conn:
        s = get_state_row(conn)
        users = conn.execute(
            "SELECT phone, first_name, last_name, queue_number, claimed_at FROM users "
            "ORDER BY queue_number IS NULL, queue_number ASC"
        ).fetchall()
        return {
            "timer_end": s["timer_end"],
            "timer_running": bool(s["timer_running"]),
            "next_number": s["next_number"],
            "users": [dict(u) for u in users],
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
