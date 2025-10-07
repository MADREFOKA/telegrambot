"""
Telegram–Twitch Sub Bot
---------------------------------

¿Qué hace?
============
- Cuando un usuario abre el bot a través de un enlace o /start, envía un mensaje directo con un enlace de inicio de sesión de Twitch OAuth para verificar su cuenta de Twitch.
- Después de iniciar sesión, el bot vincula su ID de Telegram <-> ID de Twitch.
- El bot comprueba si están suscritos al canal de Twitch configurado y, si es así, les envía un enlace de invitación a un grupo de Telegram. Si no lo está, se lo notifica.
- Semanalmente, el bot audita el grupo y expulsa a los miembros que ya no están suscritos.
- Admite múltiples canales: un difusor (propietario/moderador del canal) puede ejecutar /setup para conceder al bot el ámbito "channel:read:subscriptions" y vincular un grupo de Telegram de destino utilizando /setgroup.

Entorno
===========
Crea un archivo `.env` con:

TELEGRAM_BOT_TOKEN=123456:ABC...
FLASK_SECRET= {code secreto (puede ser una frase)}
OAUTH_CLIENT_ID=twitch_client_id
OAUTH_CLIENT_SECRET=twitch_client_secret
BASE_URL=https://your.public.domain
TZ=Europe/Madrid
# Si usas Postgres, añade:
# DATABASE_URL=postgres://usuario:pass@host:5432/bd

Ejecutar
===
$ pip install "python-telegram-bot[job-queue,rate-limiter]==21.7" aiohttp==3.9.5 Flask==3.0.3 python-dotenv==1.0.1 aiosqlite==0.20.0 asyncpg==0.29.0 waitress==3.0.0 tzdata==2024.1
$ python bot_twitch_linker.py

Notas
=====
- El bot debe ser administrador del grupo de destino y tener derechos para invitar a usuarios y eliminar miembros.
- Twitch: añade en tu app las URLs de redirección: {BASE_URL}/twitch/callback y {BASE_URL}/twitch/setup/callback
- Telegram: desactiva el modo de privacidad si deseas capturar las actualizaciones de chat_member en los grupos.

"""
from __future__ import annotations

import asyncio
import logging
import secrets
import string
import time
from html import escape
from datetime import datetime, timedelta
from typing import Optional, Tuple

import aiohttp
import aiosqlite
import psycopg
from psycopg.rows import dict_row
from flask import Flask, request, redirect, make_response
from dotenv import load_dotenv
import os

from telegram import (
    Update,
    ChatInviteLink,
)
from telegram.constants import ChatMemberStatus
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ChatMemberHandler,
    ContextTypes,
    MessageHandler,
    filters
)

# ---------------------------
# Config & Globals
# ---------------------------
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
FLASK_SECRET = os.getenv("FLASK_SECRET", "development-secret")
OAUTH_CLIENT_ID = os.getenv("OAUTH_CLIENT_ID")
OAUTH_CLIENT_SECRET = os.getenv("OAUTH_CLIENT_SECRET")
BASE_URL = os.getenv("BASE_URL", "http://localhost:8080")
TZ = os.getenv("TZ", "Europe/Madrid")
DB_PATH = os.getenv("DB_PATH", "twitch_gate.db")
DATABASE_URL = os.getenv("DATABASE_URL")
USE_PG = bool(DATABASE_URL)

if not TELEGRAM_BOT_TOKEN:
    raise SystemExit("Missing TELEGRAM_BOT_TOKEN in env")
if not OAUTH_CLIENT_ID or not OAUTH_CLIENT_SECRET:
    raise SystemExit("Missing Twitch OAUTH_CLIENT_ID / OAUTH_CLIENT_SECRET in env")

# Twitch endpoints
TW_OAUTH_AUTH = "https://id.twitch.tv/oauth2/authorize"
TW_OAUTH_TOKEN = "https://id.twitch.tv/oauth2/token"
TW_API = "https://api.twitch.tv/helix"

# Scopes
SCOPE_SETUP = ["channel:read:subscriptions"]
SCOPE_USER = ["openid", "user:read:email"]

# Flask app (OAuth callbacks)
flask_app = Flask(__name__)
flask_app.secret_key = FLASK_SECRET

# PTB app refs (set on startup)
ptb_app: Application | None = None
ptb_loop: asyncio.AbstractEventLoop | None = None

# Capture PTB loop on startup so Flask thread can schedule tasks safely
async def _on_startup(app: Application) -> None:
    global ptb_app, ptb_loop
    ptb_app = app
    ptb_loop = asyncio.get_running_loop()
    logging.info("PTB event loop capturado y listo.")


# ---------------------------
# DB Helpers (SQLite / Postgres)
# ---------------------------
CREATE_SQL = r"""
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS telegram_users (
  telegram_id INTEGER PRIMARY KEY,
  username TEXT,
  first_name TEXT,
  last_name TEXT,
  linked_twitch_id TEXT
);

CREATE TABLE IF NOT EXISTS twitch_users (
  twitch_id TEXT PRIMARY KEY,
  login TEXT,
  display_name TEXT,
  email TEXT
);

CREATE TABLE IF NOT EXISTS broadcasters (
  broadcaster_id TEXT PRIMARY KEY,
  owner_telegram_id INTEGER NOT NULL,
  access_token TEXT NOT NULL,
  refresh_token TEXT NOT NULL,
  token_obtained_at INTEGER NOT NULL,
  token_expires_in INTEGER NOT NULL,
  group_id INTEGER,
  invite_link TEXT
);

CREATE TABLE IF NOT EXISTS oauth_states (
  state TEXT PRIMARY KEY,
  telegram_id INTEGER NOT NULL,
  purpose TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS links (
  telegram_id INTEGER NOT NULL,
  twitch_id TEXT NOT NULL,
  broadcaster_id TEXT,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (telegram_id, twitch_id)
);

CREATE TABLE IF NOT EXISTS group_members (
  group_id INTEGER NOT NULL,
  telegram_id INTEGER NOT NULL,
  joined_at INTEGER,
  left_at INTEGER,
  PRIMARY KEY (group_id, telegram_id)
);

CREATE TABLE IF NOT EXISTS privileged (
  group_id INTEGER NOT NULL,
  telegram_id INTEGER NOT NULL,
  added_by INTEGER,
  note TEXT,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (group_id, telegram_id)
);
"""

# PG DDL (use BIGINT for Telegram IDs & group IDs)
CREATE_SQL_PG = r"""
CREATE TABLE IF NOT EXISTS telegram_users (
  telegram_id BIGINT PRIMARY KEY,
  username TEXT,
  first_name TEXT,
  last_name TEXT,
  linked_twitch_id TEXT
);

CREATE TABLE IF NOT EXISTS twitch_users (
  twitch_id TEXT PRIMARY KEY,
  login TEXT,
  display_name TEXT,
  email TEXT
);

CREATE TABLE IF NOT EXISTS broadcasters (
  broadcaster_id TEXT PRIMARY KEY,
  owner_telegram_id BIGINT NOT NULL,
  access_token TEXT NOT NULL,
  refresh_token TEXT NOT NULL,
  token_obtained_at INTEGER NOT NULL,
  token_expires_in INTEGER NOT NULL,
  group_id BIGINT,
  invite_link TEXT
);

CREATE TABLE IF NOT EXISTS oauth_states (
  state TEXT PRIMARY KEY,
  telegram_id BIGINT NOT NULL,
  purpose TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS links (
  telegram_id BIGINT NOT NULL,
  twitch_id TEXT NOT NULL,
  broadcaster_id TEXT,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (telegram_id, twitch_id)
);

CREATE TABLE IF NOT EXISTS group_members (
  group_id BIGINT NOT NULL,
  telegram_id BIGINT NOT NULL,
  joined_at INTEGER,
  left_at INTEGER,
  PRIMARY KEY (group_id, telegram_id)
);

CREATE TABLE IF NOT EXISTS privileged (
  group_id BIGINT NOT NULL,
  telegram_id BIGINT NOT NULL,
  added_by BIGINT,
  note TEXT,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (group_id, telegram_id)
);
"""

async def db_init():
    global pg_pool
    if USE_PG:
        pg_pool = await asyncpg.create_pool(DATABASE_URL)
        async with pg_pool.acquire() as con:
            for stmt in [s.strip() for s in CREATE_SQL_PG.split(";") if s.strip()]:
                await con.execute(stmt)
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.executescript(CREATE_SQL)
            await db.commit()

DATABASE_URL = os.getenv("DATABASE_URL")
USE_PG = bool(DATABASE_URL)

# Conversor de '?' → '%s' para psycopg
def _qmark_to_psycopg(query: str, n_params: int) -> str:
    out = []
    i = 0
    for ch in query:
        if ch == '?' and i < n_params:
            out.append('%s')
            i += 1
        else:
            out.append(ch)
    return ''.join(out)

async def db_init():
    if USE_PG:
        # Ejecuta el DDL en Postgres
        async with await psycopg.AsyncConnection.connect(DATABASE_URL) as con:
            async with con.cursor() as cur:
                for stmt in [s.strip() for s in CREATE_SQL_PG.split(';') if s.strip()]:
                    await cur.execute(stmt)
            await con.commit()
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.executescript(CREATE_SQL)
            await db.commit()

async def db_execute(query: str, *params):
    if USE_PG:
        q = _qmark_to_psycopg(query, len(params))
        async with await psycopg.AsyncConnection.connect(DATABASE_URL) as con:
            async with con.cursor() as cur:
                await cur.execute(q, params)
            await con.commit()
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(query, params)
            await db.commit()

async def db_fetchone(query: str, *params):
    if USE_PG:
        q = _qmark_to_psycopg(query, len(params))
        async with await psycopg.AsyncConnection.connect(DATABASE_URL) as con:
            con.row_factory = dict_row
            async with con.cursor() as cur:
                await cur.execute(q, params)
                row = await cur.fetchone()
            await con.commit()
            return row
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(query, params)
            row = await cur.fetchone()
            await cur.close()
            return row

async def db_fetchall(query: str, *params):
    if USE_PG:
        q = _qmark_to_psycopg(query, len(params))
        async with await psycopg.AsyncConnection.connect(DATABASE_URL) as con:
            con.row_factory = dict_row
            async with con.cursor() as cur:
                await cur.execute(q, params)
                rows = await cur.fetchall()
            await con.commit()
            return rows
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(query, params)
            rows = await cur.fetchall()
            await cur.close()
            return rows

async def is_privileged(group_id: int, user_id: int) -> bool:
    row = await db_fetchone(
        "SELECT 1 FROM privileged WHERE group_id=? AND telegram_id=? LIMIT 1",
        group_id, user_id
    )
    return bool(row)
    
# ---------------------------
# Utilities
# ---------------------------

def gen_state(n: int = 32) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(n))

async def send_async_message(chat_id: int, text: str, *, parse_mode: str | None = None, disable_preview: bool = True):
    """Thread-safe sender usable from Flask thread."""
    global ptb_app, ptb_loop
    if ptb_app is None or ptb_loop is None:
        logging.error("PTB app/loop not ready; cannot send message")
        return
    coro = ptb_app.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode=parse_mode,
        disable_web_page_preview=disable_preview,
    )
    asyncio.run_coroutine_threadsafe(coro, ptb_loop)


# ---------------------------
# Twitch API helpers
# ---------------------------
async def twitch_token_exchange(code: str, redirect_path: str) -> dict:
    redirect_uri = f"{BASE_URL}{redirect_path}"
    data = {
        "client_id": OAUTH_CLIENT_ID,
        "client_secret": OAUTH_CLIENT_SECRET,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": redirect_uri,
    }
    async with aiohttp.ClientSession() as sess:
        async with sess.post(TW_OAUTH_TOKEN, data=data) as r:
            r.raise_for_status()
            return await r.json()

async def twitch_refresh_token(refresh_token: str) -> dict:
    data = {
        "client_id": OAUTH_CLIENT_ID,
        "client_secret": OAUTH_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    async with aiohttp.ClientSession() as sess:
        async with sess.post(TW_OAUTH_TOKEN, data=data) as r:
            r.raise_for_status()
            return await r.json()

async def twitch_get_users(access_token: str, twitch_ids: list[str] | None = None) -> list[dict]:
    headers = {"Client-Id": OAUTH_CLIENT_ID, "Authorization": f"Bearer {access_token}"}
    params: list[tuple[str, str]] = []
    if twitch_ids:
        params = [("id", t) for t in twitch_ids]
    async with aiohttp.ClientSession() as sess:
        async with sess.get(f"{TW_API}/users", headers=headers, params=params) as r:
            r.raise_for_status()
            js = await r.json()
            return js.get("data", [])

async def twitch_get_self(access_token: str) -> Optional[dict]:
    users = await twitch_get_users(access_token)
    return users[0] if users else None

async def twitch_check_subscription(b_access_token: str, twitch_broadcaster_id: str, twitch_user_id: str) -> bool:
    headers = {"Client-Id": OAUTH_CLIENT_ID, "Authorization": f"Bearer {b_access_token}"}
    params = {"broadcaster_id": twitch_broadcaster_id, "user_id": twitch_user_id}
    async with aiohttp.ClientSession() as sess:
        async with sess.get(f"{TW_API}/subscriptions", headers=headers, params=params) as r:
            if r.status == 401:
                raise PermissionError("Twitch token unauthorized")
            r.raise_for_status()
            js = await r.json()
            return len(js.get("data", [])) > 0


# ---------------------------
# OAuth URL builders
# ---------------------------

def build_oauth_url_user(state: str) -> str:
    redirect_uri = f"{BASE_URL}/twitch/callback"
    scope = "+".join(SCOPE_USER)
    return (
        f"{TW_OAUTH_AUTH}?client_id={OAUTH_CLIENT_ID}&redirect_uri={redirect_uri}"
        f"&response_type=code&scope={scope}&state={state}"
    )

def build_oauth_url_setup(state: str) -> str:
    redirect_uri = f"{BASE_URL}/twitch/setup/callback"
    scope = "+".join(SCOPE_SETUP)
    return (
        f"{TW_OAUTH_AUTH}?client_id={OAUTH_CLIENT_ID}&redirect_uri={redirect_uri}"
        f"&response_type=code&scope={scope}&state={state}"
    )


# ---------------------------
# Flask Routes (OAuth callbacks)
# ---------------------------
@flask_app.get("/")
def root():
    return {"ok": True, "service": "telegram-twitch-bot"}

@flask_app.get("/healthz")
def healthz():
    return {"ok": True}

@flask_app.get("/twitch/callback")
def twitch_callback_user():
    try:
        state = request.args.get("state")
        code = request.args.get("code")
        if not state or not code:
            return make_response("Missing state/code", 400)
        # Lookup state
        row = asyncio.run(db_fetchone("SELECT * FROM oauth_states WHERE state=?", state))
        if not row or row["purpose"] != "user_link":
            return make_response("Invalid state", 400)
        telegram_id = int(row["telegram_id"])
        # Exchange code
        tokens = asyncio.run(twitch_token_exchange(code, "/twitch/callback"))
        access_token = tokens.get("access_token")
        # Identify user
        me = asyncio.run(twitch_get_self(access_token))
        if not me:
            return make_response("Cannot identify Twitch user", 400)
        twitch_id = me["id"]
        login = me.get("login")
        display_name = me.get("display_name")
        email = me.get("email")
        # Store
        if USE_PG:
            asyncio.run(db_execute(
                "INSERT INTO twitch_users(twitch_id, login, display_name, email) VALUES (?,?,?,?) "
                "ON CONFLICT (twitch_id) DO UPDATE SET login=EXCLUDED.login, display_name=EXCLUDED.display_name, email=EXCLUDED.email",
                twitch_id, login, display_name, email,
            ))
        else:
            asyncio.run(db_execute(
                "INSERT OR REPLACE INTO twitch_users(twitch_id, login, display_name, email) VALUES (?,?,?,?)",
                twitch_id, login, display_name, email,
            ))
        asyncio.run(db_execute(
            "UPDATE telegram_users SET linked_twitch_id=? WHERE telegram_id=?",
            twitch_id, telegram_id,
        ))
        if USE_PG:
            asyncio.run(db_execute(
                "INSERT INTO links(telegram_id, twitch_id, broadcaster_id, created_at) VALUES (?,?,NULL,?) "
                "ON CONFLICT (telegram_id, twitch_id) DO UPDATE SET broadcaster_id=EXCLUDED.broadcaster_id, created_at=EXCLUDED.created_at",
                telegram_id, twitch_id, int(time.time()),
            ))
        else:
            asyncio.run(db_execute(
                "INSERT OR REPLACE INTO links(telegram_id, twitch_id, broadcaster_id, created_at) VALUES (?,?,NULL,?)",
                telegram_id, twitch_id, int(time.time()),
            ))
        # Ack to user via bot (HTML safe)
        ack = (
            f"✅ Vinculación completada: Twitch <b>{escape(display_name or login)}</b> ↔️ Telegram. "
            "Ahora comprobaré tu suscripción…"
        )
        asyncio.run(send_async_message(telegram_id, ack, parse_mode="HTML"))
        # Find broadcaster config (latest by token time)
        b = asyncio.run(db_fetchone("SELECT * FROM broadcasters ORDER BY token_obtained_at DESC LIMIT 1"))
        if not b:
            asyncio.run(send_async_message(telegram_id, "Aún no hay ningún canal de Twitch configurado. Pide al streamer que haga la configuración"))
            return redirect("https://twitch.tv/")
        # Check subscription asynchronously on PTB loop
        if ptb_loop is not None:
            asyncio.run_coroutine_threadsafe(
                check_and_notify_subscription(telegram_id, twitch_id, b),
                ptb_loop,
            )
        return redirect("https://twitch.tv/")
    except Exception as e:
        logging.exception("Error in /twitch/callback: %s", e)
        return make_response("Internal error in callback", 500)

@flask_app.get("/twitch/setup/callback")
def twitch_callback_setup():
    try:
        state = request.args.get("state")
        code = request.args.get("code")
        if not state or not code:
            return make_response("Missing state/code", 400)
        row = asyncio.run(db_fetchone("SELECT * FROM oauth_states WHERE state=?", state))
        if not row or row["purpose"] != "broadcaster_setup":
            return make_response("Invalid state", 400)
        owner_tid = int(row["telegram_id"])
        tokens = asyncio.run(twitch_token_exchange(code, "/twitch/setup/callback"))
        access_token = tokens["access_token"]
        refresh_token = tokens.get("refresh_token")
        expires_in = tokens.get("expires_in", 3600)
        me = asyncio.run(twitch_get_self(access_token))
        if not me:
            return make_response("Cannot identify broadcaster", 400)
        broadcaster_id = me["id"]
        # store broadcaster record
        if USE_PG:
            asyncio.run(db_execute(
                "INSERT INTO broadcasters(broadcaster_id, owner_telegram_id, access_token, refresh_token, token_obtained_at, token_expires_in, group_id, invite_link) "
                "VALUES (?,?,?,?,?,?,NULL,NULL) "
                "ON CONFLICT (broadcaster_id) DO UPDATE SET "
                "owner_telegram_id=EXCLUDED.owner_telegram_id, "
                "access_token=EXCLUDED.access_token, "
                "refresh_token=COALESCE(EXCLUDED.refresh_token, broadcasters.refresh_token), "
                "token_obtained_at=EXCLUDED.token_obtained_at, "
                "token_expires_in=EXCLUDED.token_expires_in, "
                "group_id=COALESCE(broadcasters.group_id, EXCLUDED.group_id), "
                "invite_link=COALESCE(broadcasters.invite_link, EXCLUDED.invite_link)",
                broadcaster_id, owner_tid, access_token, refresh_token or "", int(time.time()), int(expires_in)
            ))
        else:
            asyncio.run(db_execute(
                "INSERT OR REPLACE INTO broadcasters(broadcaster_id, owner_telegram_id, access_token, refresh_token, token_obtained_at, token_expires_in, group_id, invite_link) "
                "VALUES (?,?,?,?,?,?, COALESCE((SELECT group_id FROM broadcasters WHERE broadcaster_id=?), NULL), COALESCE((SELECT invite_link FROM broadcasters WHERE broadcaster_id=?), NULL))",
                broadcaster_id, owner_tid, access_token, refresh_token or "", int(time.time()), int(expires_in), broadcaster_id, broadcaster_id
            ))
        asyncio.run(send_async_message(
            owner_tid,
            "✅ Canal vinculado como broadcaster.\nAhora ejecuta /setgroup dentro del grupo objetivo.",
        ))
        return redirect("https://twitch.tv/")
    except Exception as e:
        logging.exception("Error in /twitch/setup/callback: %s", e)
        return make_response("Internal error in setup callback", 500)


# ---------------------------
# Subscription + Invite helpers
# ---------------------------
async def ensure_valid_broadcaster_token(b_row) -> Tuple[str, str]:
    """Return (access_token, broadcaster_id), refreshing if needed."""
    access = b_row["access_token"]
    obtained = b_row["token_obtained_at"]
    expires = b_row["token_expires_in"]
    if int(time.time()) < obtained + expires - 120:
        return access, b_row["broadcaster_id"]
    # refresh
    logging.info("Refreshing broadcaster token…")
    tokens = await twitch_refresh_token(b_row["refresh_token"])
    access = tokens["access_token"]
    refresh = tokens.get("refresh_token", b_row["refresh_token"])  # sometimes absent
    expires = tokens.get("expires_in", 3600)
    await db_execute(
        "UPDATE broadcasters SET access_token=?, refresh_token=?, token_obtained_at=?, token_expires_in=? WHERE broadcaster_id=?",
        access, refresh, int(time.time()), int(expires), b_row["broadcaster_id"]
    )
    return access, b_row["broadcaster_id"]

async def create_or_get_invite_link(b_row) -> Optional[str]:
    group_id = b_row["group_id"]
    if not group_id:
        return None
    if b_row["invite_link"]:
        return b_row["invite_link"]
    try:
        link: ChatInviteLink = await ptb_app.bot.create_chat_invite_link(chat_id=group_id, creates_join_request=False)
        await db_execute("UPDATE broadcasters SET invite_link=? WHERE broadcaster_id=?", link.invite_link, b_row["broadcaster_id"]) 
        return link.invite_link
    except Exception as e:
        logging.exception("Failed to create invite link: %s", e)
        return None

async def check_and_notify_subscription(telegram_id: int, twitch_user_id: str, b_row):
    try:
        access, broadcaster_id = await ensure_valid_broadcaster_token(b_row)
        is_sub = await twitch_check_subscription(access, broadcaster_id, twitch_user_id)
    except PermissionError:
        b_row = await db_fetchone("SELECT * FROM broadcasters WHERE broadcaster_id=?", b_row["broadcaster_id"])
        access, broadcaster_id = await ensure_valid_broadcaster_token(b_row)
        is_sub = await twitch_check_subscription(access, broadcaster_id, twitch_user_id)
    except Exception as e:
        logging.exception("Subscription check failed: %s", e)
        await ptb_app.bot.send_message(chat_id=telegram_id, text="❌ Error comprobando tu suscripción. Intenta de nuevo más tarde.")
        return

    if is_sub:
        link = await create_or_get_invite_link(b_row)
        if link:
            await ptb_app.bot.send_message(chat_id=telegram_id, text=f"🎉 ¡Estás suscrito! Únete al grupo aquí: {link}")
        else:
            await ptb_app.bot.send_message(chat_id=telegram_id, text="✅ Estás suscrito, pero aún no hay grupo configurado. Pide al admin que ejecute /setgroup en el grupo.")
    else:
        await ptb_app.bot.send_message(chat_id=telegram_id, text="⚠️ No aparece una suscripción activa a este canal. Si te acabas de suscribir, espera unos minutos y usa /checkme.")


# ---------------------------
# Telegram Handlers
# ---------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not u:
        return

    if USE_PG:
        await db_execute(
            "INSERT INTO telegram_users(telegram_id, username, first_name, last_name) "
            "VALUES (%s,%s,%s,%s) "
            "ON CONFLICT (telegram_id) DO UPDATE SET "
            "username=EXCLUDED.username, first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name",
            u.id, u.username or "", u.first_name or "", u.last_name or "",
        )
    else:
        await db_execute(
            "INSERT OR IGNORE INTO telegram_users(telegram_id, username, first_name, last_name, linked_twitch_id) "
            "VALUES (?,?,?,?,NULL)",
            u.id, u.username or "", u.first_name or "", u.last_name or "",
        )
        await db_execute(
            "UPDATE telegram_users SET username=?, first_name=?, last_name=? WHERE telegram_id=?",
            u.username or "", u.first_name or "", u.last_name or "", u.id
        )

    state = gen_state()
    created = int(time.time())
    if USE_PG:
        await db_execute(
            "INSERT INTO oauth_states(state, telegram_id, purpose, created_at) "
            "VALUES (%s,%s,%s,%s) "
            "ON CONFLICT (state) DO UPDATE SET "
            "telegram_id=EXCLUDED.telegram_id, purpose=EXCLUDED.purpose, created_at=EXCLUDED.created_at",
            state, u.id, "user_link", created
        )
    else:
        await db_execute(
            "INSERT OR REPLACE INTO oauth_states(state, telegram_id, purpose, created_at) VALUES (?,?,?,?)",
            state, u.id, "user_link", created
        )

    url = build_oauth_url_user(state)
    safe_url = escape(url, quote=True) 
    msg = (
        "Para vincular tu cuenta de Twitch y comprobar si estás suscrito, haz click abajo.<br>"
        f"<a href=\"{safe_url}\">⬇️</a>"
    )
    logging.info("START -> %s", msg)
    await update.effective_chat.send_message(msg, parse_mode="HTML", disable_web_page_preview=False, 
    )

async def setup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not u:
        return
    state = gen_state()
    if USE_PG:
        await db_execute(
            "INSERT INTO oauth_states(state, telegram_id, purpose, created_at) VALUES (?,?,?,?) "
            "ON CONFLICT (state) DO UPDATE SET telegram_id=EXCLUDED.telegram_id, purpose=EXCLUDED.purpose, created_at=EXCLUDED.created_at",
            state, u.id, "broadcaster_setup", int(time.time())
        )
    else:
        await db_execute(
            "INSERT OR REPLACE INTO oauth_states(state, telegram_id, purpose, created_at) VALUES (?,?,?,?)",
            state, u.id, "broadcaster_setup", int(time.time())
        )
    url = build_oauth_url_setup(state)
    safe_url = escape(url, quote=True)
    await update.effective_chat.send_message(
        f"Vincula el canal de Twitch que administras para permitir que el bot lea suscripciones.\n"
        f"Debes entrar con la cuenta del canal y aceptar el permiso del bot haciendo click abajo.<a href=\"{url}\">⬇️</a>",
        parse_mode="HTML",
        disable_web_page_preview=False,
    )

async def setgroup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    u = update.effective_user
    if not chat:
        return
    if chat.type not in ("group", "supergroup"):
        await update.effective_message.reply_text("Ejecuta /setgroup dentro del grupo objetivo (donde el bot sea admin).")
        return
    b = await db_fetchone("SELECT * FROM broadcasters WHERE owner_telegram_id=? ORDER BY token_obtained_at DESC LIMIT 1", u.id)
    if not b:
        await update.effective_message.reply_text("No has configurado ningún broadcaster. Usa /setup primero en privado conmigo.")
        return
    try:
        link: ChatInviteLink = await context.bot.create_chat_invite_link(chat_id=chat.id, creates_join_request=False)
        await db_execute("UPDATE broadcasters SET group_id=?, invite_link=? WHERE broadcaster_id=?", chat.id, link.invite_link, b["broadcaster_id"]) 
        await update.effective_message.reply_text("✅ Grupo vinculado. A partir de ahora, los suscriptores recibirán este enlace de invitación.")
    except Exception as e:
        logging.exception("setgroup failed: %s", e)
        await update.effective_message.reply_text("❌ No pude crear el enlace de invitación. ¿Soy admin con permiso para invitar?")

async def checkme_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    row = await db_fetchone("SELECT linked_twitch_id FROM telegram_users WHERE telegram_id=?", u.id)
    if not row or not row["linked_twitch_id"]:
        await update.effective_message.reply_text("Primero necesitas vincular tu cuenta con /start.")
        return
    b = await db_fetchone("SELECT * FROM broadcasters ORDER BY token_obtained_at DESC LIMIT 1")
    if not b:
        await update.effective_message.reply_text("Aún no hay ningún canal configurado. Pide al admin que use /setup.")
        return
    await check_and_notify_subscription(u.id, row["linked_twitch_id"], b)

async def auditnow_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    b = await db_fetchone("SELECT * FROM broadcasters WHERE owner_telegram_id=? ORDER BY token_obtained_at DESC LIMIT 1", u.id)
    if not b or not b["group_id"]:
        await update.effective_message.reply_text("Configura /setup y /setgroup primero.")
        return
    kicked = await audit_group_and_kick(context, b)
    await update.effective_message.reply_text(f"Auditoría completada. Expulsados: {kicked}")

async def on_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cmu = update.chat_member
    if not cmu:
        return
    chat = cmu.chat
    user = cmu.new_chat_member.user
    status = cmu.new_chat_member.status
    if chat.type not in ("group", "supergroup"):
        return
    if status in (ChatMemberStatus.MEMBER, ChatMemberStatus.RESTRICTED):
        if USE_PG:
            await db_execute(
                "INSERT INTO group_members(group_id, telegram_id, joined_at, left_at) VALUES (?,?,?,NULL) "
                "ON CONFLICT (group_id, telegram_id) DO UPDATE SET joined_at=EXCLUDED.joined_at, left_at=NULL",
                chat.id, user.id, int(time.time())
            )
        else:
            await db_execute(
                "INSERT OR REPLACE INTO group_members(group_id, telegram_id, joined_at, left_at) VALUES (?,?,?,NULL)", chat.id, user.id, int(time.time())
            )
    elif status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED):
        await db_execute("UPDATE group_members SET left_at=? WHERE group_id=? AND telegram_id=?", int(time.time()), chat.id, user.id)

async def auditfull_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat or chat.type not in ("group", "supergroup"):
        await update.effective_message.reply_text("Ejecuta /auditfull dentro del grupo objetivo.")
        return

    # Busca la config del broadcaster para ESTE grupo
    b = await db_fetchone("SELECT * FROM broadcasters WHERE group_id=? LIMIT 1", chat.id)
    if not b:
        await update.effective_message.reply_text("Este grupo no está vinculado. Usa /setgroup primero.")
        return

    # Candidatos: vistos en grupo, usuarios que iniciaron el bot, y los que tienen link hecho
    candidates: set[int] = set()
    rows = await db_fetchall("SELECT telegram_id FROM group_members WHERE group_id=? AND left_at IS NULL", chat.id)
    for r in rows: candidates.add(int(r["telegram_id"]))
    rows = await db_fetchall("SELECT telegram_id FROM telegram_users")  # gente que hizo /start
    for r in rows: candidates.add(int(r["telegram_id"]))
    rows = await db_fetchall("SELECT telegram_id FROM links")  # cualquiera con link
    for r in rows: candidates.add(int(r["telegram_id"]))

    # Filtra los que REALMENTE están ahora en el grupo (via Bot API)
    present: list[int] = []
    for uid in candidates:
        try:
            cm = await context.bot.get_chat_member(chat.id, uid)
            if cm.status in (
                ChatMemberStatus.MEMBER,
                ChatMemberStatus.RESTRICTED,
                ChatMemberStatus.ADMINISTRATOR,
                ChatMemberStatus.OWNER,
            ):
                present.append(uid)
        except BadRequest:
            # user not found o nunca estuvo
            continue

    kicked = 0
    access, broadcaster_id = await ensure_valid_broadcaster_token(b)

    for uid in present:
        # No intentes expulsar admins/owner
        try:
            cm = await context.bot.get_chat_member(chat.id, uid)
            if cm.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
                continue
        except BadRequest:
            continue

        row = await db_fetchone("SELECT linked_twitch_id FROM telegram_users WHERE telegram_id=?", uid)
        tw_id = row["linked_twitch_id"] if row else None

        try:
            if not tw_id:
                # No vinculó Twitch -> fuera
                await context.bot.ban_chat_member(chat_id=chat.id, user_id=uid, until_date=int(time.time()) + 35)
                await context.bot.unban_chat_member(chat_id=chat.id, user_id=uid, only_if_banned=True)
                await db_execute("UPDATE group_members SET left_at=? WHERE group_id=? AND telegram_id=?",
                                 int(time.time()), chat.id, uid)
                kicked += 1
                continue

            # Vinculado: comprueba suscripción
            try:
                ok = await twitch_check_subscription(access, broadcaster_id, tw_id)
            except PermissionError:
                # refresca y reintenta una vez
                b2 = await db_fetchone("SELECT * FROM broadcasters WHERE broadcaster_id=?", b["broadcaster_id"])
                access, broadcaster_id = await ensure_valid_broadcaster_token(b2)
                ok = await twitch_check_subscription(access, broadcaster_id, tw_id)

            if not ok:
                await context.bot.ban_chat_member(chat_id=chat.id, user_id=uid, until_date=int(time.time()) + 35)
                await context.bot.unban_chat_member(chat_id=chat.id, user_id=uid, only_if_banned=True)
                await db_execute("UPDATE group_members SET left_at=? WHERE group_id=? AND telegram_id=?",
                                 int(time.time()), chat.id, uid)
                kicked += 1
        except Exception as e:
            logging.warning("auditfull: no pude procesar %s: %s", uid, e)
            continue

    await update.effective_message.reply_text(f"🧹 Limpieza completa. Expulsados: {kicked}")

async def privilege_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    if not chat or chat.type not in ("group", "supergroup"):
        await update.effective_message.reply_text("Ejecuta /privilege dentro del grupo.")
        return

    # Solo admins/owner pueden usarlo
    try:
        me = await context.bot.get_chat_member(chat.id, user.id)
        if me.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
            await update.effective_message.reply_text("Solo administradores pueden usar este comando.")
            return
    except BadRequest:
        await update.effective_message.reply_text("No pude verificar tus permisos.")
        return

    # Resolver objetivo: reply > id numérico > @username > 'me/yo'
    target_id: Optional[int] = None
    note = ""
    if update.effective_message.reply_to_message:
        target_id = update.effective_message.reply_to_message.from_user.id
        note = " ".join(context.args) if context.args else ""
    elif context.args:
        arg0 = context.args[0]
        rest = context.args[1:]
        note = " ".join(rest) if rest else ""
        if arg0.lower() in ("me", "yo"):
            target_id = user.id
        elif arg0.isdigit():
            target_id = int(arg0)
        else:
            username = arg0.lstrip("@")
            row = await db_fetchone(
                "SELECT telegram_id FROM telegram_users WHERE LOWER(username)=LOWER(?) LIMIT 1",
                username
            )
            if row:
                target_id = int(row["telegram_id"])

    if not target_id:
        await update.effective_message.reply_text(
            "Uso: responde a un mensaje con /privilege [nota]\n"
            "o /privilege <user_id|@username|me> [nota]"
        )
        return

    # Insert/Upsert
    ts = int(time.time())
    if USE_PG:
        await db_execute(
            "INSERT INTO privileged(group_id, telegram_id, added_by, note, created_at) "
            "VALUES (?,?,?,?,?) "
            "ON CONFLICT (group_id, telegram_id) DO UPDATE SET "
            "added_by=EXCLUDED.added_by, note=EXCLUDED.note, created_at=EXCLUDED.created_at",
            chat.id, target_id, user.id, note, ts
        )
    else:
        await db_execute(
            "INSERT OR REPLACE INTO privileged(group_id, telegram_id, added_by, note, created_at) "
            "VALUES (?,?,?,?,?)",
            chat.id, target_id, user.id, note, ts
        )

    await update.effective_message.reply_text(f"✅ Usuario {target_id} marcado como privilegiado en este grupo.")

async def unprivilege_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    if not chat or chat.type not in ("group", "supergroup"):
        await update.effective_message.reply_text("Ejecuta /unprivilege dentro del grupo.")
        return

    try:
        me = await context.bot.get_chat_member(chat.id, user.id)
        if me.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
            await update.effective_message.reply_text("Solo administradores pueden usar este comando.")
            return
    except BadRequest:
        await update.effective_message.reply_text("No pude verificar tus permisos.")
        return

    target_id: Optional[int] = None
    if update.effective_message.reply_to_message:
        target_id = update.effective_message.reply_to_message.from_user.id
    elif context.args:
        arg0 = context.args[0]
        if arg0.lower() in ("me", "yo"):
            target_id = user.id
        elif arg0.isdigit():
            target_id = int(arg0)
        else:
            username = arg0.lstrip("@")
            row = await db_fetchone(
                "SELECT telegram_id FROM telegram_users WHERE LOWER(username)=LOWER(?) LIMIT 1",
                username
            )
            if row:
                target_id = int(row["telegram_id"])

    if not target_id:
        await update.effective_message.reply_text(
            "Uso: responde a un mensaje con /unprivilege\n"
            "o /unprivilege <user_id|@username|me>"
        )
        return

    await db_execute(
        "DELETE FROM privileged WHERE group_id=? AND telegram_id=?",
        chat.id, target_id
    )
    await update.effective_message.reply_text(f"✅ Usuario {target_id} eliminado de privilegiados en este grupo.")

async def listprivileged_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat or chat.type not in ("group", "supergroup"):
        await update.effective_message.reply_text("Ejecuta /listprivileged dentro del grupo.")
        return

    rows = await db_fetchall(
        "SELECT telegram_id, note, created_at FROM privileged WHERE group_id=? ORDER BY created_at DESC",
        chat.id
    )
    if not rows:
        await update.effective_message.reply_text("No hay usuarios privilegiados en este grupo.")
        return

    lines = []
    for r in rows:
        uid = r["telegram_id"]
        note = r.get("note") if isinstance(r, dict) else r["note"]
        try:
            cm = await context.bot.get_chat_member(chat.id, uid)
            name = cm.user.full_name
        except Exception:
            name = str(uid)
        if note:
            lines.append(f"• {name} ({uid}) — {note}")
        else:
            lines.append(f"• {name} ({uid})")
    txt = "👑 Privilegiados:\n" + "\n".join(lines[:60])  # evita mensajes gigantes
    await update.effective_message.reply_text(txt)

# ---------------------------
# Weekly audit job
# ---------------------------
async def audit_group_and_kick(context: ContextTypes.DEFAULT_TYPE, b_row) -> int:
    group_id = b_row["group_id"]
    if not group_id:
        return 0
    # Include ALL current members in the group (even if they never linked Twitch)
    rows = await db_fetchall(
        "SELECT gm.telegram_id, tu.linked_twitch_id "
        "FROM group_members gm "
        "LEFT JOIN telegram_users tu ON gm.telegram_id = tu.telegram_id "
        "WHERE gm.group_id=? AND gm.left_at IS NULL",
        group_id,
    )
    
    if not rows:
        return 0

    kicked = 0
    # ensure broadcaster token once (refresh if needed inside loop only on 401)
    access, broadcaster_id = await ensure_valid_broadcaster_token(b_row)

    for r in rows:
        tg_id = r["telegram_id"]
        tw_id = r["linked_twitch_id"]
        
        if await is_privileged(chat.id, uid):
            continue

        try:
            # If NOT linked => kick directly
            if not tw_id:
                await context.bot.ban_chat_member(chat_id=group_id, user_id=tg_id, until_date=int(time.time()) + 35)
                await context.bot.unban_chat_member(chat_id=group_id, user_id=tg_id, only_if_banned=True)
                await db_execute(
                    "UPDATE group_members SET left_at=? WHERE group_id=? AND telegram_id=?",
                    int(time.time()), group_id, tg_id,
                )
                kicked += 1
                continue

            # Linked: check subscription
            try:
                ok = await twitch_check_subscription(access, broadcaster_id, tw_id)
            except PermissionError:
                # token unauthorized → refresh once
                b_row = await db_fetchone("SELECT * FROM broadcasters WHERE broadcaster_id=?", b_row["broadcaster_id"])
                access, broadcaster_id = await ensure_valid_broadcaster_token(b_row)
                ok = await twitch_check_subscription(access, broadcaster_id, tw_id)

            if not ok:
                await context.bot.ban_chat_member(chat_id=group_id, user_id=tg_id, until_date=int(time.time()) + 35)
                await context.bot.unban_chat_member(chat_id=group_id, user_id=tg_id, only_if_banned=True)
                await db_execute(
                    "UPDATE group_members SET left_at=? WHERE group_id=? AND telegram_id=?",
                    int(time.time()), group_id, tg_id,
                )
                kicked += 1
        except Exception as e:
            logging.warning("Audit: cannot process %s: %s", tg_id, e)
            continue

    return kicked

async def weekly_audit_job(context: ContextTypes.DEFAULT_TYPE):
    b_rows = await db_fetchall("SELECT * FROM broadcasters WHERE group_id IS NOT NULL")
    for b in b_rows:
        try:
            kicked = await audit_group_and_kick(context, b)
            owner = b["owner_telegram_id"]
            await context.bot.send_message(chat_id=owner, text=f"🔎 Auditoría semanal del grupo {b['group_id']}: expulsados {kicked} no-subs.")
        except Exception as e:
            logging.exception("Weekly audit failed for %s: %s", b["broadcaster_id"], e)


# ---------------------------
# Main
# ---------------------------

def run_flask():
    from waitress import serve
    port = int(os.getenv("PORT", "8080"))
    logging.info(f"Starting Flask with waitress on port {port}…")
    serve(flask_app, host="0.0.0.0", port=port)

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    # Ensure a default asyncio event loop exists (Python 3.13 on Render may not create one by default)
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    # Initialize DB synchronously before PTB using the ensured event loop
    loop.run_until_complete(db_init())

    application = (
        ApplicationBuilder()
        .token(TELEGRAM_BOT_TOKEN)
        .post_init(_on_startup)
        .concurrent_updates(True)
        .build()
    )

    # Handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("setup", setup_cmd))
    application.add_handler(CommandHandler("setgroup", setgroup_cmd))
    application.add_handler(CommandHandler("checkme", checkme_cmd))
    application.add_handler(CommandHandler("auditnow", auditnow_cmd))
    application.add_handler(ChatMemberHandler(on_chat_member, ChatMemberHandler.CHAT_MEMBER))
    application.add_handler(CommandHandler("auditfull", auditfull_cmd))
    application.add_handler(CommandHandler("privilege", privilege_cmd))
    application.add_handler(CommandHandler("unprivilege", unprivilege_cmd))
    application.add_handler(CommandHandler("listprivileged", listprivileged_cmd))

    # Jobs: Semanalmente cada Lunes a las 05am
    from zoneinfo import ZoneInfo
    from datetime import time as dtime
    tz = ZoneInfo(TZ)
    try:
        application.job_queue.scheduler.configure(timezone=tz)
    except Exception:
        try:
            application.job_queue.timezone = tz
        except Exception:
            pass
    application.job_queue.run_daily(
        weekly_audit_job,
        time=dtime(hour=5, minute=0, second=0),
        days=(0,),
        name="weekly_audit",
    )

    # Start Flask in background thread
    import threading
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()

    logging.info("Starting Application.run_polling()… OAuth redirect base: %s", BASE_URL)
    application.run_polling(close_loop=False)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Bye!")
