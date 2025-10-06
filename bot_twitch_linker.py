"""
Telegram–Twitch Sub Bot
---------------------------------

¿Qué hace?
============
- Cuando un usuario abre el bot a través de un enlace o /start, envía un mensaje directo con un enlace de inicio de sesión de Twitch OAuth para verificar su cuenta de Twitch.
- Después de iniciar sesión, el bot vincula su ID de Telegram <-> ID de Twitch en SQLite.
- El bot comprueba si están suscritos al canal de Twitch configurado y, si es así, les envía un enlace de invitación a un grupo de Telegram. Si no es así, se lo notifica.
- Semanalmente, el bot audita el grupo y expulsa a los miembros que ya no están suscritos.
- Admite múltiples canales: un difusor (propietario/moderador del canal) puede ejecutar /setup para conceder al bot el ámbito «channel:read:subscriptions» y vincular un grupo de Telegram de destino utilizando /setgroup.

Entorno
===========
Crea un archivo `.env` con:

TELEGRAM_BOT_TOKEN=123456:ABC...
FLASK_SECRET= {code secreto (puede ser una frase)}
OAUTH_CLIENT_ID=twitch_client_id
OAUTH_CLIENT_SECRET=twitch_client_secret
BASE_URL=https://your.public.domain
TZ=Europe/Madrid

Ejecutar
===
$ pip install python-telegram-bot==20.8 aiohttp Flask python-dotenv aiosqlite
$ python bot_twitch_linker.py

Notas
=====
- El bot debe ser administrador del grupo de destino y tener derechos para invitar a usuarios y eliminar miembros.
- Configuración de la aplicación Twitch: añade la URL de redireccionamiento: {BASE_URL}/twitch/callback y {BASE_URL}/twitch/setup/callback
- Telegram: desactiva el modo de privacidad si deseas capturar las actualizaciones de chat_member en los grupos.

"""
from __future__ import annotations

import asyncio
import json
import logging
import secrets
import string
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Tuple

import aiohttp
import aiosqlite
from flask import Flask, request, redirect, make_response
from dotenv import load_dotenv
import os

from telegram import (
    Update,
    ChatInviteLink,
)
from telegram.constants import ChatMemberStatus
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ChatMemberHandler,
    ContextTypes,
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

if not TELEGRAM_BOT_TOKEN:
    raise SystemExit("Missing TELEGRAM_BOT_TOKEN in env")
if not OAUTH_CLIENT_ID or not OAUTH_CLIENT_SECRET:
    raise SystemExit("Missing Twitch OAUTH_CLIENT_ID / OAUTH_CLIENT_SECRET in env")

# Twitch OAuth endpoints
TW_OAUTH_AUTH = "https://id.twitch.tv/oauth2/authorize"
TW_OAUTH_TOKEN = "https://id.twitch.tv/oauth2/token"
TW_API = "https://api.twitch.tv/helix"

# Scopes
SCOPE_SETUP = ["channel:read:subscriptions"]  # for broadcaster channel setup
SCOPE_USER = ["openid", "user:read:email"]   # for linking a viewer's identity

# SQLite file
DB_PATH = os.getenv("DB_PATH", "twitch_gate.db")

# Flask app (OAuth callbacks)
flask_app = Flask(__name__)
flask_app.secret_key = FLASK_SECRET

# PTB Application (initialized later)
ptb_app: Application | None = None
ptb_loop: asyncio.AbstractEventLoop | None = None

# Capture PTB loop on startup so we can safely schedule bot calls from Flask thread
async def _on_startup(app: Application) -> None:
    global ptb_app, ptb_loop
    ptb_app = app
    ptb_loop = asyncio.get_running_loop()
    logging.info("PTB event loop capturado y listo.")

# ---------------------------
# Data & DB Helpers
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

-- Broadcaster setup per Telegram admin (owner). One admin can configure one channel binding.
CREATE TABLE IF NOT EXISTS broadcasters (
  broadcaster_id TEXT PRIMARY KEY,
  owner_telegram_id INTEGER NOT NULL,
  access_token TEXT NOT NULL,
  refresh_token TEXT NOT NULL,
  token_obtained_at INTEGER NOT NULL,
  token_expires_in INTEGER NOT NULL,
  group_id INTEGER,         -- target Telegram group chat id
  invite_link TEXT          -- cached current invite link
);

-- State store for CSRF + routing
CREATE TABLE IF NOT EXISTS oauth_states (
  state TEXT PRIMARY KEY,
  telegram_id INTEGER NOT NULL,
  purpose TEXT NOT NULL,  -- 'user_link' or 'broadcaster_setup'
  created_at INTEGER NOT NULL
);

-- Link table (tele <-> twitch) explicitly
CREATE TABLE IF NOT EXISTS links (
  telegram_id INTEGER NOT NULL,
  twitch_id TEXT NOT NULL,
  broadcaster_id TEXT, -- optional; who granted access
  created_at INTEGER NOT NULL,
  PRIMARY KEY (telegram_id, twitch_id)
);

-- Track group membership we have invited/seen
CREATE TABLE IF NOT EXISTS group_members (
  group_id INTEGER NOT NULL,
  telegram_id INTEGER NOT NULL,
  joined_at INTEGER,
  left_at INTEGER,
  PRIMARY KEY (group_id, telegram_id)
);
"""

async def db_init():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(CREATE_SQL)
        await db.commit()

async def db_execute(query: str, *params):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(query, params)
        await db.commit()

async def db_fetchone(query: str, *params):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(query, params)
        row = await cur.fetchone()
        await cur.close()
        return row

async def db_fetchall(query: str, *params):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(query, params)
        rows = await cur.fetchall()
        await cur.close()
        return rows

# ---------------------------
# Utilities
# ---------------------------

def gen_state(n: int = 32) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(n))

async def send_async_message(chat_id: int, text: str):
    """Thread-safe sender usable from Flask thread."""
    global ptb_app, ptb_loop
    if ptb_app is None or ptb_loop is None:
        logging.error("PTB app/loop not ready; cannot send message")
        return
    coro = ptb_app.bot.send_message(chat_id=chat_id, text=text, disable_web_page_preview=True)
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
    params = []
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
                # will be handled by caller (refresh)
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
        asyncio.run(db_execute(
            "INSERT OR REPLACE INTO twitch_users(twitch_id, login, display_name, email) VALUES (?,?,?,?)",
            twitch_id, login, display_name, email,
        ))
        asyncio.run(db_execute(
            "UPDATE telegram_users SET linked_twitch_id=? WHERE telegram_id=?",
            twitch_id, telegram_id,
        ))
        asyncio.run(db_execute(
            "INSERT OR REPLACE INTO links(telegram_id, twitch_id, broadcaster_id, created_at) VALUES (?,?,NULL,?)",
            telegram_id, twitch_id, int(time.time()),
        ))
        # Ack to user via bot
        asyncio.run(send_async_message(telegram_id, f"✅ Vinculación completada " **{display_name or login}** ". Ahora comprobaré tu suscripción…"))
        # Find broadcaster config (simple use-case: last one)
        b = asyncio.run(db_fetchone("SELECT * FROM broadcasters ORDER BY rowid DESC LIMIT 1"))
        if not b:
            asyncio.run(send_async_message(telegram_id, "Aún no hay ningún canal de Twitch configurado. Pide al dueño que ejecute /setup."))
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
        asyncio.run(db_execute(
            "INSERT OR REPLACE INTO broadcasters(broadcaster_id, owner_telegram_id, access_token, refresh_token, token_obtained_at, token_expires_in, group_id, invite_link) "
            "VALUES (?,?,?,?,?,?, COALESCE((SELECT group_id FROM broadcasters WHERE broadcaster_id=?), NULL), COALESCE((SELECT invite_link FROM broadcasters WHERE broadcaster_id=?), NULL))",
            broadcaster_id, owner_tid, access_token, refresh_token or "", int(time.time()), int(expires_in), broadcaster_id, broadcaster_id
        ))
        asyncio.run(send_async_message(owner_tid,
                    "✅ Canal vinculado como broadcaster.\n\nAhora ejecuta /setgroup dentro del grupo objetivo."))
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
    # Prefer existing valid invite link
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
    # Ensure token alive
    try:
        access, broadcaster_id = await ensure_valid_broadcaster_token(b_row)
        is_sub = await twitch_check_subscription(access, broadcaster_id, twitch_user_id)
    except PermissionError:
        # try refresh then retry once
        b_row = await db_fetchone("SELECT * FROM broadcasters WHERE broadcaster_id=?", b_row["broadcaster_id"])
        access, broadcaster_id = await ensure_valid_broadcaster_token(b_row)
        is_sub = await twitch_check_subscription(access, broadcaster_id, twitch_user_id)
    except Exception as e:
        logging.exception("Subscription check failed: %s", e)
        await ptb_app.bot.send_message(chat_id=telegram_id, text="❌ Error comprobando tu suscripción. Intenta de nuevo más tarde.")
        return

    if is_sub:
        # get invite link
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
    # upsert telegram user
    await db_execute(
        "INSERT OR IGNORE INTO telegram_users(telegram_id, username, first_name, last_name, linked_twitch_id) VALUES (?,?,?,?,NULL)",
        u.id, u.username or "", u.first_name or "", u.last_name or "",
    )
    await db_execute(
        "UPDATE telegram_users SET username=?, first_name=?, last_name=? WHERE telegram_id=?",
        u.username or "", u.first_name or "", u.last_name or "", u.id
    )
    state = gen_state()
    await db_execute(
        "INSERT OR REPLACE INTO oauth_states(state, telegram_id, purpose, created_at) VALUES (?,?,?,?)",
        state, u.id, "user_link", int(time.time())
    )
    url = build_oauth_url_user(state)
    msg = (
    f"Bienvenido al bot de subs Telegram–Twitch\n"
    f"Para vincular tu cuenta de Twitch y comprobar si estás suscrito, haz click '<a href=\"{url}\">aquí</a>' y sabrás si puedes acceder al grupo de suscriptores."
    )
    await update.effective_chat.send_message(msg, parse_mode="HTML",disable_web_page_preview=False)

async def setup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not u:
        return
    state = gen_state()
    await db_execute(
        "INSERT OR REPLACE INTO oauth_states(state, telegram_id, purpose, created_at) VALUES (?,?,?,?)",
        state, u.id, "broadcaster_setup", int(time.time())
    )
    url = build_oauth_url_setup(state)
    await update.effective_chat.send_message(
        f"Vincula el canal de Twitch que administras para permitir que el bot lea suscripciones.\n"
        f"Debes entrar con la cuenta del canal y aceptar el permiso <code>channel:read:subscriptions</code> haciendo click <a href=\"{url}\">aquí</a>.",
        parse_mode="HTML",
        disable_web_page_preview=True,
    )

async def setgroup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    u = update.effective_user
    if not chat:
        return
    if chat.type not in ("group", "supergroup"):
        await update.effective_message.reply_text("Ejecuta /setgroup dentro del grupo objetivo (donde el bot sea admin).")
        return
    # find broadcaster owned by this user
    b = await db_fetchone("SELECT * FROM broadcasters WHERE owner_telegram_id=? ORDER BY rowid DESC LIMIT 1", u.id)
    if not b:
        await update.effective_message.reply_text("No has configurado ningún broadcaster. Usa /setup primero en privado conmigo.")
        return
    try:
        # Ensure bot admin & create invite
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
    b = await db_fetchone("SELECT * FROM broadcasters ORDER BY rowid DESC LIMIT 1")
    if not b:
        await update.effective_message.reply_text("Aún no hay ningún canal configurado. Pide al admin que use /setup.")
        return
    await check_and_notify_subscription(u.id, row["linked_twitch_id"], b)

async def auditnow_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    b = await db_fetchone("SELECT * FROM broadcasters WHERE owner_telegram_id=? ORDER BY rowid DESC LIMIT 1", u.id)
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
        await db_execute("INSERT OR REPLACE INTO group_members(group_id, telegram_id, joined_at, left_at) VALUES (?,?,?,NULL)", chat.id, user.id, int(time.time()))
    elif status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED):
        await db_execute("UPDATE group_members SET left_at=? WHERE group_id=? AND telegram_id=?", int(time.time()), chat.id, user.id)

# ---------------------------
# Weekly audit job
# ---------------------------

async def audit_group_and_kick(context: ContextTypes.DEFAULT_TYPE, b_row) -> int:
    group_id = b_row["group_id"]
    if not group_id:
        return 0
    # collect linked users who appear in group
    rows = await db_fetchall(
        "SELECT gm.telegram_id, tu.linked_twitch_id FROM group_members gm JOIN telegram_users tu ON gm.telegram_id=tu.telegram_id "
        "WHERE gm.group_id=? AND gm.left_at IS NULL AND tu.linked_twitch_id IS NOT NULL",
        group_id,
    )
    if not rows:
        return 0
    kicked = 0
    # ensure broadcaster token
    access, broadcaster_id = await ensure_valid_broadcaster_token(b_row)
    for r in rows:
        tg_id = r["telegram_id"]
        tw_id = r["linked_twitch_id"]
        try:
            ok = await twitch_check_subscription(access, broadcaster_id, tw_id)
        except PermissionError:
            b_row = await db_fetchone("SELECT * FROM broadcasters WHERE broadcaster_id=?", b_row["broadcaster_id"])
            access, broadcaster_id = await ensure_valid_broadcaster_token(b_row)
            ok = await twitch_check_subscription(access, broadcaster_id, tw_id)
        except Exception as e:
            logging.warning("Audit: error checking %s: %s", tg_id, e)
            continue
        if not ok:
            try:
                await context.bot.ban_chat_member(chat_id=group_id, user_id=tg_id, until_date=int(time.time()) + 35)  # short ban to force removal
                await context.bot.unban_chat_member(chat_id=group_id, user_id=tg_id, only_if_banned=True)
                await db_execute("UPDATE group_members SET left_at=? WHERE group_id=? AND telegram_id=?", int(time.time()), group_id, tg_id)
                kicked += 1
            except Exception as e:
                logging.warning("Audit: cannot kick %s: %s", tg_id, e)
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
    # Use waitress (production WSGI server) and bind to Render's PORT
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

    # Jobs: weekly Monday 05:00 Europe/Madrid
    from zoneinfo import ZoneInfo
    from datetime import time as dtime
    tz = ZoneInfo(TZ)
    # Configure JobQueue timezone if supported (PTB v21 removed the 'timezone' kw in run_daily)
    try:
        application.job_queue.scheduler.configure(timezone=tz)
    except Exception:
        try:
            application.job_queue.timezone = tz  # fallback for other PTB versions
        except Exception:
            pass
    application.job_queue.run_daily(
        weekly_audit_job,
        time=dtime(hour=5, minute=0, second=0),
        days=(0,),
        name="weekly_audit",
    )

    # Start Flask in background thread (waitress binds to Render's PORT)
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

