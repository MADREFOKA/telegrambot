# bot.py — Minimal en Railway: /start -> login Twitch -> /callback -> comprueba sub y avisa

import os
import logging
import requests
import sqlite3
import asyncio
import threading
from urllib.parse import quote_plus
from flask import Flask, request
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, ContextTypes

# -------------------- Variables de entorno (Railway -> Variables) --------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")                  # ej: https://tu-app.up.railway.app
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
TWITCH_CHANNEL_ID = os.getenv("TWITCH_CHANNEL_ID")
TWITCH_REDIRECT_URI = os.getenv("TWITCH_REDIRECT_URI")  # ej: https://tu-app.up.railway.app/callback
PORT = int(os.environ.get("PORT", 5000))

# -------------------- Validaciones básicas --------------------
if not BOT_TOKEN or not WEBHOOK_URL:
    raise ValueError("Faltan BOT_TOKEN o WEBHOOK_URL")
if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET or not TWITCH_CHANNEL_ID or not TWITCH_REDIRECT_URI:
    raise ValueError("Faltan credenciales/config de Twitch")

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# -------------------- DB --------------------
conn = sqlite3.connect("db.sqlite", check_same_thread=False)
c = conn.cursor()
c.execute("""
CREATE TABLE IF NOT EXISTS users (
    telegram_id INTEGER PRIMARY KEY,
    twitch_id   TEXT,
    access_token TEXT
)
""")
conn.commit()

# -------------------- Flask --------------------
app = Flask(__name__)
bot = Bot(token=BOT_TOKEN)

async def send_telegram_message(chat_id: int, text: str):
    await bot.send_message(chat_id=chat_id, text=text)

@app.route(f"/{BOT_TOKEN}", methods=["POST"])
def telegram_webhook():
    payload = request.get_json(force=True)
    update = Update.de_json(payload, application.bot)
    application.update_queue.put_nowait(update)
    return "OK", 200

@app.route("/callback", methods=["GET"])
def twitch_callback():
    code = request.args.get("code")
    state = request.args.get("state")  # chat_id de Telegram

    if not code or not state:
        return "Faltan parámetros 'code' o 'state'", 400

    # Intercambiar code por access_token
    token_resp = requests.post(
        "https://id.twitch.tv/oauth2/token",
        data={
            "client_id": TWITCH_CLIENT_ID,
            "client_secret": TWITCH_CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": TWITCH_REDIRECT_URI
        },
        timeout=15
    ).json()

    access_token = token_resp.get("access_token")
    if not access_token:
        return f"Error obteniendo token de Twitch: {token_resp}", 400

    # Datos del usuario de Twitch
    user_resp = requests.get(
        "https://api.twitch.tv/helix/users",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Client-Id": TWITCH_CLIENT_ID
        },
        timeout=15
    ).json()
    if "data" not in user_resp or not user_resp["data"]:
        return f"Error obteniendo datos del usuario: {user_resp}", 400

    twitch_id = user_resp["data"][0]["id"]

    # Guardar/actualizar vínculo
    c.execute(
        "REPLACE INTO users (telegram_id, twitch_id, access_token) VALUES (?, ?, ?)",
        (int(state), twitch_id, access_token)
    )
    conn.commit()

    # Comprobar suscripción
    sub_resp = requests.get(
        "https://api.twitch.tv/helix/subscriptions",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Client-Id": TWITCH_CLIENT_ID
        },
        params={
            "broadcaster_id": TWITCH_CHANNEL_ID,
            "user_id": twitch_id
        },
        timeout=15
    ).json()

    text = (
        "✅ ¡Eres suscriptor! Enlace al grupo: https://t.me/+pjcgoeLWrOxmMTk0"
        if ("data" in sub_resp and len(sub_resp["data"]) > 0)
        else "❌ No estás suscrito al canal de Twitch."
    )

    # Enviar mensaje a Telegram
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(send_telegram_message(int(state), text))
    loop.close()

    return "Vinculación completada. Puedes cerrar esta ventana."

# -------------------- Telegram bot --------------------
application = Application.builder().token(BOT_TOKEN).build()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    scope = "user:read:subscriptions"
    auth_url = (
        "https://id.twitch.tv/oauth2/authorize"
        f"?client_id={TWITCH_CLIENT_ID}"
        f"&redirect_uri={quote_plus(TWITCH_REDIRECT_URI)}"
        f"&response_type=code"
        f"&scope={quote_plus(scope)}"
        f"&state={chat_id}"
    )
    await update.message.reply_text(
        f"Vincula tu Twitch aquí:\n{auth_url}"
    )

application.add_handler(CommandHandler("start", start))

def run_application():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(application.initialize())
    loop.run_until_complete(application.start())
    loop.run_forever()

# -------------------- Arranque --------------------
if __name__ == "__main__":
    # Configurar webhook de Telegram
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",
            params={"url": f"{WEBHOOK_URL}/{BOT_TOKEN}"},
            timeout=10
        )
        logging.info("setWebhook: %s %s", r.status_code, r.text)
    except Exception as e:
        logging.exception("Error configurando setWebhook")

    # Iniciar bot (procesa /start)
    t = threading.Thread(target=run_application, daemon=True)
    t.start()

    # Iniciar servidor HTTP (callback Twitch + webhook Telegram)
    app.run(host="0.0.0.0", port=PORT)
