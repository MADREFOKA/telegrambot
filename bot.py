import os
import logging
import requests
import sqlite3
import asyncio
from urllib.parse import quote_plus
from flask import Flask, request
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from threading import Thread

# ========= Variables de entorno =========
BOT_TOKEN = os.getenv("BOT_TOKEN")
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
TWITCH_CHANNEL_ID = os.getenv("TWITCH_CHANNEL_ID")            # ID numérico del canal
TWITCH_REDIRECT_URI = os.getenv("TWITCH_REDIRECT_URI")        # ej: https://tu-app.up.railway.app/callback
PORT = int(os.environ.get("PORT", 5000))

if not BOT_TOKEN:
    raise ValueError("Falta BOT_TOKEN")
for k, v in {
    "TWITCH_CLIENT_ID": TWITCH_CLIENT_ID,
    "TWITCH_CLIENT_SECRET": TWITCH_CLIENT_SECRET,
    "TWITCH_CHANNEL_ID": TWITCH_CHANNEL_ID,
    "TWITCH_REDIRECT_URI": TWITCH_REDIRECT_URI,
}.items():
    if not v:
        raise ValueError(f"Falta {k}")

# ========= Logging =========
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)

# ========= SQLite =========
conn = sqlite3.connect("db.sqlite", check_same_thread=False)
c = conn.cursor()
c.execute("""
CREATE TABLE IF NOT EXISTS users (
    telegram_id  INTEGER PRIMARY KEY,
    twitch_id    TEXT,
    access_token TEXT
)
""")
conn.commit()

# ========= Flask =========
app = Flask(__name__)

@app.route("/")
def home():
    return "✅ Flask OK /callback listo", 200

@app.route("/callback", methods=["GET"])
def twitch_callback():
    """Callback OAuth de Twitch con try/except y logs (evita 500 ciegos)."""
    try:
        code = request.args.get("code")
        state = request.args.get("state")  # chat_id de Telegram
        if not code or not state:
            logging.error("Callback sin code/state: %s", dict(request.args))
            return "Faltan parámetros 'code' o 'state'", 400

        logging.info("Intercambiando code por token...")
        token_resp = requests.post(
            "https://id.twitch.tv/oauth2/token",
            data={
                "client_id": TWITCH_CLIENT_ID,
                "client_secret": TWITCH_CLIENT_SECRET,
                "code": code,
                "grant_type": "authorization_code",
                "redirect_uri": TWITCH_REDIRECT_URI
            },
            timeout=20
        ).json()

        access_token = token_resp.get("access_token")
        if not access_token:
            logging.error("Token error: %s", token_resp)
            return f"Error obteniendo token de Twitch: {token_resp}", 400

        logging.info("Pidiendo datos de usuario...")
        user_resp = requests.get(
            "https://api.twitch.tv/helix/users",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Client-Id": TWITCH_CLIENT_ID
            },
            timeout=20
        ).json()
        if "data" not in user_resp or not user_resp["data"]:
            logging.error("Users error: %s", user_resp)
            return f"Error obteniendo datos del usuario: {user_resp}", 400

        twitch_id = user_resp["data"][0]["id"]

        logging.info("Guardando vínculo en SQLite...")
        c.execute(
            "REPLACE INTO users (telegram_id, twitch_id, access_token) VALUES (?, ?, ?)",
            (int(state), twitch_id, access_token)
        )
        conn.commit()

        logging.info("Comprobando suscripción...")
        sub_resp = requests.get(
            "https://api.twitch.tv/helix/subscriptions",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Client-Id": TWITCH_CLIENT_ID
            },
            params={"broadcaster_id": TWITCH_CHANNEL_ID, "user_id": twitch_id},
            timeout=20
        ).json()

        text = (
            "✅ ¡Eres suscriptor! Enlace al grupo: https://t.me/+pjcgoeLWrOxmMTk0"
            if ("data" in sub_resp and len(sub_resp["data"]) > 0)
            else "❌ No estás suscrito al canal de Twitch."
        )

        logging.info("Enviando mensaje a Telegram chat_id=%s...", state)
        async def _send(chat_id: int, msg: str):
            await application.bot.send_message(chat_id=chat_id, text=msg)

        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_send(int(state), text))
        finally:
            loop.close()

        return "Vinculación completada. Puedes cerrar esta ventana."
    except Exception as e:
        logging.exception("Excepción en /callback")
        return f"Error interno en callback: {repr(e)}", 500

def run_flask():
    logging.info("Iniciando Flask en puerto %s", PORT)
    app.run(host="0.0.0.0", port=PORT)

# ========= Telegram (POLLING) =========
application = Application.builder().token(BOT_TOKEN).build()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info("/start recibido de chat_id=%s", update.effective_chat.id)
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
    await update.message.reply_text(f"Vincula tu Twitch aquí:\n{auth_url}")

application.add_handler(CommandHandler("start", start))

# ========= Arranque =========
if __name__ == "__main__":
    # Asegurar que NO hay webhook activo (si lo hay, el polling no recibe nada)
    try:
        r = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook", timeout=10)
        logging.info("deleteWebhook: %s %s", r.status_code, r.text)
    except Exception as e:
        logging.warning("No se pudo borrar webhook: %s", e)

    # Flask en hilo aparte
    Thread(target=run_flask, daemon=True).start()

    # Polling en hilo principal (bloqueante)
    logging.info("Iniciando polling de Telegram…")
    application.run_polling(drop_pending_updates=True)
