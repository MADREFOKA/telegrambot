import os
import logging
import requests
import sqlite3
import asyncio
from urllib.parse import quote_plus
from flask import Flask, request
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, ContextTypes
from threading import Thread

# ===== Variables de entorno =====
BOT_TOKEN = os.getenv("BOT_TOKEN")
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
TWITCH_CHANNEL_ID = os.getenv("TWITCH_CHANNEL_ID")
TWITCH_REDIRECT_URI = os.getenv("TWITCH_REDIRECT_URI")  # https://tu-app.up.railway.app/callback

PORT = int(os.environ.get("PORT", 5000))

if not BOT_TOKEN:
    raise ValueError("Falta BOT_TOKEN")

# ===== Logging =====
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# ===== Flask =====
app = Flask(__name__)

@app.route("/")
def home():
    return "✅ Bot funcionando", 200

# ===== Base de datos =====
conn = sqlite3.connect('db.sqlite', check_same_thread=False)
c = conn.cursor()
c.execute('CREATE TABLE IF NOT EXISTS users (telegram_id INTEGER PRIMARY KEY, twitch_id TEXT, access_token TEXT)')
conn.commit()

# ===== Telegram Bot =====
bot = Bot(token=BOT_TOKEN)

async def send_telegram_message(chat_id: int, text: str):
    await bot.send_message(chat_id=chat_id, text=text)

# ===== Callback de Twitch =====
@app.route("/callback", methods=["GET"])
def twitch_callback():
    code = request.args.get("code")
    state = request.args.get("state")  # chat_id de Telegram

    if not code or not state:
        return "Faltan parámetros 'code' o 'state'", 400

    # Intercambia code por access_token
    token_resp = requests.post("https://id.twitch.tv/oauth2/token", data={
        "client_id": TWITCH_CLIENT_ID,
        "client_secret": TWITCH_CLIENT_SECRET,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": TWITCH_REDIRECT_URI
    }).json()

    access_token = token_resp.get("access_token")
    if not access_token:
        return "Error obteniendo token de Twitch", 400

    # Obtiene usuario de Twitch
    user_resp = requests.get("https://api.twitch.tv/helix/users", headers={
        "Authorization": f"Bearer {access_token}",
        "Client-Id": TWITCH_CLIENT_ID
    }).json()

    if "data" not in user_resp or len(user_resp["data"]) == 0:
        return "Error obteniendo datos de usuario Twitch", 400

    twitch_user = user_resp["data"][0]
    twitch_id = twitch_user["id"]

    # Guarda en base de datos
    c.execute("REPLACE INTO users (telegram_id, twitch_id, access_token) VALUES (?, ?, ?)",
              (int(state), twitch_id, access_token))
    conn.commit()

    # Comprueba suscripción
    sub_resp = requests.get("https://api.twitch.tv/helix/subscriptions", headers={
        "Authorization": f"Bearer {access_token}",
        "Client-Id": TWITCH_CLIENT_ID
    }, params={
        "broadcaster_id": TWITCH_CHANNEL_ID,
        "user_id": twitch_id
    }).json()

    chat_id = int(state)

    # Respuesta al usuario
    async def send_result():
        if "data" in sub_resp and len(sub_resp["data"]) > 0:
            await send_telegram_message(chat_id, "¡Eres suscriptor! Enlace al grupo: https://t.me/+pjcgoeLWrOxmMTk0")
        else:
            await send_telegram_message(chat_id, "No estás suscrito al canal de Twitch.")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(send_result())
    loop.close()

    return "✅ Vinculación completada. Puedes cerrar esta ventana."

# ===== Comandos de Telegram =====
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
    await update.message.reply_text(f"Vincula tu Twitch aquí:\n{auth_url}")

# ===== Iniciar bot con polling =====
def run_polling():
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.run_polling()

if __name__ == "__main__":
    # Hilo para el bot
    Thread(target=run_polling, daemon=True).start()
    # Flask para el callback
    app.run(host="0.0.0.0", port=PORT)
