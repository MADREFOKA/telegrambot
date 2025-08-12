import os
import logging
import requests
import sqlite3
import asyncio
from flask import Flask, request
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, ContextTypes

BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.environ.get("PORT", 5000))
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # https://tu-proyecto.up.railway.app
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
TWITCH_CHANNEL_ID = os.getenv("TWITCH_CHANNEL_ID")
TWITCH_REDIRECT_URI = os.getenv("TWITCH_REDIRECT_URI")  # https://tu-proyecto.up.railway.app/callback

if not BOT_TOKEN:
    raise ValueError("Falta la variable BOT_TOKEN en Railway")

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

app = Flask(__name__)
bot = Bot(token=BOT_TOKEN)

conn = sqlite3.connect('db.sqlite', check_same_thread=False)
c = conn.cursor()
c.execute('CREATE TABLE IF NOT EXISTS users (telegram_id INTEGER PRIMARY KEY, twitch_id TEXT, access_token TEXT)')
conn.commit()

async def send_telegram_message(chat_id: int, text: str):
    await bot.send_message(chat_id=chat_id, text=text)

@app.route("/callback", methods=["GET"])
def twitch_callback():
    code = request.args.get("code")
    state = request.args.get("state")  # Aquí llega el telegram chat_id

    if not code or not state:
        return "Faltan parámetros 'code' o 'state'", 400

    # Intercambia el código por token
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

    # Obtiene info del usuario Twitch
    user_resp = requests.get("https://api.twitch.tv/helix/users", headers={
        "Authorization": f"Bearer {access_token}",
        "Client-Id": TWITCH_CLIENT_ID
    }).json()

    if "data" not in user_resp or len(user_resp["data"]) == 0:
        return "Error obteniendo datos de usuario Twitch", 400

    twitch_user = user_resp["data"][0]
    twitch_id = twitch_user["id"]

    # Guarda o actualiza en la base de datos
    c.execute("REPLACE INTO users (telegram_id, twitch_id, access_token) VALUES (?, ?, ?)", (int(state), twitch_id, access_token))
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
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    if "data" in sub_resp and len(sub_resp["data"]) > 0:
        text = "¡Eres suscriptor! Aquí tienes el enlace al grupo: https://t.me/+pjcgoeLWrOxmMTk0"
    else:
        text = "No estás suscrito al canal de Twitch."

    loop.run_until_complete(send_telegram_message(chat_id, text))
    loop.close()

    return "Vinculación completada. Puedes cerrar esta ventana."

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    auth_url = (
        f"https://id.twitch.tv/oauth2/authorize"
        f"?client_id={TWITCH_CLIENT_ID}"
        f"&redirect_uri={TWITCH_REDIRECT_URI}"
        f"&response_type=code"
        f"&scope=user:read:subscriptions"
        f"&state={chat_id}"
    )
    await update.message.reply_text(f"Para vincular Twitch, inicia sesión aquí:\n{auth_url}")

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Bot funcionando correctamente.")

application = Application.builder().token(BOT_TOKEN).build()
application.add_handler(CommandHandler("start", start))
application.add_handler(CommandHandler("ping", ping))

@app.route(f"/{BOT_TOKEN}", methods=["POST"])
def telegram_webhook():
    update = Update.de_json(request.get_json(force=True), application.bot)
    application.update_queue.put_nowait(update)
    return "OK", 200

if __name__ == "__main__":
    application.bot.set_webhook(url=f"{WEBHOOK_URL}/{BOT_TOKEN}")
    app.run(host="0.0.0.0", port=PORT)
