import nest_asyncio
nest_asyncio.apply()
from flask import Flask, request
import requests
import os
import sqlite3
from telegram import Bot
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from threading import Thread

# DATOS A CAMBIAR !!!!
TELEGRAM_TOKEN = '8233729452:AAG8GP8Bm_mDwC1f9Hgxx9m9dIpKOmz3TKo'  # Token del Bot
TWITCH_CLIENT_ID = '702uubahdlbcg7flmqqy2ntpzya7c5'  # ID Client de Twitch
TWITCH_CLIENT_SECRET = 'eob9vwbrkx51xfmick9ncxr7oeskxn'  # Secret Client
TWITCH_CHANNEL_ID = '26806052'  # ID del Canal de Twitch
TWITCH_REDIRECT_URI = "https://5f54f8f57124.ngrok-free.app/twitch_callback"  # Cambia aquí tu ngrok URL

app = Flask(__name__)
bot = Bot(token=TELEGRAM_TOKEN)

conn = sqlite3.connect('db.sqlite', check_same_thread=False)
c = conn.cursor()
c.execute('CREATE TABLE IF NOT EXISTS users (telegram_id INTEGER PRIMARY KEY, twitch_id TEXT, access_token TEXT)')
conn.commit()

@app.route('/twitch_callback')
def twitch_callback():
    code = request.args.get('code')
    state = request.args.get('state')
    if not code or not state:
        return "Faltan parámetros", 400

    token_resp = requests.post('https://id.twitch.tv/oauth2/token', data={
        'client_id': TWITCH_CLIENT_ID,
        'client_secret': TWITCH_CLIENT_SECRET,
        'code': code,
        'grant_type': 'authorization_code',
        'redirect_uri': TWITCH_REDIRECT_URI
    }).json()

    access_token = token_resp.get('access_token')
    if not access_token:
        return "Error obteniendo token", 400

    user_resp = requests.get('https://api.twitch.tv/helix/users', headers={
        'Authorization': f'Bearer {access_token}',
        'Client-Id': TWITCH_CLIENT_ID
    }).json()
    twitch_user = user_resp['data'][0]
    twitch_id = twitch_user['id']

    c.execute('REPLACE INTO users (telegram_id, twitch_id, access_token) VALUES (?, ?, ?)', (int(state), twitch_id, access_token))
    conn.commit()

    sub_resp = requests.get('https://api.twitch.tv/helix/subscriptions', headers={
        'Authorization': f'Bearer {access_token}',
        'Client-Id': TWITCH_CLIENT_ID
    }, params={
        'broadcaster_id': TWITCH_CHANNEL_ID,
        'user_id': twitch_id
    }).json()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def send_message():
        if 'data' in sub_resp and len(sub_resp['data']) > 0:
            await bot.send_message(chat_id=int(state), text="¡Eres suscriptor! Aquí tienes el enlace al grupo: https://t.me/+pjcgoeLWrOxmMTk0")
        else:
            await bot.send_message(chat_id=int(state), text="No estás suscrito al canal de Twitch.")

    loop.run_until_complete(send_message())
    loop.close()

    return "Vinculación completada. Puedes cerrar esta ventana."

async def start(update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    auth_url = (
        f"https://id.twitch.tv/oauth2/authorize"
        f"?client_id={TWITCH_CLIENT_ID}"
        f"&redirect_uri={TWITCH_REDIRECT_URI}"
        f"&response_type=code"
        f"&scope=user:read:subscriptions"
        f"&state={chat_id}"
    )
    await update.message.reply_text("Para vincular Twitch, inicia sesión aquí:\n" + auth_url)

async def run_flask():
    app.run(port=5000, host='0.0.0.0', use_reloader=False)

async def main():
    application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    application.add_handler(CommandHandler('start', start))

    loop = asyncio.get_event_loop()
    flask_thread = Thread(target=app.run, kwargs={'port':5000, 'host':'0.0.0.0', 'use_reloader':False})
    flask_thread.start()

    await application.run_polling()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
