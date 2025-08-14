# Flujo:
#  - /link_canal  -> el dueño autoriza channel:read:subscriptions -> guardamos token del canal en BD
#  - /start       -> el usuario autoriza -> vinculamos twitch_id y comprobamos sub usando el token del canal
#  - /callback    -> procesa ambos casos según el 'state'

import os
import time
import logging
import requests
import sqlite3
from urllib.parse import quote_plus
from threading import Thread
from flask import Flask, request
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ========= VARIABLES DE ENTORNO. Cambiar en servicio web =========
BOT_TOKEN = os.getenv("BOT_TOKEN") # Token del bot de telegram
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID") # ID cliente de Twitch
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET") # Secret client de Twitch
TWITCH_CHANNEL_ID = os.getenv("TWITCH_CHANNEL_ID") # Channel ID de Twitch
TWITCH_REDIRECT_URI = os.getenv("TWITCH_REDIRECT_URI") # Redirect puesto en Twitch develops. Ej. https://{Servicio web}/callback
PORT = int(os.environ.get("PORT", 5000))

required = {
    "BOT_TOKEN": BOT_TOKEN,
    "TWITCH_CLIENT_ID": TWITCH_CLIENT_ID,
    "TWITCH_CLIENT_SECRET": TWITCH_CLIENT_SECRET,
    "TWITCH_CHANNEL_ID": TWITCH_CHANNEL_ID,
    "TWITCH_REDIRECT_URI": TWITCH_REDIRECT_URI,
}
for k, v in required.items():
    if not v:
        raise ValueError(f"Falta variable de entorno: {k}")

TWITCH_CHANNEL_ID = str(TWITCH_CHANNEL_ID).strip()  # normaliza para comparar
logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)

# ========= Base de datos de telegram y twitch =========
conn = sqlite3.connect("db.sqlite", check_same_thread=False)
c = conn.cursor()
c.execute("""
CREATE TABLE IF NOT EXISTS users (
    telegram_id  INTEGER PRIMARY KEY,
    twitch_id    TEXT,
    access_token TEXT
)
""")
c.execute("""
CREATE TABLE IF NOT EXISTS broadcaster_token (
    broadcaster_id TEXT PRIMARY KEY,
    access_token   TEXT NOT NULL,
    refresh_token  TEXT NOT NULL,
    expires_at     INTEGER NOT NULL
)
""")
conn.commit()

def save_broadcaster_tokens(broadcaster_id: str, access_token: str, refresh_token: str, expires_in: int):
    expires_at = int(time.time()) + int(expires_in) - 60  # margen 60s
    c.execute("""
        INSERT INTO broadcaster_token (broadcaster_id, access_token, refresh_token, expires_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(broadcaster_id) DO UPDATE SET
            access_token=excluded.access_token,
            refresh_token=excluded.refresh_token,
            expires_at=excluded.expires_at
    """, (broadcaster_id, access_token, refresh_token or "", expires_at))
    conn.commit()

def get_broadcaster_row():
    return c.execute("SELECT broadcaster_id, access_token, refresh_token, expires_at FROM broadcaster_token LIMIT 1").fetchone()

def refresh_broadcaster_token(refresh_token: str):
    resp = requests.post("https://id.twitch.tv/oauth2/token", data={
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": TWITCH_CLIENT_ID,
        "client_secret": TWITCH_CLIENT_SECRET
    }, timeout=20).json()
    if "access_token" not in resp:
        raise RuntimeError(f"Error al refrescar token del broadcaster: {resp}")
    return resp  # {access_token, refresh_token?, expires_in, ...}

def get_valid_broadcaster_token():
    row = get_broadcaster_row()
    if not row:
        raise RuntimeError("Aún no hay token del canal. El dueño debe ejecutar /link_canal y autorizar.")
    broadcaster_id, access_token, refresh_token, expires_at = row
    if time.time() < int(expires_at):
        return broadcaster_id, access_token
    new = refresh_broadcaster_token(refresh_token)
    save_broadcaster_tokens(broadcaster_id, new["access_token"], new.get("refresh_token", refresh_token), new.get("expires_in", 3600))
    return broadcaster_id, new["access_token"]

app = Flask(__name__)

@app.route("/")
def home():
    return "✅ Flask OK /callback listo", 200

def send_telegram_sync(chat_id: int, text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=20)
    except Exception as e:
        logging.exception("Error enviando mensaje a Telegram: %s", e)

@app.route("/callback", methods=["GET"])
def twitch_callback():
    """Callback OAuth de Twitch. Maneja flujo OWNER (/link_canal) y USER (/start)."""
    try:
        code = request.args.get("code")
        state = request.args.get("state", "")
        if not code or not state:
            logging.error("Callback sin code/state: %s", dict(request.args))
            return "Faltan parámetros 'code' o 'state'", 400

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
        if "access_token" not in token_resp:
            logging.error("Token error: %s", token_resp)
            return f"Error obteniendo token de Twitch: {token_resp}", 400

        access_token = token_resp["access_token"]
        refresh_token = token_resp.get("refresh_token")
        expires_in = token_resp.get("expires_in", 3600)

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

        twitch_user = user_resp["data"][0]
        twitch_id = str(twitch_user["id"])

        # ===== Flujo para el OWNER: /link_canal =====
        if state.startswith("owner:"):
            if twitch_id != TWITCH_CHANNEL_ID:
                return "El usuario autorizado no coincide con el canal configurado.", 400
            if not refresh_token:
                return "No se recibió refresh_token para el broadcaster (necesario).", 400
            save_broadcaster_tokens(twitch_id, access_token, refresh_token, expires_in)
            return "✔️ Canal del streamer vinculado. Ya puedo comprobar suscripciones.", 200

        chat_id = int(state)
        c.execute(
            "REPLACE INTO users (telegram_id, twitch_id, access_token) VALUES (?, ?, ?)",
            (chat_id, twitch_id, access_token)
        )
        conn.commit()
        try:
            broadcaster_id, broadcaster_token = get_valid_broadcaster_token()
        except Exception as e:
            logging.error("No hay token del canal: %s", e)
            send_telegram_sync(chat_id, "⚠️ El canal aún no está vinculado. El dueño debe ejecutar /link_canal y autorizar.")
            return "Falta token del canal. Pida al dueño que haga /link_canal.", 200

        sub_resp = requests.get(
            "https://api.twitch.tv/helix/subscriptions",
            headers={
                "Authorization": f"Bearer {broadcaster_token}",
                "Client-Id": TWITCH_CLIENT_ID
            },
            params={"broadcaster_id": TWITCH_CHANNEL_ID, "user_id": twitch_id},
            timeout=20
        ).json()

        if "data" in sub_resp and len(sub_resp["data"]) > 0:
            text = "✅ ¡Eres suscriptor! Enlace al grupo:" # Insertar enlace del grupo de Telegram
        else:
            text = "❌ No estás suscrito al canal de Twitch."

        send_telegram_sync(chat_id, text)
        return "Vinculación completada. Puedes cerrar esta ventana."
    except Exception as e:
        logging.exception("Excepción en /callback")
        return f"Error interno en callback: {repr(e)}", 500

def run_flask():
    logging.info("Iniciando Flask en puerto %s", PORT)
    app.run(host="0.0.0.0", port=PORT)

application = Application.builder().token(BOT_TOKEN).build()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info("/start de chat_id=%s", update.effective_chat.id)
    chat_id = update.effective_chat.id
    auth_url = (
        "https://id.twitch.tv/oauth2/authorize"
        f"?client_id={TWITCH_CLIENT_ID}"
        f"&redirect_uri={quote_plus(TWITCH_REDIRECT_URI)}"
        f"&response_type=code"
        f"&state={chat_id}"
    )
    await update.message.reply_text(f"Vincula tu Twitch aquí:\n{auth_url}")

async def link_canal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info("/link_canal por chat_id=%s", update.effective_chat.id)
    # Aqui por si quieres poner restricciones segun TelegramId o TwitchNick.
    scope = "channel:read:subscriptions"
    state = f"owner:{update.effective_chat.id}"
    auth_url = (
        "https://id.twitch.tv/oauth2/authorize"
        f"?client_id={TWITCH_CLIENT_ID}"
        f"&redirect_uri={quote_plus(TWITCH_REDIRECT_URI)}"
        f"&response_type=code"
        f"&scope={quote_plus(scope)}"
        f"&state={state}"
    )
    await update.message.reply_text(
        "Autoriza el bot para leer las suscripciones de tu canal:\n" + auth_url
    )

application.add_handler(CommandHandler("start", start))
application.add_handler(CommandHandler("link_canal", link_canal))

if __name__ == "__main__":
    try:
        r = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook", timeout=10)
        logging.info("deleteWebhook: %s %s", r.status_code, r.text)
    except Exception as e:
        logging.warning("No se pudo borrar webhook: %s", e)

    Thread(target=run_flask, daemon=True).start()

    logging.info("Iniciando polling de Telegram…")
    application.run_polling(drop_pending_updates=True)

