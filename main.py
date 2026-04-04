import logging
import asyncio
import json
import os
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    filters, ContextTypes
)
from keep_alive import keep_alive

BOT_TOKEN = "8666624638:AAHmGnnyuVXTHDKUaTA6syhODSf6DF65Zbw"
ADMIN_ID = 6102256074

MOVIES_FILE = "movies.json"

logging.basicConfig(level=logging.INFO)

def load_movies():
    if os.path.exists(MOVIES_FILE):
        with open(MOVIES_FILE, "r") as f:
            return json.load(f)
    return {}

def save_movies(movies):
    with open(MOVIES_FILE, "w") as f:
        json.dump(movies, f, ensure_ascii=False)

MOVIES = load_movies()
pending_video = {}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Salom! Movie HD botiga xush kelibsiz! 🎥\nKino kodini yozing ✍️🗒️"
    )

async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id == ADMIN_ID:
        file_id = update.message.video.file_id
        pending_video[user_id] = ("video", file_id)
        await update.message.reply_text(
            "✅ Video qabul qilindi!\n"
            "Kod va ma'lumot berish uchun:\n"
            "/add <kod> <nom> | <sifat> | <til> | <vaqt>\n\n"
            "Masalan:\n/add 1 Culpa Mia | 1080p | O'zbek | 1:57:36"
        )

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id == ADMIN_ID:
        file_id = update.message.document.file_id
        pending_video[user_id] = ("document", file_id)
        await update.message.reply_text(
            "✅ Fayl qabul qilindi!\n"
            "Kod va ma'lumot berish uchun:\n"
            "/add <kod> <nom> | <sifat> | <til> | <vaqt>\n\n"
            "Masalan:\n/add 1 Culpa Mia | 1080p | O'zbek | 1:57:36"
        )

async def add_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text(
            "Ishlatish:\n/add <kod> <nom> | <sifat> | <til> | <vaqt>\n\n"
            "Masalan:\n/add 1 Culpa Mia | 1080p | O'zbek | 1:57:36"
        )
        return
    if user_id not in pending_video:
        await update.message.reply_text("Avval video yuboring, keyin /add yozing.")
        return

    code = context.args[0]
    rest = " ".join(context.args[1:])
    parts = [p.strip() for p in rest.split("|")]

    nom = parts[0] if len(parts) > 0 and parts[0] else "Noma'lum"
    sifat = parts[1] if len(parts) > 1 else "—"
    til = parts[2] if len(parts) > 2 else "—"
    vaqt = parts[3] if len(parts) > 3 else "—"

    media_type, file_id = pending_video.pop(user_id)
    MOVIES[code] = {
        "type": media_type,
        "file_id": file_id,
        "nom": nom,
        "sifat": sifat,
        "til": til,
        "vaqt": vaqt
    }
    save_movies(MOVIES)
    await update.message.reply_text(
        f"✅ Saqlandi!\n"
        f"📌 Kod: {code}\n"
        f"🎬 Nom: {nom}\n"
        f"📺 Sifat: {sifat}\n"
        f"🌐 Til: {til}\n"
        f"⏱ Vaqt: {vaqt}"
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    if not text.isdigit():
        await update.message.reply_text("Kino kodini yozing ✍️🗒️")
        return

    code = text
    data = MOVIES.get(code)

    if not data:
        await update.message.reply_text(f"❌ {code} kodli kino topilmadi.")
        return

    caption = (
        f"🎬 {data.get('nom', '—')}\n"
        f"📺 Sifat: {data.get('sifat', '—')}\n"
        f"🌐 Til: {data.get('til', '—')}\n"
        f"⏱ Davomiylik: {data.get('vaqt', '—')}\n"
        f"📌 Kod: {code}"
    )

    file_id = data["file_id"]
    if data["type"] == "video":
        await update.message.reply_video(video=file_id, caption=caption)
    else:
        await update.message.reply_document(document=file_id, caption=caption)

async def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("add", add_movie))
    app.add_handler(MessageHandler(filters.VIDEO, handle_video))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    keep_alive()
    print("Bot ishga tushdi...")
    async with app:
        await app.start()
        await app.updater.start_polling()
        await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
