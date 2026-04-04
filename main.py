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
        json.dump(movies, f)

MOVIES = load_movies()
pending_video = {}  # admin yuborgan videoni vaqtincha saqlash

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
            f"✅ Video qabul qilindi!\nKod berish uchun: /add <raqam>\nMasalan: /add 2"
        )

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id == ADMIN_ID:
        file_id = update.message.document.file_id
        pending_video[user_id] = ("document", file_id)
        await update.message.reply_text(
            f"✅ Fayl qabul qilindi!\nKod berish uchun: /add <raqam>\nMasalan: /add 2"
        )

async def add_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Ishlatish: /add <raqam>")
        return
    code = context.args[0]
    if user_id not in pending_video:
        await update.message.reply_text("Avval video yuboring, keyin /add <raqam> yozing.")
        return
    media_type, file_id = pending_video.pop(user_id)
    MOVIES[code] = (media_type, file_id)
    save_movies(MOVIES)
    await update.message.reply_text(f"✅ Kod {code} saqlandi!")

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

    media_type, file_id = data
    if media_type == "video":
        await update.message.reply_video(video=file_id, caption=f"📌 Kod: {code}")
    else:
        await update.message.reply_document(document=file_id, caption=f"📌 Kod: {code}")

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
