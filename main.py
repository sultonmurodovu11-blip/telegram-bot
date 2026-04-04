import logging
import asyncio
import json
import os
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ConversationHandler, filters, ContextTypes
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

# Conversation holatlari
KOD, NOM, SIFAT, TIL, VAQT = range(5)

# --- START ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Salom! Movie HD botiga xush kelibsiz! 🎥\nKino kodini yozing ✍️🗒️"
    )

# --- VIDEO/DOCUMENT qabul qilish ---
async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return
    context.user_data["file_id"] = update.message.video.file_id
    context.user_data["file_type"] = "video"
    await update.message.reply_text("✅ Video qabul qilindi!\n\nKino raqamini kiriting:")
    return KOD

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return
    context.user_data["file_id"] = update.message.document.file_id
    context.user_data["file_type"] = "document"
    await update.message.reply_text("✅ Fayl qabul qilindi!\n\nKino raqamini kiriting:")
    return KOD

async def get_kod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["kod"] = update.message.text.strip()
    await update.message.reply_text("Kino nomini kiriting:")
    return NOM

async def get_nom(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["nom"] = update.message.text.strip()
    await update.message.reply_text("Sifatini kiriting (masalan: 1080p, 720p):")
    return SIFAT

async def get_sifat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["sifat"] = update.message.text.strip()
    await update.message.reply_text("Tilini kiriting (masalan: O'zbek, Rus):")
    return TIL

async def get_til(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["til"] = update.message.text.strip()
    await update.message.reply_text("Davomiyligini kiriting (masalan: 1:57:36):")
    return VAQT

async def get_vaqt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    d = context.user_data
    d["vaqt"] = update.message.text.strip()
    code = d["kod"]
    MOVIES[code] = {
        "type": d["file_type"],
        "file_id": d["file_id"],
        "nom": d["nom"],
        "sifat": d["sifat"],
        "til": d["til"],
        "vaqt": d["vaqt"]
    }
    save_movies(MOVIES)
    await update.message.reply_text(
        f"✅ Saqlandi!\n\n"
        f"📌 Kod: {code}\n"
        f"🎬 Nom: {d['nom']}\n"
        f"📺 Sifat: {d['sifat']}\n"
        f"🌐 Til: {d['til']}\n"
        f"⏱ Davomiylik: {d['vaqt']}"
    )
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Bekor qilindi.")
    return ConversationHandler.END

# --- O'CHIRISH ---
async def delete_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Ishlatish: /delete <kod>")
        return
    code = context.args[0]
    if code not in MOVIES:
        await update.message.reply_text(f"❌ {code} kodli kino topilmadi.")
        return
    del MOVIES[code]
    save_movies(MOVIES)
    await update.message.reply_text(f"✅ {code} kodli kino o'chirildi.")

# --- FOYDALANUVCHI ---
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

    conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.VIDEO & filters.User(ADMIN_ID), handle_video),
            MessageHandler(filters.Document.ALL & filters.User(ADMIN_ID), handle_document),
        ],
        states={
            KOD:  [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_kod)],
            NOM:  [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_nom)],
            SIFAT:[MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_sifat)],
            TIL:  [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_til)],
            VAQT: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_vaqt)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("delete", delete_movie))
    app.add_handler(conv)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    keep_alive()
    print("Bot ishga tushdi...")
    async with app:
        await app.start()
        await app.updater.start_polling()
        await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
