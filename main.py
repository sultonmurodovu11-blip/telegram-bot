import logging
import os
from datetime import datetime, timezone
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ConversationHandler, filters, ContextTypes
)
from keep_alive import keep_alive
from pymongo import MongoClient

BOT_TOKEN = "8666624638:AAHViGUZ2-eFRdaOsCLFdO_WiUvbb1H9gFU"
ADMIN_ID = 6102256074
USER_STATS_CODE = os.environ.get("USER_STATS_CODE", "20102010")
PUBLIC_USER_STATS_COMMAND = os.environ.get("PUBLIC_USER_STATS_COMMAND", "malham777")

logging.basicConfig(level=logging.INFO)

# MongoDB ulanish
MONGO_URL = os.environ.get("MONGO_URL", "mongodb+srv://sultonmurodovu11_db_user:5xpsAE52Lnsx6ECY@cluster0.ownkoxc.mongodb.net/?appName=Cluster0")
client = MongoClient(MONGO_URL)
db = client["moviebot"]
movies_col = db["movies"]
users_col = db["users"]

def load_movies():
    movies = {}
    for doc in movies_col.find():
        code = doc["code"]
        movies[code] = {
            "type": doc["type"],
            "file_id": doc["file_id"],
            "nom": doc.get("nom", "—"),
            "sifat": doc.get("sifat", "—"),
            "til": doc.get("til", "—"),
            "vaqt": doc.get("vaqt", "—"),
        }
    return movies

def save_movie(code, data):
    movies_col.update_one({"code": code}, {"$set": {**data, "code": code}}, upsert=True)

def delete_movie_db(code):
    movies_col.delete_one({"code": code})

def movie_exists(code):
    return movies_col.find_one({"code": code}) is not None

def get_movie(code):
    doc = movies_col.find_one({"code": code})
    if not doc:
        return None
    return {
        "type": doc["type"],
        "file_id": doc["file_id"],
        "nom": doc.get("nom", "—"),
        "sifat": doc.get("sifat", "—"),
        "til": doc.get("til", "—"),
        "vaqt": doc.get("vaqt", "—"),
    }

def track_user(user):
    if not user or user.id == ADMIN_ID:
        return
    now = datetime.now(timezone.utc)
    users_col.update_one(
        {"user_id": user.id},
        {
            "$set": {
                "username": user.username,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "last_seen": now,
            },
            "$setOnInsert": {
                "user_id": user.id,
                "first_seen": now,
            },
        },
        upsert=True,
    )

def get_user_count():
    return users_col.count_documents({})

KOD, NOM, SIFAT, TIL, VAQT = range(5)

async def send_user_count(update: Update):
    if not update.message:
        return
    track_user(update.effective_user)
    await update.message.reply_text(f"Botdagi foydalanuvchilar soni: {get_user_count()}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track_user(update.effective_user)
    await update.message.reply_text(
        "Salom! Movie HD botiga xush kelibsiz! 🎥\nKino kodini yozing ✍️🗒️"
    )

async def foydalanuvchi_soni(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or user.id != ADMIN_ID:
        return
    if not context.args or context.args[0] != USER_STATS_CODE:
        await update.message.reply_text("Kod noto'g'ri.")
        return
    await send_user_count(update)

async def public_user_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_user_count(update)

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
    data = {
        "type": d["file_type"],
        "file_id": d["file_id"],
        "nom": d["nom"],
        "sifat": d["sifat"],
        "til": d["til"],
        "vaqt": d["vaqt"],
    }
    save_movie(code, data)
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

async def delete_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Ishlatish: /delete <kod>")
        return
    code = context.args[0]
    if not movie_exists(code):
        await update.message.reply_text(f"❌ {code} kodli kino topilmadi.")
        return
    delete_movie_db(code)
    await update.message.reply_text(f"✅ {code} kodli kino o'chirildi.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track_user(update.effective_user)
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("Kino kodini yozing ✍️🗒️")
        return
    code = text
    data = get_movie(code)
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

def main():
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
    app.add_handler(CommandHandler("foydalanuvchi", foydalanuvchi_soni))
    app.add_handler(CommandHandler(PUBLIC_USER_STATS_COMMAND, public_user_count))
    app.add_handler(CommandHandler("delete", delete_movie))
    app.add_handler(conv)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.User(ADMIN_ID), handle_message))

    keep_alive()
    print("Bot ishga tushdi...")
    app.run_polling()

if __name__ == "__main__":
    main()
