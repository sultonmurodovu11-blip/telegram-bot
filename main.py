from __future__ import annotations

import asyncio
import importlib
import logging
import os
import signal
import sys
import time
from typing import TYPE_CHECKING

try:
    from keep_alive import keep_alive, set_health_state
except ImportError:
    from .keep_alive import keep_alive, set_health_state

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.environ.get("ADMIN_ID", "6102256074"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
)
logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from telegram import Update
    from telegram.ext import ContextTypes

ApplicationBuilder = None
CommandHandler = None
ContextTypes = None
ConversationHandler = None
MessageHandler = None
filters = None
MongoClient = None
PyMongoError = Exception

# Global flag — SIGTERM kelganda bot restart qiladi, o'chirmaydi
_shutdown_requested = False


def ensure_telegram_imports():
    global ApplicationBuilder, CommandHandler, ContextTypes, ConversationHandler, MessageHandler, filters
    if ApplicationBuilder is not None:
        return
    telegram_ext = importlib.import_module("telegram.ext")
    ApplicationBuilder = telegram_ext.ApplicationBuilder
    CommandHandler = telegram_ext.CommandHandler
    ContextTypes = telegram_ext.ContextTypes
    ConversationHandler = telegram_ext.ConversationHandler
    MessageHandler = telegram_ext.MessageHandler
    filters = telegram_ext.filters


def ensure_pymongo_imports():
    global MongoClient, PyMongoError
    if MongoClient is not None and PyMongoError is not Exception:
        return
    pymongo = importlib.import_module("pymongo")
    pymongo_errors = importlib.import_module("pymongo.errors")
    MongoClient = pymongo.MongoClient
    PyMongoError = pymongo_errors.PyMongoError


# MongoDB
MONGO_URL = os.environ.get("MONGO_URL", "").strip()
client = None
db = None
movies_col = None
SERVICE_UNAVAILABLE_TEXT = "Serverda vaqtincha muammo bor. Keyinroq yana urinib ko'ring."


def reset_db_connection():
    global client, db, movies_col
    if client is not None:
        try:
            client.close()
        except Exception:
            pass
    client = None
    db = None
    movies_col = None
    set_health_state(db="disconnected")


def get_movies_col():
    global client, db, movies_col
    if movies_col is not None:
        return movies_col
    ensure_pymongo_imports()
    if not MONGO_URL:
        set_health_state(db="error", last_error="MONGO_URL topilmadi")
        raise RuntimeError("MONGO_URL topilmadi.")
    try:
        client = MongoClient(
            MONGO_URL,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=5000,
        )
        client.admin.command("ping")
        db = client["moviebot"]
        movies_col = db["movies"]
        set_health_state(db="connected", last_error="")
        return movies_col
    except Exception as exc:
        reset_db_connection()
        set_health_state(db="error", last_error=f"MongoDB: {exc}")
        raise


def run_db(operation):
    col = get_movies_col()
    try:
        return operation(col)
    except PyMongoError as exc:
        reset_db_connection()
        set_health_state(db="error", last_error=f"MongoDB: {exc}")
        raise


def save_movie(code, data):
    run_db(lambda col: col.update_one({"code": code}, {"$set": {**data, "code": code}}, upsert=True))


def delete_movie_db(code):
    run_db(lambda col: col.delete_one({"code": code}))


def movie_exists(code):
    return run_db(lambda col: col.find_one({"code": code}) is not None)


def get_movie(code):
    doc = run_db(lambda col: col.find_one({"code": code}))
    if not doc:
        return None
    return {
        "type": doc["type"],
        "file_id": doc["file_id"],
        "nom": doc.get("nom", "-"),
        "sifat": doc.get("sifat", "-"),
        "til": doc.get("til", "-"),
        "vaqt": doc.get("vaqt", "-"),
    }


KOD, NOM, SIFAT, TIL, VAQT = range(5)


async def log_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Telegram handler error", exc_info=context.error)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Salom! Movie HD botiga xush kelibsiz!\nKino kodini yozing."
    )


async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Bu komanda mavjud emas. Kino kodini yozing.")


async def reply_service_unavailable(update: Update):
    if update.message:
        await update.message.reply_text(SERVICE_UNAVAILABLE_TEXT)


async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return
    context.user_data["file_id"] = update.message.video.file_id
    context.user_data["file_type"] = "video"
    await update.message.reply_text("Video qabul qilindi.\n\nKino raqamini kiriting:")
    return KOD


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return
    context.user_data["file_id"] = update.message.document.file_id
    context.user_data["file_type"] = "document"
    await update.message.reply_text("Fayl qabul qilindi.\n\nKino raqamini kiriting:")
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
    try:
        save_movie(code, data)
    except Exception:
        logger.exception("Kinoni saqlashda xato yuz berdi")
        await reply_service_unavailable(update)
        return ConversationHandler.END
    await update.message.reply_text(
        f"Saqlandi.\n\n"
        f"Kod: {code}\n"
        f"Nom: {d['nom']}\n"
        f"Sifat: {d['sifat']}\n"
        f"Til: {d['til']}\n"
        f"Davomiylik: {d['vaqt']}"
    )
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Bekor qilindi.")
    return ConversationHandler.END


async def delete_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Ishlatish: /delete <kod>")
        return
    code = context.args[0]
    try:
        exists = movie_exists(code)
    except Exception:
        logger.exception("DB tekshiruvda xato")
        await reply_service_unavailable(update)
        return
    if not exists:
        await update.message.reply_text(f"{code} kodli kino topilmadi.")
        return
    try:
        delete_movie_db(code)
    except Exception:
        logger.exception("O'chirishda xato")
        await reply_service_unavailable(update)
        return
    await update.message.reply_text(f"{code} kodli kino o'chirildi.")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("Kino kodini yozing.")
        return
    code = text
    try:
        data = get_movie(code)
    except Exception:
        logger.exception("Kinoni qidirishda xato")
        await reply_service_unavailable(update)
        return
    if not data:
        await update.message.reply_text(f"{code} kodli kino topilmadi.")
        return
    caption = (
        f"{data.get('nom', '-')}\n"
        f"Sifat: {data.get('sifat', '-')}\n"
        f"Til: {data.get('til', '-')}\n"
        f"Davomiylik: {data.get('vaqt', '-')}\n"
        f"Kod: {code}"
    )
    file_id = data["file_id"]
    if data["type"] == "video":
        await update.message.reply_video(video=file_id, caption=caption)
    else:
        await update.message.reply_document(document=file_id, caption=caption)


def build_application():
    ensure_telegram_imports()
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN topilmadi.")

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_error_handler(log_error)

    conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.VIDEO & filters.User(ADMIN_ID), handle_video),
            MessageHandler(filters.Document.ALL & filters.User(ADMIN_ID), handle_document),
        ],
        states={
            KOD: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_kod)],
            NOM: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_nom)],
            SIFAT: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_sifat)],
            TIL: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_til)],
            VAQT: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_vaqt)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("delete", delete_movie))
    app.add_handler(conv)
    app.add_handler(MessageHandler(filters.COMMAND, unknown_command))
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.User(ADMIN_ID), handle_message)
    )
    return app


def run_bot_forever():
    global _shutdown_requested

    # SIGTERM kelganda o'chirmasdan restart qilish uchun ignore qilamiz
    # Render.com deploy paytida SIGTERM yuboradi — biz uni ushlab qayta ishga tushiramiz
    def handle_sigterm(signum, frame):
        logger.warning("SIGTERM qabul qilindi — bot qayta ishga tushadi...")
        # _shutdown_requested = False — bot loop davom etaveradi

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    while True:
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            set_health_state(bot="starting", last_error="")
            app = build_application()
            logger.info("Bot ishga tushdi...")
            set_health_state(bot="running", last_error="")
            app.run_polling(
                bootstrap_retries=-1,
                drop_pending_updates=True,   # eski xabarlarni o'tkazib yuboradi
                close_loop=False,
            )
            logger.warning("Polling to'xtadi. 3 soniyadan keyin qayta ishga tushadi...")
            set_health_state(bot="restarting")
        except Exception as exc:
            logger.exception("Bot xatosi. 3 soniyadan keyin qayta urinish...")
            set_health_state(bot="error", last_error=str(exc))
        finally:
            try:
                loop.close()
            except Exception:
                pass
        time.sleep(3)


def main():
    keep_alive()
    set_health_state(service="running", bot="starting", db="unknown", last_error="")
    run_bot_forever()


if __name__ == "__main__":
    main()
