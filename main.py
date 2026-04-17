from __future__ import annotations

import asyncio
import importlib
import logging
import os
import time
from typing import TYPE_CHECKING

try:
    from keep_alive import keep_alive, set_health_state
except ImportError:
    from .keep_alive import keep_alive, set_health_state

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.environ.get("ADMIN_ID", "6102256074"))
DEFAULT_INSTAGRAM_URL = "https://www.instagram.com/kinotop.bot/"
INSTAGRAM_CHANNEL_URL = os.environ.get("INSTAGRAM_CHANNEL_URL", "").strip() or DEFAULT_INSTAGRAM_URL

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
InlineKeyboardButton = None
InlineKeyboardMarkup = None
ReplyKeyboardMarkup = None
ReplyKeyboardRemove = None
MongoClient = None
PyMongoError = Exception


def ensure_telegram_imports():
    global ApplicationBuilder, CommandHandler, ContextTypes, ConversationHandler, MessageHandler, filters
    global InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove
    if ApplicationBuilder is not None:
        return

    telegram = importlib.import_module("telegram")
    telegram_ext = importlib.import_module("telegram.ext")
    InlineKeyboardButton = telegram.InlineKeyboardButton
    InlineKeyboardMarkup = telegram.InlineKeyboardMarkup
    ReplyKeyboardMarkup = telegram.ReplyKeyboardMarkup
    ReplyKeyboardRemove = telegram.ReplyKeyboardRemove
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

# MongoDB ulanish
MONGO_URL = os.environ.get("MONGO_URL", "").strip()
client = None
db = None
movies_col = None
users_col = None
SERVICE_UNAVAILABLE_TEXT = "Serverda vaqtincha muammo bor. Keyinroq yana urinib ko'ring."
DEFAULT_SIFAT = os.environ.get("DEFAULT_SIFAT", "720p").strip() or "720p"
DEFAULT_TIL = os.environ.get("DEFAULT_TIL", "O'zbek").strip() or "O'zbek"
DEFAULT_VAQT = os.environ.get("DEFAULT_VAQT", "-").strip() or "-"
KEEP_PREVIOUS_TEXT = "♻️ Oldingisini qoldirish"
CONFIRM_SAVE_TEXT = "✅ Saqlash"
CONFIRM_CANCEL_TEXT = "❌ Bekor qilish"


def reset_db_connection():
    global client, db, movies_col, users_col
    if client is not None:
        try:
            client.close()
        except Exception:
            pass
    client = None
    db = None
    movies_col = None
    users_col = None
    set_health_state(db="disconnected")


def get_movies_col():
    global client, db, movies_col, users_col
    if movies_col is not None:
        return movies_col
    ensure_pymongo_imports()
    if not MONGO_URL:
        set_health_state(db="error", last_error="MONGO_URL topilmadi")
        raise RuntimeError("MONGO_URL topilmadi. Environment variable sifatida sozlang.")
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
        users_col = db["users"]
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


def get_users_col():
    global users_col
    if users_col is not None:
        return users_col
    get_movies_col()
    return users_col


def run_users_db(operation):
    col = get_users_col()
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


def get_movie_by_file_id(file_id):
    doc = run_db(lambda col: col.find_one({"file_id": file_id}, {"code": 1, "nom": 1, "_id": 0}))
    if not doc:
        return None
    return {
        "code": doc.get("code", "-"),
        "nom": doc.get("nom", "-"),
    }


def get_next_movie_code():
    def operation(col):
        pipeline = [
            {
                "$addFields": {
                    "code_num": {
                        "$convert": {"input": "$code", "to": "int", "onError": None, "onNull": None}
                    }
                }
            },
            {"$match": {"code_num": {"$ne": None}}},
            {"$sort": {"code_num": -1}},
            {"$limit": 1},
        ]
        latest = next(col.aggregate(pipeline), None)
        next_code = (latest["code_num"] + 1) if latest else 1
        return str(next_code)

    return run_db(operation)


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


def track_user(user):
    if user is None:
        return

    run_users_db(
        lambda col: col.update_one(
            {"user_id": user.id},
            {
                "$set": {
                    "user_id": user.id,
                    "username": user.username or "",
                    "first_name": user.first_name or "",
                    "last_name": user.last_name or "",
                    "is_admin": user.id == ADMIN_ID,
                    "last_seen_at": int(time.time()),
                }
            },
            upsert=True,
        )
    )


def get_tracked_user_count():
    return run_users_db(lambda col: col.count_documents({"is_admin": {"$ne": True}}))


def remember_user(update):
    user = update.effective_user
    if user is None:
        return
    try:
        track_user(user)
    except Exception:
        logger.exception("Foydalanuvchini saqlashda xato yuz berdi")


def get_sifat_keyboard():
    return ReplyKeyboardMarkup(
        [["1080p", "720p", "480p"], [KEEP_PREVIOUS_TEXT]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def get_til_keyboard():
    return ReplyKeyboardMarkup(
        [["O'zbek", "Rus", "Ingliz"], [KEEP_PREVIOUS_TEXT]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def get_vaqt_keyboard():
    return ReplyKeyboardMarkup(
        [[KEEP_PREVIOUS_TEXT]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def get_confirm_keyboard():
    return ReplyKeyboardMarkup(
        [[CONFIRM_SAVE_TEXT, CONFIRM_CANCEL_TEXT]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def resolve_input_value(raw_value, previous_value, fallback_value):
    value = raw_value.strip()
    if value == KEEP_PREVIOUS_TEXT:
        return previous_value or fallback_value
    return value or previous_value or fallback_value


NOM, SIFAT, TIL, VAQT, CONFIRM = range(5)
EDIT_KOD, EDIT_NOM, EDIT_SIFAT, EDIT_TIL, EDIT_VAQT = range(5, 10)


async def log_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Telegram handler error", exc_info=context.error)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remember_user(update)
    await update.message.reply_text(
        "🎬 Salom! Movie HD botiga xush kelibsiz!\nKino kodini yozing."
    )


async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❓ Bu komanda mavjud emas. 🎬 Kino kodini yozing.")


async def reply_service_unavailable(update: Update):
    if update.message:
        await update.message.reply_text(SERVICE_UNAVAILABLE_TEXT)


async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return
    context.user_data["file_id"] = update.message.video.file_id
    context.user_data["file_type"] = "video"
    try:
        existing_movie = get_movie_by_file_id(context.user_data["file_id"])
    except Exception:
        logger.exception("Dublikat videoni tekshirishda xato yuz berdi")
        await reply_service_unavailable(update)
        return ConversationHandler.END
    if existing_movie:
        await update.message.reply_text(
            f"⚠️ Bu fayl allaqachon bazada bor.\n"
            f"🆔 Kod: {existing_movie['code']}\n"
            f"🎬 Nom: {existing_movie['nom']}\n\n"
            f"Boshqa kino faylini yuboring."
        )
        return ConversationHandler.END
    try:
        next_code = get_next_movie_code()
    except Exception:
        logger.exception("Keyingi kino kodini olishda xato yuz berdi")
        await reply_service_unavailable(update)
        return ConversationHandler.END
    context.user_data["kod"] = next_code
    await update.message.reply_text(
        f"🎥 Video qabul qilindi.\n\n🆔 Kod avtomatik biriktirildi: {next_code}\n🎬 Kino nomini kiriting:"
    )
    return NOM


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return
    context.user_data["file_id"] = update.message.document.file_id
    context.user_data["file_type"] = "document"
    try:
        existing_movie = get_movie_by_file_id(context.user_data["file_id"])
    except Exception:
        logger.exception("Dublikat faylni tekshirishda xato yuz berdi")
        await reply_service_unavailable(update)
        return ConversationHandler.END
    if existing_movie:
        await update.message.reply_text(
            f"⚠️ Bu fayl allaqachon bazada bor.\n"
            f"🆔 Kod: {existing_movie['code']}\n"
            f"🎬 Nom: {existing_movie['nom']}\n\n"
            f"Boshqa kino faylini yuboring."
        )
        return ConversationHandler.END
    try:
        next_code = get_next_movie_code()
    except Exception:
        logger.exception("Keyingi kino kodini olishda xato yuz berdi")
        await reply_service_unavailable(update)
        return ConversationHandler.END
    context.user_data["kod"] = next_code
    await update.message.reply_text(
        f"📄 Fayl qabul qilindi.\n\n🆔 Kod avtomatik biriktirildi: {next_code}\n🎬 Kino nomini kiriting:"
    )
    return NOM


async def get_nom(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["nom"] = update.message.text.strip()
    previous_sifat = context.user_data.get("last_sifat", DEFAULT_SIFAT)
    await update.message.reply_text(
        f"🎥 Sifatni tanlang yoki yozing.\n"
        f"♻️ Oldingi qiymat: {previous_sifat}",
        reply_markup=get_sifat_keyboard(),
    )
    return SIFAT


async def get_sifat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    previous_sifat = context.user_data.get("last_sifat", DEFAULT_SIFAT)
    context.user_data["sifat"] = resolve_input_value(update.message.text, previous_sifat, DEFAULT_SIFAT)
    previous_til = context.user_data.get("last_til", DEFAULT_TIL)
    await update.message.reply_text(
        f"🌐 Tilni tanlang yoki yozing.\n"
        f"♻️ Oldingi qiymat: {previous_til}",
        reply_markup=get_til_keyboard(),
    )
    return TIL


async def get_til(update: Update, context: ContextTypes.DEFAULT_TYPE):
    previous_til = context.user_data.get("last_til", DEFAULT_TIL)
    context.user_data["til"] = resolve_input_value(update.message.text, previous_til, DEFAULT_TIL)
    previous_vaqt = context.user_data.get("last_vaqt", DEFAULT_VAQT)
    await update.message.reply_text(
        f"⏱️ Davomiylikni kiriting (masalan: 1:57:36).\n"
        f"♻️ Oldingi qiymat: {previous_vaqt}",
        reply_markup=get_vaqt_keyboard(),
    )
    return VAQT


async def get_vaqt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    d = context.user_data
    previous_vaqt = d.get("last_vaqt", DEFAULT_VAQT)
    d["vaqt"] = resolve_input_value(update.message.text, previous_vaqt, DEFAULT_VAQT)
    await update.message.reply_text(
        f"📋 Tekshirib chiqing:\n\n"
        f"🆔 Kod: {d['kod']}\n"
        f"🎬 Nom: {d['nom']}\n"
        f"🎥 Sifat: {d['sifat']}\n"
        f"🌐 Til: {d['til']}\n"
        f"⏱️ Davomiylik: {d['vaqt']}\n\n"
        f"Saqlaymizmi?",
        reply_markup=get_confirm_keyboard(),
    )
    return CONFIRM


async def confirm_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    choice = update.message.text.strip()
    if choice == CONFIRM_CANCEL_TEXT:
        await update.message.reply_text("❌ Bekor qilindi.", reply_markup=ReplyKeyboardRemove())
        return ConversationHandler.END
    if choice != CONFIRM_SAVE_TEXT:
        await update.message.reply_text("Iltimos, tugmadan birini tanlang: ✅ Saqlash yoki ❌ Bekor qilish.")
        return CONFIRM

    d = context.user_data
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
    d["last_sifat"] = d["sifat"]
    d["last_til"] = d["til"]
    d["last_vaqt"] = d["vaqt"]
    await update.message.reply_text(
        f"✅ Saqlandi.\n\n"
        f"🆔 Kod: {code}\n"
        f"🎬 Nom: {d['nom']}\n"
        f"🎥 Sifat: {d['sifat']}\n"
        f"🌐 Til: {d['til']}\n"
        f"⏱️ Davomiylik: {d['vaqt']}\n\n"
        f"Keyingi kino uchun yana video yoki fayl yuboring.",
        reply_markup=ReplyKeyboardRemove(),
    )
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Bekor qilindi.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END


async def edit_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remember_user(update)
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return
    await update.message.reply_text("✏️ Tahrirlash uchun kino kodini kiriting:")
    return EDIT_KOD


async def edit_get_kod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    code = update.message.text.strip()
    try:
        data = get_movie(code)
    except Exception:
        logger.exception("Kinoni qidirishda xato yuz berdi")
        await reply_service_unavailable(update)
        return ConversationHandler.END
    if not data:
        await update.message.reply_text(f"❌ {code} kodli kino topilmadi.")
        return ConversationHandler.END
    context.user_data['edit_code'] = code
    context.user_data['current_data'] = data
    await update.message.reply_text(
        f"📋 Joriy ma'lumotlar:\n"
        f"🎬 Nom: {data['nom']}\n"
        f"🎥 Sifat: {data['sifat']}\n"
        f"🌐 Til: {data['til']}\n"
        f"⏱️ Davomiylik: {data['vaqt']}\n\n"
        f"🎬 Yangi nomni kiriting (bo'sh qoldiring saqlash uchun):"
    )
    return EDIT_NOM


async def edit_get_nom(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_nom = update.message.text.strip()
    if new_nom:
        context.user_data['nom'] = new_nom
    else:
        context.user_data['nom'] = context.user_data['current_data']['nom']
    await update.message.reply_text("🎥 Yangi sifatni kiriting (bo'sh qoldiring saqlash uchun):")
    return EDIT_SIFAT


async def edit_get_sifat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_sifat = update.message.text.strip()
    if new_sifat:
        context.user_data['sifat'] = new_sifat
    else:
        context.user_data['sifat'] = context.user_data['current_data']['sifat']
    await update.message.reply_text("🌐 Yangi tilni kiriting (bo'sh qoldiring saqlash uchun):")
    return EDIT_TIL


async def edit_get_til(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_til = update.message.text.strip()
    if new_til:
        context.user_data['til'] = new_til
    else:
        context.user_data['til'] = context.user_data['current_data']['til']
    await update.message.reply_text("⏱️ Yangi davomiylikni kiriting (bo'sh qoldiring saqlash uchun):")
    return EDIT_VAQT


async def edit_get_vaqt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_vaqt = update.message.text.strip()
    if new_vaqt:
        context.user_data['vaqt'] = new_vaqt
    else:
        context.user_data['vaqt'] = context.user_data['current_data']['vaqt']
    d = context.user_data
    code = d['edit_code']
    data = {
        "type": d['current_data']['type'],
        "file_id": d['current_data']['file_id'],
        "nom": d['nom'],
        "sifat": d['sifat'],
        "til": d['til'],
        "vaqt": d['vaqt'],
    }
    try:
        save_movie(code, data)
    except Exception:
        logger.exception("Kinoni tahrirlashda xato yuz berdi")
        await reply_service_unavailable(update)
        return ConversationHandler.END
    await update.message.reply_text("✅ Tahrirlandi!")
    return ConversationHandler.END


async def delete_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("📝 Ishlatish: /delete <kod>")
        return
    code = context.args[0]
    try:
        exists = movie_exists(code)
    except Exception:
        logger.exception("Kinoni o'chirish uchun DB tekshiruvda xato yuz berdi")
        await reply_service_unavailable(update)
        return
    if not exists:
        await update.message.reply_text(f"❌ {code} kodli kino topilmadi.")
        return
    try:
        delete_movie_db(code)
    except Exception:
        logger.exception("Kinoni o'chirishda xato yuz berdi")
        await reply_service_unavailable(update)
        return
    await update.message.reply_text(f"🗑️ {code} kodli kino o'chirildi.")


async def show_user_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return
    if not context.args or context.args[0] != "777":
        await update.message.reply_text("📝 Ishlatish: /foydalanuvchi 777")
        return
    try:
        total_users = get_tracked_user_count()
    except Exception:
        logger.exception("Foydalanuvchilar sonini olishda xato yuz berdi")
        await reply_service_unavailable(update)
        return
    await update.message.reply_text(f"👥 Foydalanuvchilar soni: {total_users}")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remember_user(update)
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("🎬 Kino kodini yozing.")
        return

    code = text
    try:
        data = get_movie(code)
    except Exception:
        logger.exception("Kinoni qidirishda xato yuz berdi")
        await reply_service_unavailable(update)
        return
    if not data:
        await update.message.reply_text(f"❌ {code} kodli kino topilmadi.")
        return

    caption = (
        f"🎬 {data.get('nom', '-')}\n"
        f"🎥 Sifat: {data.get('sifat', '-')}\n"
        f"🌐 Til: {data.get('til', '-')}\n"
        f"⏱️ Davomiylik: {data.get('vaqt', '-')}\n"
        f"🆔 Kod: {code}"
    )
    reply_markup = None
    if INSTAGRAM_CHANNEL_URL:
        reply_markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton("Qolgan kino kodlarini ko'rish uchun bosing", url=INSTAGRAM_CHANNEL_URL)]]
        )
    file_id = data["file_id"]
    if data["type"] == "video":
        await update.message.reply_video(video=file_id, caption=caption, reply_markup=reply_markup)
    else:
        await update.message.reply_document(document=file_id, caption=caption, reply_markup=reply_markup)

    # Ba'zi Telegram klientlarida media xabaridagi tugma chiqmasligi mumkin.
    # Shu sabab tugmani alohida xabarda ham yuboramiz.
    if reply_markup is not None:
        await update.message.reply_text(
            "Qolgan kino kodlari uchun quyidagi tugmani bosing:",
            reply_markup=reply_markup,
        )


def build_application():
    ensure_telegram_imports()
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN topilmadi. Environment variable sifatida sozlang.")

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_error_handler(log_error)

    conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.VIDEO & filters.User(ADMIN_ID), handle_video),
            MessageHandler(filters.Document.ALL & filters.User(ADMIN_ID), handle_document),
        ],
        states={
            NOM: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_nom)],
            SIFAT: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_sifat)],
            TIL: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_til)],
            VAQT: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_vaqt)],
            CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), confirm_save)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    edit_conv = ConversationHandler(
        entry_points=[CommandHandler("edit", edit_start)],
        states={
            EDIT_KOD: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), edit_get_kod)],
            EDIT_NOM: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), edit_get_nom)],
            EDIT_SIFAT: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), edit_get_sifat)],
            EDIT_TIL: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), edit_get_til)],
            EDIT_VAQT: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), edit_get_vaqt)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("delete", delete_movie))
    app.add_handler(CommandHandler("foydalanuvchi", show_user_count))
    app.add_handler(conv)
    app.add_handler(edit_conv)
    app.add_handler(MessageHandler(filters.COMMAND, unknown_command))
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.User(ADMIN_ID), handle_message)
    )
    return app


def run_bot_forever():
    while True:
        try:
            asyncio.set_event_loop(asyncio.new_event_loop())
            set_health_state(bot="starting", last_error="")
            app = build_application()
            logger.info("Bot ishga tushdi...")
            set_health_state(bot="running", last_error="")
            app.run_polling(bootstrap_retries=-1)
            logger.warning("Polling to'xtadi. 5 soniyadan keyin qayta ishga tushadi.")
            set_health_state(bot="stopped")
        except Exception as exc:
            logger.exception("Bot ishida xato yuz berdi. 5 soniyadan keyin qayta urinish bo'ladi.")
            set_health_state(bot="error", last_error=str(exc))
        time.sleep(5)


def main():
    if os.environ.get("PORT"):
        keep_alive()
        set_health_state(service="running", bot="starting", db="unknown", last_error="")
    else:
        logger.info("PORT topilmadi. Bot worker rejimida ishga tushmoqda.")
        set_health_state(service="worker", bot="starting", db="unknown", last_error="")

    run_bot_forever()


if __name__ == "__main__":
    main()
