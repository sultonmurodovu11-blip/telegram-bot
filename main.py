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
CallbackQueryHandler = None
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
    global ApplicationBuilder, CallbackQueryHandler, CommandHandler, ContextTypes
    global ConversationHandler, MessageHandler, filters
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
    CallbackQueryHandler = telegram_ext.CallbackQueryHandler
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
series_col = None
folders_col = None
users_col = None
SERVICE_UNAVAILABLE_TEXT = "Serverda vaqtincha muammo bor. Keyinroq yana urinib ko'ring."
DEFAULT_SIFAT = os.environ.get("DEFAULT_SIFAT", "720p").strip() or "720p"
DEFAULT_TIL = os.environ.get("DEFAULT_TIL", "O'zbek").strip() or "O'zbek"
DEFAULT_VAQT = os.environ.get("DEFAULT_VAQT", "-").strip() or "-"
KEEP_PREVIOUS_TEXT = "♻️ Oldingisini qoldirish"
CONFIRM_SAVE_TEXT = "✅ Saqlash"
CONFIRM_CANCEL_TEXT = "❌ Bekor qilish"
FOLDER_SKIP_TEXT = "❌ Yo'q, oddiy saqlash"
FOLDER_CREATE_TEXT = "🆕 Yangi jild yaratish"
FOLDER_ADD_EXISTING_TEXT = "📂 Mavjud jildga qo'shish"
FOLDER_BACK_TEXT = "🔙 Orqaga"
DURATION_CONFIRM_TEXT = "✅ Vaqtni tasdiqlash"
DURATION_BACKSPACE_TEXT = "⌫ O'chirish"
DURATION_CLEAR_TEXT = "🧹 Tozalash"
DURATION_ALLOWED_CHARS = set("0123456789:")
DURATION_MAX_LENGTH = 10
SERIES_CALLBACK_PREFIX = "series_part:"


def reset_db_connection():
    global client, db, movies_col, series_col, folders_col, users_col
    if client is not None:
        try:
            client.close()
        except Exception:
            pass
    client = None
    db = None
    movies_col = None
    series_col = None
    folders_col = None
    users_col = None
    set_health_state(db="disconnected")


def get_movies_col():
    global client, db, movies_col, series_col, folders_col, users_col
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
        series_col = db["series_groups"]
        folders_col = db["movie_folders"]
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


def get_series_col():
    global series_col
    if series_col is not None:
        return series_col
    get_movies_col()
    return series_col


def run_series_db(operation):
    col = get_series_col()
    try:
        return operation(col)
    except PyMongoError as exc:
        reset_db_connection()
        set_health_state(db="error", last_error=f"MongoDB: {exc}")
        raise


def get_folders_col():
    global folders_col
    if folders_col is not None:
        return folders_col
    get_movies_col()
    return folders_col


def run_folders_db(operation):
    col = get_folders_col()
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


def get_last_and_next_movie_code():
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
        if latest:
            last_code_num = int(latest["code_num"])
            return str(last_code_num), str(last_code_num + 1)
        return "yo'q", "1"

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


def parse_numeric_code(value):
    if not value or not value.isdigit():
        return None
    return int(value)


def save_series_range(start_code_num, end_code_num, title):
    run_series_db(
        lambda col: col.update_one(
            {"start_code_num": start_code_num, "end_code_num": end_code_num},
            {
                "$set": {
                    "start_code_num": start_code_num,
                    "end_code_num": end_code_num,
                    "title": title,
                }
            },
            upsert=True,
        )
    )


def delete_series_range(start_code_num, end_code_num):
    return run_series_db(
        lambda col: col.delete_one(
            {"start_code_num": start_code_num, "end_code_num": end_code_num}
        ).deleted_count
    )


def get_series_range_by_code(code):
    code_num = parse_numeric_code(code)
    if code_num is None:
        return None

    def operation(col):
        cursor = col.find(
            {
                "start_code_num": {"$lte": code_num},
                "end_code_num": {"$gte": code_num},
            },
            {"_id": 0},
        ).sort("start_code_num", 1).limit(1)
        return next(cursor, None)

    return run_series_db(operation)


def get_overlapping_series_ranges(start_code_num, end_code_num):
    def operation(col):
        cursor = col.find(
            {
                "start_code_num": {"$lte": end_code_num},
                "end_code_num": {"$gte": start_code_num},
            },
            {"_id": 0},
        ).sort("start_code_num", 1)
        return list(cursor)

    return run_series_db(operation)


def get_all_series_ranges():
    return run_series_db(lambda col: list(col.find({}, {"_id": 0}).sort("start_code_num", 1)))


def get_movies_in_range(start_code_num, end_code_num):
    def operation(col):
        pipeline = [
            {
                "$addFields": {
                    "code_num": {
                        "$convert": {"input": "$code", "to": "int", "onError": None, "onNull": None}
                    }
                }
            },
            {
                "$match": {
                    "code_num": {
                        "$ne": None,
                        "$gte": start_code_num,
                        "$lte": end_code_num,
                    }
                }
            },
            {"$sort": {"code_num": 1}},
            {"$project": {"_id": 0, "code": 1, "nom": 1, "code_num": 1}},
        ]
        return list(col.aggregate(pipeline))

    return run_db(operation)


def get_all_folder_names():
    return run_folders_db(lambda col: [item["name"] for item in col.find({}, {"_id": 0, "name": 1}).sort("name", 1)])


def folder_exists_by_name(name):
    return run_folders_db(lambda col: col.find_one({"name": name}, {"_id": 1}) is not None)


def get_folder_by_code(code):
    return run_folders_db(lambda col: col.find_one({"codes": code}, {"_id": 0, "name": 1, "codes": 1}))


def add_movie_to_folder(folder_name, code):
    run_folders_db(
        lambda col: col.update_one(
            {"name": folder_name},
            {
                "$set": {"name": folder_name},
                "$addToSet": {"codes": code},
                "$setOnInsert": {"created_at": int(time.time())},
            },
            upsert=True,
        )
    )


def sort_codes_for_folder(codes):
    numeric = []
    non_numeric = []
    for code in codes:
        parsed = parse_numeric_code(code)
        if parsed is None:
            non_numeric.append(code)
        else:
            numeric.append((parsed, code))
    numeric.sort(key=lambda item: item[0])
    non_numeric.sort()
    return [item[1] for item in numeric] + non_numeric


def get_movies_for_folder(folder_name):
    folder = run_folders_db(lambda col: col.find_one({"name": folder_name}, {"_id": 0, "codes": 1}))
    if not folder:
        return []
    codes = sort_codes_for_folder(folder.get("codes", []))
    if not codes:
        return []
    code_to_movie = {
        movie["code"]: movie
        for movie in run_db(lambda col: list(col.find({"code": {"$in": codes}}, {"_id": 0, "code": 1, "nom": 1})))
    }
    return [code_to_movie[code] for code in codes if code in code_to_movie]


def get_instagram_reply_markup():
    if not INSTAGRAM_CHANNEL_URL:
        return None
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("Qolgan kino kodlarini ko'rish uchun bosing", url=INSTAGRAM_CHANNEL_URL)]]
    )


def build_movie_caption(code, data):
    return (
        f"🎬 {data.get('nom', '-')}\n"
        f"🎥 Sifat: {data.get('sifat', '-')}\n"
        f"🌐 Til: {data.get('til', '-')}\n"
        f"⏱️ Davomiylik: {data.get('vaqt', '-')}\n"
        f"🆔 Kod: {code}"
    )


def get_series_parts_keyboard(series_data, movies):
    rows = []
    row = []
    start_code_num = series_data["start_code_num"]

    for movie in movies:
        part_number = (movie["code_num"] - start_code_num) + 1
        row.append(
            InlineKeyboardButton(
                f"{part_number}-qism",
                callback_data=f"{SERIES_CALLBACK_PREFIX}{movie['code']}",
            )
        )
        if len(row) == 3:
            rows.append(row)
            row = []

    if row:
        rows.append(row)

    return InlineKeyboardMarkup(rows)


def get_folder_parts_keyboard(movies):
    rows = []
    row = []
    for index, movie in enumerate(movies, start=1):
        row.append(
            InlineKeyboardButton(
                f"{index}-qism",
                callback_data=f"{SERIES_CALLBACK_PREFIX}{movie['code']}",
            )
        )
        if len(row) == 3:
            rows.append(row)
            row = []

    if row:
        rows.append(row)

    return InlineKeyboardMarkup(rows)


async def send_movie_to_chat(target_message, code, data):
    caption = build_movie_caption(code, data)
    reply_markup = get_instagram_reply_markup()
    file_id = data["file_id"]

    if data["type"] == "video":
        await target_message.reply_video(video=file_id, caption=caption, reply_markup=reply_markup)
    else:
        await target_message.reply_document(document=file_id, caption=caption, reply_markup=reply_markup)


async def send_series_parts_prompt(target_message, series_data, movies):
    await target_message.reply_text(
        f"🎞️ {series_data['title']}\n"
        f"🧾 Kodlar oralig'i: {series_data['start_code_num']} - {series_data['end_code_num']}\n"
        "Kerakli qismni tanlang:",
        reply_markup=get_series_parts_keyboard(series_data, movies),
    )


async def send_folder_parts_prompt(target_message, folder_data, movies):
    await target_message.reply_text(
        f"🎞️ {folder_data['name']}\n"
        f"📚 Qismlar soni: {len(movies)}\n"
        "Kerakli qismni tanlang:",
        reply_markup=get_folder_parts_keyboard(movies),
    )


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
        [
            ["1", "2", "3"],
            ["4", "5", "6"],
            ["7", "8", "9"],
            [":", "0", DURATION_BACKSPACE_TEXT],
            [DURATION_CLEAR_TEXT, DURATION_CONFIRM_TEXT],
            [KEEP_PREVIOUS_TEXT],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


def get_confirm_keyboard():
    return ReplyKeyboardMarkup(
        [[CONFIRM_SAVE_TEXT, CONFIRM_CANCEL_TEXT]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def get_folder_choice_keyboard():
    return ReplyKeyboardMarkup(
        [
            [FOLDER_SKIP_TEXT],
            [FOLDER_CREATE_TEXT, FOLDER_ADD_EXISTING_TEXT],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def build_folder_list_keyboard(folder_names):
    rows = []
    row = []
    for name in folder_names:
        row.append(name)
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([FOLDER_BACK_TEXT, FOLDER_SKIP_TEXT])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=True)


def resolve_input_value(raw_value, previous_value, fallback_value):
    value = raw_value.strip()
    if value == KEEP_PREVIOUS_TEXT:
        return previous_value or fallback_value
    return value or previous_value or fallback_value


def append_duration_char(current_value, char):
    if char not in DURATION_ALLOWED_CHARS:
        return current_value
    if len(current_value) >= DURATION_MAX_LENGTH:
        return current_value
    if char == ":":
        if not current_value or current_value.endswith(":"):
            return current_value
        if current_value.count(":") >= 2:
            return current_value
    return current_value + char


def is_duration_format_valid(value):
    parts = value.split(":")
    if len(parts) != 3:
        return False
    hours, minutes, seconds = parts
    if not (hours.isdigit() and minutes.isdigit() and seconds.isdigit()):
        return False
    if not (1 <= len(hours) <= 3 and len(minutes) == 2 and len(seconds) == 2):
        return False
    return 0 <= int(minutes) < 60 and 0 <= int(seconds) < 60


def get_duration_input_text(draft_value, previous_value):
    current_text = draft_value if draft_value else "-"
    return (
        "⏱️ Davomiylikni tugmalar bilan kiriting (masalan: 1:57:36).\n"
        "🔢 Raqamlar va ':' tugmasini bosib kiriting.\n"
        f"♻️ Oldingi qiymat: {previous_value}\n"
        f"📝 Joriy: {current_text}\n"
        f"✅ Tayyor bo'lsa {DURATION_CONFIRM_TEXT} tugmasini bosing."
    )


NOM, SIFAT, TIL, VAQT, CONFIRM, FOLDER_CHOICE, FOLDER_CREATE, FOLDER_PICK = range(8)
EDIT_KOD, EDIT_NOM, EDIT_SIFAT, EDIT_TIL, EDIT_VAQT = range(8, 13)


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
    elif update.callback_query and update.callback_query.message:
        await update.callback_query.message.reply_text(SERVICE_UNAVAILABLE_TEXT)


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
        last_code, next_code = get_last_and_next_movie_code()
    except Exception:
        logger.exception("Keyingi kino kodini olishda xato yuz berdi")
        await reply_service_unavailable(update)
        return ConversationHandler.END
    context.user_data["kod"] = next_code
    await update.message.reply_text(
        f"🎥 Video qabul qilindi.\n\n"
        f"🧾 Oxirgi kino kodi: {last_code}\n"
        f"🆔 Yangi kod avtomatik biriktirildi: {next_code}\n"
        f"🎬 Kino nomini kiriting:"
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
        last_code, next_code = get_last_and_next_movie_code()
    except Exception:
        logger.exception("Keyingi kino kodini olishda xato yuz berdi")
        await reply_service_unavailable(update)
        return ConversationHandler.END
    context.user_data["kod"] = next_code
    await update.message.reply_text(
        f"📄 Fayl qabul qilindi.\n\n"
        f"🧾 Oxirgi kino kodi: {last_code}\n"
        f"🆔 Yangi kod avtomatik biriktirildi: {next_code}\n"
        f"🎬 Kino nomini kiriting:"
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
    context.user_data["vaqt_draft"] = ""
    await update.message.reply_text(
        get_duration_input_text(context.user_data["vaqt_draft"], previous_vaqt),
        reply_markup=get_vaqt_keyboard(),
    )
    return VAQT


async def get_vaqt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    d = context.user_data
    previous_vaqt = d.get("last_vaqt", DEFAULT_VAQT)
    value = update.message.text.strip()
    draft_value = d.get("vaqt_draft", "")

    if value == KEEP_PREVIOUS_TEXT:
        d["vaqt"] = previous_vaqt
    elif value == DURATION_BACKSPACE_TEXT:
        d["vaqt_draft"] = draft_value[:-1]
        await update.message.reply_text(
            get_duration_input_text(d["vaqt_draft"], previous_vaqt),
            reply_markup=get_vaqt_keyboard(),
        )
        return VAQT
    elif value == DURATION_CLEAR_TEXT:
        d["vaqt_draft"] = ""
        await update.message.reply_text(
            get_duration_input_text(d["vaqt_draft"], previous_vaqt),
            reply_markup=get_vaqt_keyboard(),
        )
        return VAQT
    elif value == DURATION_CONFIRM_TEXT:
        if not draft_value:
            await update.message.reply_text(
                "⛔ Avval davomiylikni kiriting.",
                reply_markup=get_vaqt_keyboard(),
            )
            return VAQT
        if not is_duration_format_valid(draft_value):
            await update.message.reply_text(
                "⛔ Format noto'g'ri. To'g'ri misol: 1:57:36",
                reply_markup=get_vaqt_keyboard(),
            )
            return VAQT
        d["vaqt"] = draft_value
    elif len(value) == 1 and value in DURATION_ALLOWED_CHARS:
        updated_draft = append_duration_char(draft_value, value)
        if updated_draft == draft_value:
            await update.message.reply_text(
                "⛔ Bu belgini shu joyga qo'shib bo'lmaydi.",
                reply_markup=get_vaqt_keyboard(),
            )
            return VAQT
        d["vaqt_draft"] = updated_draft
        await update.message.reply_text(
            get_duration_input_text(d["vaqt_draft"], previous_vaqt),
            reply_markup=get_vaqt_keyboard(),
        )
        return VAQT
    else:
        if not is_duration_format_valid(value):
            await update.message.reply_text(
                "⛔ Noto'g'ri format. Masalan: 1:57:36\nYoki tugmalar bilan kiriting.",
                reply_markup=get_vaqt_keyboard(),
            )
            return VAQT
        d["vaqt"] = value

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
        "📁 Jildga saqlashni xohlaysizmi?",
        reply_markup=get_folder_choice_keyboard(),
    )
    return FOLDER_CHOICE


async def finish_movie_save(update: Update, context: ContextTypes.DEFAULT_TYPE, folder_note=None):
    d = context.user_data
    note_text = f"\n{folder_note}\n" if folder_note else "\n"
    await update.message.reply_text(
        f"✅ Saqlandi.\n\n"
        f"🆔 Kod: {d['kod']}\n"
        f"🎬 Nom: {d['nom']}\n"
        f"🎥 Sifat: {d['sifat']}\n"
        f"🌐 Til: {d['til']}\n"
        f"⏱️ Davomiylik: {d['vaqt']}\n"
        f"{note_text}"
        f"Keyingi kino uchun yana video yoki fayl yuboring.",
        reply_markup=ReplyKeyboardRemove(),
    )
    return ConversationHandler.END


def get_part_number_in_movies(movies, code):
    for idx, movie in enumerate(movies, start=1):
        if movie["code"] == code:
            return idx
    return None


async def save_to_folder_and_finish(update: Update, context: ContextTypes.DEFAULT_TYPE, folder_name):
    d = context.user_data
    code = d["kod"]
    try:
        add_movie_to_folder(folder_name, code)
        movies = get_movies_for_folder(folder_name)
    except Exception:
        logger.exception("Jildga kinoni saqlashda xato yuz berdi")
        await reply_service_unavailable(update)
        return ConversationHandler.END

    part_number = get_part_number_in_movies(movies, code)
    if part_number is None:
        folder_note = f"📂 Jild: {folder_name}"
    else:
        folder_note = f"📂 Jild: {folder_name}\n🔢 Qism: {part_number}/{len(movies)}"
    return await finish_movie_save(update, context, folder_note=folder_note)


async def handle_folder_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    choice = update.message.text.strip()
    if choice == FOLDER_SKIP_TEXT:
        return await finish_movie_save(update, context)

    if choice == FOLDER_CREATE_TEXT:
        await update.message.reply_text(
            "🆕 Yangi jild nomini yozing:",
            reply_markup=ReplyKeyboardRemove(),
        )
        return FOLDER_CREATE

    if choice == FOLDER_ADD_EXISTING_TEXT:
        try:
            folder_names = get_all_folder_names()
        except Exception:
            logger.exception("Jildlar ro'yxatini olishda xato yuz berdi")
            await reply_service_unavailable(update)
            return ConversationHandler.END
        if not folder_names:
            await update.message.reply_text(
                "📭 Hali jildlar yo'q. Avval yangi jild yarating yoki oddiy saqlang.",
                reply_markup=get_folder_choice_keyboard(),
            )
            return FOLDER_CHOICE
        await update.message.reply_text(
            "📂 Jildni tanlang:",
            reply_markup=build_folder_list_keyboard(folder_names),
        )
        return FOLDER_PICK

    await update.message.reply_text(
        "Iltimos, tugmalardan birini tanlang.",
        reply_markup=get_folder_choice_keyboard(),
    )
    return FOLDER_CHOICE


async def handle_folder_create(update: Update, context: ContextTypes.DEFAULT_TYPE):
    folder_name = update.message.text.strip()
    if not folder_name:
        await update.message.reply_text("⛔ Jild nomi bo'sh bo'lmasin. Qayta kiriting:")
        return FOLDER_CREATE
    try:
        exists = folder_exists_by_name(folder_name)
    except Exception:
        logger.exception("Jild nomini tekshirishda xato yuz berdi")
        await reply_service_unavailable(update)
        return ConversationHandler.END
    if exists:
        await update.message.reply_text(
            "⚠️ Bu nomli jild bor. Boshqa nom kiriting yoki mavjud jildga qo'shishdan foydalaning:"
        )
        return FOLDER_CREATE
    return await save_to_folder_and_finish(update, context, folder_name)


async def handle_folder_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    value = update.message.text.strip()
    if value == FOLDER_BACK_TEXT:
        await update.message.reply_text(
            "📁 Jildga saqlashni xohlaysizmi?",
            reply_markup=get_folder_choice_keyboard(),
        )
        return FOLDER_CHOICE
    if value == FOLDER_SKIP_TEXT:
        return await finish_movie_save(update, context)
    try:
        folder_names = get_all_folder_names()
    except Exception:
        logger.exception("Jildlar ro'yxatini tekshirishda xato yuz berdi")
        await reply_service_unavailable(update)
        return ConversationHandler.END
    if value not in folder_names:
        await update.message.reply_text(
            "⛔ Ro'yxatdan jild tanlang.",
            reply_markup=build_folder_list_keyboard(folder_names),
        )
        return FOLDER_PICK
    return await save_to_folder_and_finish(update, context, value)


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


async def add_series_range(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remember_user(update)
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return
    if len(context.args) < 3:
        await update.message.reply_text("📝 Ishlatish: /serialadd <boshlanish_kodi> <tugash_kodi> <nom>")
        return

    start_code_num = parse_numeric_code(context.args[0])
    end_code_num = parse_numeric_code(context.args[1])
    title = " ".join(context.args[2:]).strip()

    if start_code_num is None or end_code_num is None or not title:
        await update.message.reply_text("❌ Kodlar faqat raqam bo'lsin va nom ham yozilsin.")
        return
    if start_code_num > end_code_num:
        await update.message.reply_text("❌ Boshlanish kodi tugash kodidan katta bo'lmasligi kerak.")
        return

    try:
        overlaps = get_overlapping_series_ranges(start_code_num, end_code_num)
    except Exception:
        logger.exception("Serial diapazonlarini tekshirishda xato yuz berdi")
        await reply_service_unavailable(update)
        return

    conflicting_ranges = [
        item
        for item in overlaps
        if item["start_code_num"] != start_code_num or item["end_code_num"] != end_code_num
    ]
    if conflicting_ranges:
        conflict = conflicting_ranges[0]
        await update.message.reply_text(
            f"⚠️ Bu oraliq boshqa guruh bilan ustma-ust keladi:\n"
            f"🎞️ {conflict['title']}\n"
            f"🧾 {conflict['start_code_num']} - {conflict['end_code_num']}"
        )
        return

    try:
        movies = get_movies_in_range(start_code_num, end_code_num)
    except Exception:
        logger.exception("Serial diapazonidagi kinolarni olishda xato yuz berdi")
        await reply_service_unavailable(update)
        return

    if not movies:
        await update.message.reply_text("❌ Bu oraliqda hali kino topilmadi. Avval kinolarni qo'shib oling.")
        return

    try:
        save_series_range(start_code_num, end_code_num, title)
    except Exception:
        logger.exception("Serial diapazonini saqlashda xato yuz berdi")
        await reply_service_unavailable(update)
        return

    await update.message.reply_text(
        f"✅ Qismlar guruhi saqlandi.\n"
        f"🎞️ Nomi: {title}\n"
        f"🧾 Oraliq: {start_code_num} - {end_code_num}\n"
        f"📚 Topilgan qismlar: {len(movies)} ta"
    )


async def delete_series_range_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remember_user(update)
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return
    if len(context.args) < 2:
        await update.message.reply_text("📝 Ishlatish: /serialdel <boshlanish_kodi> <tugash_kodi>")
        return

    start_code_num = parse_numeric_code(context.args[0])
    end_code_num = parse_numeric_code(context.args[1])

    if start_code_num is None or end_code_num is None:
        await update.message.reply_text("❌ Kodlar faqat raqam bo'lsin.")
        return

    try:
        deleted_count = delete_series_range(start_code_num, end_code_num)
    except Exception:
        logger.exception("Serial diapazonini o'chirishda xato yuz berdi")
        await reply_service_unavailable(update)
        return

    if not deleted_count:
        await update.message.reply_text("❌ Bu oraliq bo'yicha saqlangan guruh topilmadi.")
        return

    await update.message.reply_text(
        f"🗑️ Qismlar guruhi o'chirildi: {start_code_num} - {end_code_num}"
    )


async def list_series_ranges(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remember_user(update)
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return

    try:
        ranges = get_all_series_ranges()
    except Exception:
        logger.exception("Serial diapazonlari ro'yxatini olishda xato yuz berdi")
        await reply_service_unavailable(update)
        return

    if not ranges:
        await update.message.reply_text("📭 Hali birorta ham qismlar guruhi saqlanmagan.")
        return

    lines = ["🎞️ Qismlar guruhlari:"]
    for item in ranges:
        lines.append(f"{item['start_code_num']}-{item['end_code_num']} | {item['title']}")
    await update.message.reply_text("\n".join(lines))


async def handle_series_part_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remember_user(update)
    query = update.callback_query
    if query is None or query.message is None:
        return

    await query.answer()
    code = query.data[len(SERIES_CALLBACK_PREFIX):]

    try:
        data = get_movie(code)
    except Exception:
        logger.exception("Qism bo'yicha kinoni olishda xato yuz berdi")
        await reply_service_unavailable(update)
        return

    if not data:
        await query.message.reply_text(f"❌ {code} kodli kino topilmadi.")
        return

    await send_movie_to_chat(query.message, code, data)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remember_user(update)
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("🎬 Kino kodini yozing.")
        return

    code = text
    try:
        folder_data = get_folder_by_code(code)
    except Exception:
        logger.exception("Kino jildini qidirishda xato yuz berdi")
        await reply_service_unavailable(update)
        return

    if folder_data is not None:
        try:
            folder_movies = get_movies_for_folder(folder_data["name"])
        except Exception:
            logger.exception("Jilddagi kinolarni olishda xato yuz berdi")
            await reply_service_unavailable(update)
            return
        if folder_movies:
            await send_folder_parts_prompt(update.message, folder_data, folder_movies)
            return

    try:
        series_data = get_series_range_by_code(code)
    except Exception:
        logger.exception("Kinoni qidirishda xato yuz berdi")
        await reply_service_unavailable(update)
        return

    if series_data is not None:
        try:
            movies = get_movies_in_range(series_data["start_code_num"], series_data["end_code_num"])
        except Exception:
            logger.exception("Qismlar guruhidagi kinolarni olishda xato yuz berdi")
            await reply_service_unavailable(update)
            return

        if movies:
            await send_series_parts_prompt(update.message, series_data, movies)
            return

    try:
        data = get_movie(code)
    except Exception:
        logger.exception("Kinoni qidirishda xato yuz berdi")
        await reply_service_unavailable(update)
        return

    if not data:
        await update.message.reply_text(f"❌ {code} kodli kino topilmadi.")
        return

    await send_movie_to_chat(update.message, code, data)


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
            FOLDER_CHOICE: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), handle_folder_choice)],
            FOLDER_CREATE: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), handle_folder_create)],
            FOLDER_PICK: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), handle_folder_pick)],
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
    app.add_handler(CommandHandler("serialadd", add_series_range))
    app.add_handler(CommandHandler("serialdel", delete_series_range_command))
    app.add_handler(CommandHandler("seriallist", list_series_ranges))
    app.add_handler(conv)
    app.add_handler(edit_conv)
    app.add_handler(CallbackQueryHandler(handle_series_part_callback, pattern=f"^{SERIES_CALLBACK_PREFIX}"))
    app.add_handler(MessageHandler(filters.COMMAND, unknown_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
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
