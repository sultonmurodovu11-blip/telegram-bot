from __future__ import annotations

import asyncio
import importlib
import logging
import os
import time
from typing import TYPE_CHECKING
import html
import uuid

try:
    from keep_alive import keep_alive, set_health_state
except ImportError:
    from .keep_alive import keep_alive, set_health_state

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.environ.get("ADMIN_ID", "6102256074"))
DEFAULT_INSTAGRAM_URL = "https://www.instagram.com/kinotop.bot/"
INSTAGRAM_CHANNEL_URL = os.environ.get("INSTAGRAM_CHANNEL_URL", "https://t.me/gram_piarbot?start=7259908930").strip() or DEFAULT_INSTAGRAM_URL

VERIFICATION_BOT_URL = os.environ.get("VERIFICATION_BOT_URL", "").strip()
VERIFICATION_WAIT_SECONDS = 15

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
ChatMemberHandler = None
InlineQueryHandler = None
InlineQueryResultArticle = None
InputTextMessageContent = None
MongoClient = None
PyMongoError = Exception

SEARCH_RESULTS_LIMIT = 10
LIST_PAGE_SIZE = 20
ADMIN_LIST_PAGE_CALLBACK = "adminlistpage:"


def ensure_telegram_imports():
    global ApplicationBuilder, CallbackQueryHandler, CommandHandler, ContextTypes
    global ConversationHandler, MessageHandler, filters
    global InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove
    global ChatMemberHandler, InlineQueryHandler, InlineQueryResultArticle, InputTextMessageContent
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
    ChatMemberHandler = telegram_ext.ChatMemberHandler
    InlineQueryHandler = telegram_ext.InlineQueryHandler
    InlineQueryResultArticle = telegram.InlineQueryResultArticle
    InputTextMessageContent = telegram.InputTextMessageContent


def ensure_pymongo_imports():
    global MongoClient, PyMongoError
    if MongoClient is not None and PyMongoError is not Exception:
        return

    pymongo = importlib.import_module("pymongo")
    pymongo_errors = importlib.import_module("pymongo.errors")
    MongoClient = pymongo.MongoClient
    PyMongoError = pymongo_errors.PyMongoError


def escape_html(text: str) -> str:
    if not text:
        return ""
    return html.escape(str(text))


MONGO_URL = os.environ.get("MONGO_URL", "").strip()
client = None
db = None
movies_col = None
series_col = None
folders_col = None
users_col = None
channels_col = None
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
JILD_FINISH_TEXT = "✅ Tugatish"
JILD_CLEAR_TEXT = "🧹 Tozalash"
DURATION_ALLOWED_CHARS = set("0123456789:")
DURATION_MAX_LENGTH = 10
SERIES_CALLBACK_PREFIX = "series_part:"
LIST_PAGE_CALLBACK = "listpage:"

_broadcast_active = False
_broadcast_status_message_id = None
_admin_sent_messages = {}


def reset_db_connection():
    global client, db, movies_col, series_col, folders_col, users_col, channels_col
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
    channels_col = None
    set_health_state(db="disconnected")


def get_movies_col():
    global client, db, movies_col, series_col, folders_col, users_col, channels_col
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

        try:
            db["users"].create_index("user_id", unique=True)
        except Exception:
            pass

        movies_col = db["movies"]
        series_col = db["series_groups"]
        folders_col = db["movie_folders"]
        users_col = db["users"]
        channels_col = db["required_channels"]
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


def get_channels_col():
    global channels_col
    if channels_col is not None:
        return channels_col
    get_movies_col()
    return channels_col


def run_channels_db(operation):
    col = get_channels_col()
    try:
        return operation(col)
    except PyMongoError as exc:
        reset_db_connection()
        set_health_state(db="error", last_error=f"MongoDB: {exc}")
        raise


def get_all_required_channels():
    return run_channels_db(lambda col: list(col.find({}, {"_id": 0})))


def add_required_channel(link: str, channel_id: int, title: str, button_title: str = ""):
    run_channels_db(
        lambda col: col.update_one(
            {"channel_id": channel_id},
            {
                "$set": {
                    "channel_id": channel_id,
                    "link": link,
                    "title": title,
                    "button_title": button_title,
                    "added_at": int(time.time()),
                }
            },
            upsert=True,
        )
    )


def remove_required_channel(channel_id: int):
    run_channels_db(lambda col: col.delete_one({"channel_id": channel_id}))


def increment_channel_join_count(channel_id: int):
    try:
        run_channels_db(
            lambda col: col.update_one(
                {"channel_id": channel_id},
                {"$inc": {"join_count": 1}},
            )
        )
    except Exception:
        pass


# ===================== MOVIES DB =====================

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
    return {"code": doc.get("code", "-"), "nom": doc.get("nom", "-")}


def get_last_and_next_movie_code():
    def operation(col):
        pipeline = [
            {"$addFields": {"code_num": {"$convert": {"input": "$code", "to": "int", "onError": None, "onNull": None}}}},
            {"$match": {"code_num": {"$ne": None, "$lt": 1000000}}},
            {"$sort": {"_id": -1}},
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


def increment_view_count(code):
    try:
        run_db(lambda col: col.update_one({"code": code}, {"$inc": {"views": 1}}))
    except Exception:
        pass


# ===================== QIDIRISH DB =====================

def search_movies_by_name(query: str, limit: int = SEARCH_RESULTS_LIMIT):
    if not query or len(query) < 2:
        return []

    def operation(col):
        import re
        pattern = re.compile(re.escape(query), re.IGNORECASE)
        cursor = col.find(
            {"nom": {"$regex": pattern}},
            {"_id": 0, "code": 1, "nom": 1, "sifat": 1, "til": 1, "vaqt": 1}
        ).sort("nom", 1).limit(limit)
        return list(cursor)

    try:
        return run_db(operation)
    except Exception:
        logger.exception("Qidirishda xato yuz berdi")
        return []


def get_all_movies_sorted(skip: int = 0, limit: int = LIST_PAGE_SIZE):
    def operation(col):
        pipeline = [
            {"$addFields": {"code_num": {"$convert": {"input": "$code", "to": "int", "onError": 999999999, "onNull": 999999999}}}},
            {"$sort": {"code_num": 1}},
            {"$skip": skip},
            {"$limit": limit},
            {"$project": {"_id": 0, "code": 1, "nom": 1}},
        ]
        return list(col.aggregate(pipeline))

    try:
        return run_db(operation)
    except Exception:
        logger.exception("Barcha kinolarni olishda xato")
        return []


def get_total_movies_count():
    try:
        return run_db(lambda col: col.count_documents({}))
    except Exception:
        return 0


# ===================== SERIES & FOLDERS =====================

def get_series_range_by_code(code):
    code_num = parse_numeric_code(code)
    if code_num is None:
        return None

    def operation(col):
        cursor = col.find(
            {"start_code_num": {"$lte": code_num}, "end_code_num": {"$gte": code_num}},
            {"_id": 0},
        ).sort("start_code_num", 1).limit(1)
        return next(cursor, None)

    return run_series_db(operation)


def get_all_series_ranges():
    return run_series_db(lambda col: list(col.find({}, {"_id": 0}).sort("start_code_num", 1)))


def get_movies_in_range(start_code_num, end_code_num):
    def operation(col):
        pipeline = [
            {"$addFields": {"code_num": {"$convert": {"input": "$code", "to": "int", "onError": None, "onNull": None}}}},
            {"$match": {"code_num": {"$ne": None, "$gte": start_code_num, "$lte": end_code_num}}},
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
                "$set": {"name": folder_name, "name_lower": folder_name.lower()},
                "$addToSet": {"codes": code},
                "$setOnInsert": {"created_at": int(time.time())},
            },
            upsert=True,
        )
    )


def add_movies_to_folder(folder_name, codes):
    unique_codes = sort_codes_for_folder(list(set(codes)))
    if not unique_codes:
        return
    run_folders_db(
        lambda col: col.update_one(
            {"name": folder_name},
            {
                "$set": {"name": folder_name, "name_lower": folder_name.lower()},
                "$addToSet": {"codes": {"$each": unique_codes}},
                "$setOnInsert": {"created_at": int(time.time())},
            },
            upsert=True,
        )
    )


def get_existing_movie_codes(codes):
    if not codes:
        return []
    return run_db(
        lambda col: [item["code"] for item in col.find({"code": {"$in": codes}}, {"_id": 0, "code": 1})]
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


def get_all_user_ids():
    try:
        raw = run_users_db(
            lambda col: col.distinct("user_id", {"is_admin": {"$ne": True}})
        )
        result = []
        seen = set()
        for uid in raw:
            try:
                uid_int = int(uid)
                if uid_int not in seen:
                    seen.add(uid_int)
                    result.append(uid_int)
            except Exception:
                pass
        return result
    except Exception:
        logger.exception("get_all_user_ids xato")
        return []


# ===================== SEVIMLILAR =====================

def add_to_favorites(user_id, code):
    run_users_db(
        lambda col: col.update_one(
            {"user_id": user_id},
            {"$addToSet": {"favorites": code}},
            upsert=True,
        )
    )


def remove_from_favorites(user_id, code):
    run_users_db(
        lambda col: col.update_one(
            {"user_id": user_id},
            {"$pull": {"favorites": code}},
        )
    )


def get_favorites(user_id):
    try:
        doc = run_users_db(lambda col: col.find_one({"user_id": user_id}, {"favorites": 1, "_id": 0}))
        if doc and "favorites" in doc:
            return doc["favorites"]
        return []
    except Exception:
        return []


def is_favorite(user_id, code):
    try:
        doc = run_users_db(lambda col: col.find_one({"user_id": user_id, "favorites": code}, {"_id": 1}))
        return doc is not None
    except Exception:
        return False


# ===================== VERIFICATION =====================

def mark_user_started(user_id):
    run_users_db(
        lambda col: col.update_one(
            {"user_id": user_id},
            {"$setOnInsert": {"started_at": int(time.time())}},
            upsert=True,
        )
    )


def get_user_started_at(user_id):
    try:
        doc = run_users_db(lambda col: col.find_one({"user_id": user_id}, {"started_at": 1, "_id": 0}))
        if doc and "started_at" in doc:
            return doc["started_at"]
        return None
    except Exception:
        return None


def get_verification_keyboard():
    if not VERIFICATION_BOT_URL:
        return None
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("✅ Botga o'tish", url=VERIFICATION_BOT_URL)]]
    )


# ===================== KANAL SUBSCRIPTION HELPERS =====================

async def check_user_subscribed(bot, user_id: int):
    try:
        channels = get_all_required_channels()
    except Exception:
        return True, []

    if not channels:
        return True, []

    not_subscribed = []
    for ch in channels:
        try:
            member = await bot.get_chat_member(chat_id=ch["channel_id"], user_id=user_id)
            if member.status in ("left", "kicked", "banned"):
                not_subscribed.append(ch)
        except Exception:
            pass

    return len(not_subscribed) == 0, not_subscribed


def get_subscribe_keyboard(not_subscribed_channels: list):
    rows = []
    for ch in not_subscribed_channels:
        btn_label = ch.get("button_title") or ch.get("title") or "Kanalga o'tish"
        rows.append([InlineKeyboardButton(f"📢 {btn_label}", url=ch["link"])])
    rows.append([InlineKeyboardButton("✅ Obuna bo'ldim", callback_data="check_subscription")])
    return InlineKeyboardMarkup(rows)


async def send_subscribe_required_message(target, not_subscribed_channels: list):
    await target.reply_text(
        "⚠️ Botdan foydalanish uchun quyidagi kanal(lar)ga obuna bo'ling:\n\n"
        "Obuna bo'lgandan so'ng ✅ <b>Obuna bo'ldim</b> tugmasini bosing.",
        reply_markup=get_subscribe_keyboard(not_subscribed_channels),
        parse_mode="HTML",
    )


# ===================== MOVIE HELPERS =====================

def get_movie_reply_markup(code, user_id=None):
    rows = []
    if user_id is not None and user_id != ADMIN_ID:
        try:
            in_fav = is_favorite(user_id, code)
        except Exception:
            in_fav = False
        fav_text = "❤️ Sevimlilardan chiqarish" if in_fav else "🤍 Sevimlilarga qo'shish"
        rows.append([InlineKeyboardButton(fav_text, callback_data=f"fav:{code}")])
    if INSTAGRAM_CHANNEL_URL:
        rows.append([InlineKeyboardButton("Qolgan kino kodlarini ko'rish uchun bosing", url=INSTAGRAM_CHANNEL_URL)])
    if not rows:
        return None
    return InlineKeyboardMarkup(rows)


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
        row.append(InlineKeyboardButton(f"{part_number}-qism", callback_data=f"{SERIES_CALLBACK_PREFIX}{movie['code']}"))
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
        row.append(InlineKeyboardButton(f"{index}-qism", callback_data=f"{SERIES_CALLBACK_PREFIX}{movie['code']}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)


async def send_movie_to_chat(target_message, code, data, user_id=None):
    caption = build_movie_caption(code, data)
    reply_markup = get_movie_reply_markup(code, user_id=user_id)
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
                },
                "$setOnInsert": {
                    "started_at": int(time.time()),
                },
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


# ===================== TUGMALAR =====================

def get_sifat_keyboard():
    return ReplyKeyboardMarkup(
        [["480p", "720p", "1080p"], ["1080p Full HD"], [KEEP_PREVIOUS_TEXT]],
        resize_keyboard=True, one_time_keyboard=True,
    )


def get_til_keyboard():
    return ReplyKeyboardMarkup(
        [["🇺🇿 O'zbek", "🇷🇺 Rus", "🇬🇧 Ingliz"], [KEEP_PREVIOUS_TEXT]],
        resize_keyboard=True, one_time_keyboard=True,
    )


def get_confirm_keyboard():
    return ReplyKeyboardMarkup(
        [[CONFIRM_SAVE_TEXT, CONFIRM_CANCEL_TEXT]],
        resize_keyboard=True, one_time_keyboard=True,
    )


def get_folder_choice_keyboard():
    return ReplyKeyboardMarkup(
        [[FOLDER_SKIP_TEXT], [FOLDER_CREATE_TEXT, FOLDER_ADD_EXISTING_TEXT]],
        resize_keyboard=True, one_time_keyboard=True,
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


def get_jild_codes_keyboard():
    return ReplyKeyboardMarkup(
        [[JILD_FINISH_TEXT, JILD_CLEAR_TEXT]],
        resize_keyboard=True, one_time_keyboard=False,
    )


def get_kod_suggestion_keyboard(next_code):
    return ReplyKeyboardMarkup(
        [[next_code, KEEP_PREVIOUS_TEXT]],
        resize_keyboard=True, one_time_keyboard=True,
    )


def get_admin_menu_keyboard():
    return ReplyKeyboardMarkup(
        [
            ["/edit", "/delete <kod>"],
            ["/jild", "/seriallist"],
            ["/addchannel <link>", "/removechannel"],
            ["/kanallar"],
            ["/foydalanuvchi 777", "/adminlik"],
            ["/ochirish", "/stat"],
            ["/top", "/sevimli"],
            ["/qidirish", "/barchasi"],
            ["/barchakino"],                   # ← YANGI: admin uchun barcha kinolar
            ["/help"],
        ],
        resize_keyboard=True, one_time_keyboard=False,
    )


def get_user_menu_keyboard():
    return ReplyKeyboardMarkup(
        [
            ["🔍 Qidirish", "📋 Barcha kinolar"],
            ["❤️ Sevimlilar"],
        ],
        resize_keyboard=True, one_time_keyboard=False,
    )


def get_broadcast_stop_keyboard():
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⛔ Yuborishni to'xtatish", callback_data="stop_broadcast")]]
    )


def get_bc_ask_button_keyboard():
    return ReplyKeyboardMarkup(
        [["✅ Ha, tugma qo'shish", "❌ Yo'q, shunchaki yuborish"]],
        resize_keyboard=True, one_time_keyboard=True,
    )


def get_bc_cancel_keyboard():
    return ReplyKeyboardMarkup(
        [["❌ Bekor qilish"]],
        resize_keyboard=True, one_time_keyboard=True,
    )


def get_bc_media_skip_keyboard():
    return ReplyKeyboardMarkup(
        [["⏭ Media qo'shmasdan o'tish"]],
        resize_keyboard=True, one_time_keyboard=True,
    )


# ===================== QIDIRISH TUGMALARI =====================

def build_search_results_keyboard(movies):
    rows = []
    for movie in movies:
        nom = movie.get("nom", "-")
        code = movie.get("code", "-")
        rows.append([InlineKeyboardButton(f"🎬 {nom}  |  #{code}", callback_data=f"getmovie:{code}")])
    return InlineKeyboardMarkup(rows)


def build_list_page_keyboard(page: int, total: int, page_size: int = LIST_PAGE_SIZE):
    total_pages = (total + page_size - 1) // page_size
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Oldingi", callback_data=f"{LIST_PAGE_CALLBACK}{page - 1}"))
    nav.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="noop"))
    if (page + 1) * page_size < total:
        nav.append(InlineKeyboardButton("Keyingi ➡️", callback_data=f"{LIST_PAGE_CALLBACK}{page + 1}"))
    return InlineKeyboardMarkup([nav]) if nav else None


def build_list_page_text(movies, page: int, total: int, page_size: int = LIST_PAGE_SIZE):
    start = page * page_size + 1
    lines = [f"📋 Barcha kinolar (jami: {total} ta) — {start}-{start + len(movies) - 1}:\n"]
    for i, movie in enumerate(movies, start=start):
        nom = movie.get("nom", "-")
        code = movie.get("code", "-")
        lines.append(f"{i}. 🎬 {nom}  |  Kod: <code>{code}</code>")
    lines.append("\nKino olish uchun kodini yuboring.")
    return "\n".join(lines)


# ===================== ADMIN BARCHAKINO TUGMALARI =====================

def build_admin_list_page_keyboard(page: int, total: int, page_size: int = LIST_PAGE_SIZE):
    total_pages = (total + page_size - 1) // page_size
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Oldingi", callback_data=f"{ADMIN_LIST_PAGE_CALLBACK}{page - 1}"))
    nav.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="noop"))
    if (page + 1) * page_size < total:
        nav.append(InlineKeyboardButton("Keyingi ➡️", callback_data=f"{ADMIN_LIST_PAGE_CALLBACK}{page + 1}"))
    rows = [nav]
    rows.append([InlineKeyboardButton("🔍 Qidirish", switch_inline_query_current_chat="")])
    return InlineKeyboardMarkup(rows) if nav else None


def build_admin_list_page_text(movies, page: int, total: int, page_size: int = LIST_PAGE_SIZE):
    start = page * page_size + 1
    total_pages = (total + page_size - 1) // page_size
    lines = [
        f"🎬 <b>Barcha kinolar</b> — sahifa {page + 1}/{total_pages}",
        f"📊 Jami: <b>{total} ta</b> | Ko'rsatilayapti: {start}–{start + len(movies) - 1}\n",
    ]
    for i, movie in enumerate(movies, start=start):
        nom = escape_html(movie.get("nom", "-"))
        code = movie.get("code", "-")
        lines.append(f"{i}. <code>{code}</code> | {nom}")
    lines.append("\n💡 Kino olish uchun kodini yuboring.")
    return "\n".join(lines)


# ====================================================

def parse_codes_input(raw_value):
    normalized = raw_value.replace(",", " ").replace(";", " ").replace("\n", " ").strip()
    if not normalized:
        return [], ["bo'sh qiymat"]

    parsed_codes = set()
    invalid_tokens = []
    for token in normalized.split():
        value = token.strip()
        if not value:
            continue
        if "-" in value:
            parts = value.split("-", 1)
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                start_num = int(parts[0])
                end_num = int(parts[1])
                if start_num <= end_num:
                    for code_num in range(start_num, end_num + 1):
                        parsed_codes.add(str(code_num))
                    continue
        elif value.isdigit():
            parsed_codes.add(value)
            continue
        invalid_tokens.append(value)
    return sort_codes_for_folder(list(parsed_codes)), invalid_tokens


def format_codes_for_text(codes, limit=20):
    if not codes:
        return "-"
    if len(codes) <= limit:
        return ", ".join(codes)
    preview = ", ".join(codes[:limit])
    return f"{preview} ... (jami {len(codes)} ta)"


async def send_confirm_prompt(update, data):
    await update.message.reply_text(
        f"📋 Tekshirib chiqing:\n\n"
        f"🆔 Kod: {data['kod']}\n"
        f"🎬 Nom: {data['nom']}\n"
        f"🎥 Sifat: {data['sifat']}\n"
        f"🌐 Til: {data['til']}\n"
        f"⏱️ Davomiylik: {data['vaqt']}\n\n"
        f"Saqlaymizmi?",
        reply_markup=get_confirm_keyboard(),
    )


# State raqamlari
KOD_VAQT, NOM, SIFAT, TIL, VAQT, CONFIRM, FOLDER_CHOICE, FOLDER_CREATE, FOLDER_PICK = range(9)
EDIT_KOD, EDIT_NOM, EDIT_SIFAT, EDIT_TIL, EDIT_VAQT = range(9, 14)
JILD_CODES, JILD_NAME = range(14, 16)
BC_GET_TEXT, BC_GET_MEDIA, BC_ASK_BUTTON, BC_GET_URL, BC_GET_BTN_NAME, BC_CONFIRM = range(16, 22)
ADDCH_GET_TITLE = 22
SEARCH_STATE = 23


async def log_error(update: object, context):
    logger.exception("Telegram handler error", exc_info=context.error)


async def reply_service_unavailable(update):
    if update.message:
        await update.message.reply_text(SERVICE_UNAVAILABLE_TEXT)
    elif update.callback_query and update.callback_query.message:
        await update.callback_query.message.reply_text(SERVICE_UNAVAILABLE_TEXT)


# ===================== START =====================

async def start(update, context):
    remember_user(update)
    user_id = update.message.from_user.id

    if user_id == ADMIN_ID:
        await update.message.reply_text(
            "Salom Admin! Movie HD botiga xush kelibsiz!\n\n"
            "Kino qo'shish uchun video yoki fayl yuboring.",
            reply_markup=get_admin_menu_keyboard(),
        )
        return

    try:
        mark_user_started(user_id)
    except Exception:
        logger.exception("Foydalanuvchi started_at ni saqlashda xato")

    if VERIFICATION_BOT_URL:
        keyboard = get_verification_keyboard()
        await update.message.reply_text(
            "🎬 Salom! Movie HD botiga xush kelibsiz!\n\n"
            "✅ Botdan foydalanish uchun quyidagi botga o'ting va /start bosing:\n\n"
            "⬇️ Tugmani bosing va start bosing:",
            reply_markup=keyboard,
        )
        await update.message.reply_text(
            f"⏳ Start bosgandan so'ng {VERIFICATION_WAIT_SECONDS} soniya kuting va "
            "shu yerga kino kodini yuboring!"
        )
    else:
        await update.message.reply_text(
            "🎬 Salom! Movie HD botiga xush kelibsiz!\n\n"
            "✅ Kino kodini yuboring va filmni oling!\n\n"
            "🔍 Kino nomini qidirish: /qidirish\n"
            "📋 Barcha kinolar: /barchasi\n"
            "❤️ Sevimlilar: /sevimli\n\n"
            "Masalan: 1 yoki 25 yoki 100",
            reply_markup=get_user_menu_keyboard(),
        )


async def unknown_command(update, context):
    await update.message.reply_text("❓ Bu komanda mavjud emas. Kino kodini yozing.")


def seconds_to_hhmmss(seconds: int) -> str:
    if not seconds or seconds <= 0:
        return "-"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    else:
        return f"{minutes}:{secs:02d}"


# ===================== QIDIRISH HANDLERLARI =====================

async def search_start(update, context):
    remember_user(update)
    user_id = update.effective_user.id

    if user_id != ADMIN_ID:
        try:
            is_subscribed, not_subscribed = await check_user_subscribed(context.bot, user_id)
            if not is_subscribed:
                await send_subscribe_required_message(update.message, not_subscribed)
                return ConversationHandler.END
        except Exception:
            pass

    await update.message.reply_text(
        "🔍 Qidirish\n\n"
        "Kino nomini yozing (kamida 2 harf):\n\n"
        "Bekor qilish: /cancel",
        reply_markup=ReplyKeyboardMarkup(
            [["❌ Bekor qilish"]], resize_keyboard=True, one_time_keyboard=True
        ),
    )
    return SEARCH_STATE


async def search_handle_query(update, context):
    query_text = update.message.text.strip()

    if query_text in ("❌ Bekor qilish", "/cancel"):
        user_id = update.effective_user.id
        kb = get_admin_menu_keyboard() if user_id == ADMIN_ID else get_user_menu_keyboard()
        await update.message.reply_text("❌ Bekor qilindi.", reply_markup=kb)
        return ConversationHandler.END

    if len(query_text) < 2:
        await update.message.reply_text("⚠️ Kamida 2 harf kiriting:")
        return SEARCH_STATE

    try:
        results = search_movies_by_name(query_text, limit=SEARCH_RESULTS_LIMIT)
    except Exception:
        logger.exception("Qidiruvda xato")
        await update.message.reply_text(SERVICE_UNAVAILABLE_TEXT)
        return SEARCH_STATE

    if not results:
        await update.message.reply_text(
            f"❌ «{query_text}» bo'yicha hech narsa topilmadi.\n\nBoshqa nom bilan urinib ko'ring:"
        )
        return SEARCH_STATE

    lines = [f"🔍 «{escape_html(query_text)}» — {len(results)} ta natija:\n"]
    for movie in results:
        nom = escape_html(movie.get("nom", "-"))
        code = movie.get("code", "-")
        lines.append(f"🎬 {nom}  |  Kod: <code>{code}</code>")
    lines.append("\n👆 Tugmani bosing yoki kodini yuboring.")

    keyboard = build_search_results_keyboard(results)
    await update.message.reply_text(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=keyboard,
    )
    return SEARCH_STATE


async def handle_getmovie_callback(update, context):
    remember_user(update)
    query = update.callback_query
    if query is None:
        return
    await query.answer()

    code = query.data[len("getmovie:"):]
    user_id = update.effective_user.id if update.effective_user else None

    try:
        data = get_movie(code)
    except Exception:
        await query.message.reply_text(SERVICE_UNAVAILABLE_TEXT)
        return

    if not data:
        await query.message.reply_text(f"❌ {code} kodli kino topilmadi.")
        return

    increment_view_count(code)
    await send_movie_to_chat(query.message, code, data, user_id=user_id)


# ===================== BARCHA KINOLAR (foydalanuvchi) =====================

async def show_all_movies(update, context):
    remember_user(update)
    user_id = update.effective_user.id

    if user_id != ADMIN_ID:
        try:
            is_subscribed, not_subscribed = await check_user_subscribed(context.bot, user_id)
            if not is_subscribed:
                await send_subscribe_required_message(update.message, not_subscribed)
                return
        except Exception:
            pass

    try:
        total = get_total_movies_count()
        movies = get_all_movies_sorted(skip=0, limit=LIST_PAGE_SIZE)
    except Exception:
        logger.exception("Barcha kinolarni olishda xato")
        await reply_service_unavailable(update)
        return

    if not movies:
        await update.message.reply_text("Hali birorta kino qo'shilmagan.")
        return

    text = build_list_page_text(movies, page=0, total=total)
    keyboard = build_list_page_keyboard(page=0, total=total)
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=keyboard)


async def handle_list_page_callback(update, context):
    query = update.callback_query
    if query is None:
        return
    await query.answer()

    if query.data == "noop":
        return

    try:
        page = int(query.data[len(LIST_PAGE_CALLBACK):])
    except ValueError:
        return

    try:
        total = get_total_movies_count()
        movies = get_all_movies_sorted(skip=page * LIST_PAGE_SIZE, limit=LIST_PAGE_SIZE)
    except Exception:
        await query.answer(SERVICE_UNAVAILABLE_TEXT, show_alert=True)
        return

    if not movies:
        await query.answer("Bu sahifada kinolar topilmadi.", show_alert=True)
        return

    text = build_list_page_text(movies, page=page, total=total)
    keyboard = build_list_page_keyboard(page=page, total=total)
    try:
        await query.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    except Exception:
        pass


# ===================== BARCHA KINOLAR (admin /barchakino) =====================

async def barchakino_admin(update, context):
    """
    /barchakino — faqat admin.
    KOD | NOM formatida sahifalab chiqaradi.
    Qidiruv inline tugmasi ham bor.
    """
    remember_user(update)
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Bu buyruq faqat admin uchun.")
        return

    try:
        total = get_total_movies_count()
        movies = get_all_movies_sorted(skip=0, limit=LIST_PAGE_SIZE)
    except Exception:
        logger.exception("barchakino_admin: kinolarni olishda xato")
        await reply_service_unavailable(update)
        return

    if not movies:
        await update.message.reply_text("Hali birorta kino qo'shilmagan.")
        return

    text = build_admin_list_page_text(movies, page=0, total=total)
    keyboard = build_admin_list_page_keyboard(page=0, total=total)
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=keyboard)


async def handle_admin_list_page_callback(update, context):
    """Admin /barchakino sahifalash callback."""
    query = update.callback_query
    if query is None:
        return
    await query.answer()

    if update.effective_user.id != ADMIN_ID:
        return

    try:
        page = int(query.data[len(ADMIN_LIST_PAGE_CALLBACK):])
    except ValueError:
        return

    try:
        total = get_total_movies_count()
        movies = get_all_movies_sorted(skip=page * LIST_PAGE_SIZE, limit=LIST_PAGE_SIZE)
    except Exception:
        await query.answer(SERVICE_UNAVAILABLE_TEXT, show_alert=True)
        return

    if not movies:
        await query.answer("Bu sahifada kinolar topilmadi.", show_alert=True)
        return

    text = build_admin_list_page_text(movies, page=page, total=total)
    keyboard = build_admin_list_page_keyboard(page=page, total=total)
    try:
        await query.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    except Exception:
        pass


# ===================== INLINE QIDIRISH =====================

async def handle_inline_query(update, context):
    inline_query = update.inline_query
    if inline_query is None:
        return

    query_text = inline_query.query.strip()

    if len(query_text) < 2:
        await inline_query.answer(
            [],
            switch_pm_text="Kino nomini yozing (kamida 2 harf)",
            switch_pm_parameter="search",
            cache_time=0,
        )
        return

    try:
        results_raw = search_movies_by_name(query_text, limit=SEARCH_RESULTS_LIMIT)
    except Exception:
        await inline_query.answer([], cache_time=0)
        return

    results = []
    for movie in results_raw:
        nom = movie.get("nom", "-")
        code = movie.get("code", "-")
        sifat = movie.get("sifat", "-")
        til = movie.get("til", "-")
        vaqt = movie.get("vaqt", "-")
        message_text = (
            f"🎬 {nom}\n"
            f"🎥 Sifat: {sifat}\n"
            f"🌐 Til: {til}\n"
            f"⏱️ Davomiylik: {vaqt}\n"
            f"🆔 Kod: {code}\n\n"
            f"Kinoni olish uchun botga kiring va {code} kodini yuboring."
        )
        results.append(
            InlineQueryResultArticle(
                id=str(uuid.uuid4()),
                title=f"🎬 {nom}",
                description=f"Sifat: {sifat} | Til: {til} | Kod: {code}",
                input_message_content=InputTextMessageContent(message_text),
            )
        )

    if not results:
        await inline_query.answer(
            [],
            switch_pm_text=f"«{query_text}» topilmadi",
            switch_pm_parameter="search",
            cache_time=0,
        )
        return

    await inline_query.answer(results, cache_time=5)


# ===================== BROADCAST =====================

async def _remove_broadcast_stop_button(bot, chat_id, message_id):
    global _broadcast_status_message_id
    if message_id is None:
        return
    try:
        await bot.edit_message_reply_markup(chat_id=chat_id, message_id=message_id, reply_markup=None)
    except Exception:
        pass
    _broadcast_status_message_id = None


async def admin_broadcast_start(update, context):
    remember_user(update)
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return ConversationHandler.END

    if _broadcast_active:
        await update.message.reply_text("⚠️ Hozir yuborish jarayoni faol. Kuting yoki /cancel bosing.")
        return ConversationHandler.END

    await update.message.reply_text(
        "📢 Ommaviy xabar yozish\n\n"
        "Yubormoqchi bo'lgan matnni kiriting.\n"
        "Formatlash (HTML) ishlaydi:\n"
        "  <b>qalin matn</b>\n"
        "  <i>kursiv matn</i>\n"
        "  <code>kod</code>\n"
        "  <a href='https://link.com'>Havola matni</a>\n\n"
        "❌ Bekor qilish uchun /cancel",
        reply_markup=get_bc_cancel_keyboard(),
    )
    return BC_GET_TEXT


async def bc_get_text(update, context):
    text = update.message.text.strip()
    if text in ("❌ Bekor qilish", "/cancel"):
        await update.message.reply_text("❌ Bekor qilindi.", reply_markup=get_admin_menu_keyboard())
        return ConversationHandler.END
    if not text:
        await update.message.reply_text("Matn bo'sh bo'lmasin. Qayta yuboring:")
        return BC_GET_TEXT

    context.user_data["bc_text"] = text
    context.user_data["bc_media"] = None
    context.user_data["bc_media_type"] = None

    await update.message.reply_text(
        "🖼 Xabarga rasm, GIF yoki video qo'shmoqchimisiz?\n\n"
        "Rasm, GIF yoki video yuboring.\n"
        "Yoki o'tkazib yuborish uchun tugmani bosing:",
        reply_markup=get_bc_media_skip_keyboard(),
    )
    return BC_GET_MEDIA


async def bc_get_media(update, context):
    msg = update.message
    if msg.text and msg.text.strip() in ("⏭ Media qo'shmasdan o'tish", "❌ Bekor qilish", "/cancel"):
        if msg.text.strip() in ("❌ Bekor qilish", "/cancel"):
            await msg.reply_text("❌ Bekor qilindi.", reply_markup=get_admin_menu_keyboard())
            return ConversationHandler.END
        context.user_data["bc_media"] = None
        context.user_data["bc_media_type"] = None
        await msg.reply_text(
            "🔗 Xabarga inline tugma qo'shmoqchimisiz?",
            reply_markup=get_bc_ask_button_keyboard(),
        )
        return BC_ASK_BUTTON

    if msg.photo:
        context.user_data["bc_media"] = msg.photo[-1].file_id
        context.user_data["bc_media_type"] = "photo"
        media_label = "🖼 Rasm"
    elif msg.animation:
        context.user_data["bc_media"] = msg.animation.file_id
        context.user_data["bc_media_type"] = "animation"
        media_label = "🎞 GIF"
    elif msg.video:
        context.user_data["bc_media"] = msg.video.file_id
        context.user_data["bc_media_type"] = "video"
        media_label = "🎬 Video"
    else:
        await msg.reply_text(
            "⚠️ Faqat rasm, GIF yoki video yuboring.\n"
            "O'tkazib yuborish uchun tugmani bosing:",
            reply_markup=get_bc_media_skip_keyboard(),
        )
        return BC_GET_MEDIA

    await msg.reply_text(
        f"✅ {media_label} qabul qilindi!\n\n🔗 Inline tugma qo'shmoqchimisiz?",
        reply_markup=get_bc_ask_button_keyboard(),
    )
    return BC_ASK_BUTTON


async def bc_ask_button(update, context):
    choice = update.message.text.strip()
    if choice in ("❌ Bekor qilish", "/cancel"):
        await update.message.reply_text("❌ Bekor qilindi.", reply_markup=get_admin_menu_keyboard())
        return ConversationHandler.END
    if choice == "✅ Ha, tugma qo'shish":
        await update.message.reply_text(
            "🌐 Tugma URL manzilini yuboring:\n\nMisol: https://t.me/kanalim",
            reply_markup=get_bc_cancel_keyboard(),
        )
        return BC_GET_URL
    if choice == "❌ Yo'q, shunchaki yuborish":
        context.user_data["bc_url"] = None
        context.user_data["bc_btn_name"] = None
        return await _bc_show_preview(update, context)
    await update.message.reply_text("Tugmalardan birini tanlang.", reply_markup=get_bc_ask_button_keyboard())
    return BC_ASK_BUTTON


async def bc_get_url(update, context):
    url = update.message.text.strip()
    if url in ("❌ Bekor qilish", "/cancel"):
        await update.message.reply_text("❌ Bekor qilindi.", reply_markup=get_admin_menu_keyboard())
        return ConversationHandler.END
    if not (url.startswith("http://") or url.startswith("https://") or url.startswith("tg://")):
        await update.message.reply_text(
            "⚠️ URL noto'g'ri. https:// bilan boshlang.",
            reply_markup=get_bc_cancel_keyboard(),
        )
        return BC_GET_URL
    context.user_data["bc_url"] = url
    await update.message.reply_text("✏️ Tugma ustidagi matnni yuboring:", reply_markup=get_bc_cancel_keyboard())
    return BC_GET_BTN_NAME


async def bc_get_btn_name(update, context):
    btn_name = update.message.text.strip()
    if btn_name in ("❌ Bekor qilish", "/cancel"):
        await update.message.reply_text("❌ Bekor qilindi.", reply_markup=get_admin_menu_keyboard())
        return ConversationHandler.END
    if not btn_name:
        await update.message.reply_text("Tugma nomi bo'sh bo'lmasin. Qayta yuboring:")
        return BC_GET_BTN_NAME
    context.user_data["bc_btn_name"] = btn_name
    return await _bc_show_preview(update, context)


async def _bc_show_preview(update, context):
    d = context.user_data
    bc_text = d.get("bc_text", "")
    bc_url = d.get("bc_url")
    bc_btn_name = d.get("bc_btn_name")
    bc_media = d.get("bc_media")
    bc_media_type = d.get("bc_media_type")

    user_markup = None
    if bc_url and bc_btn_name:
        user_markup = InlineKeyboardMarkup([[InlineKeyboardButton(bc_btn_name, url=bc_url)]])

    if bc_media and bc_media_type:
        preview_caption = f"👁 Ko'rinishi (preview):\n\n{bc_text}"
        try:
            if bc_media_type == "photo":
                await update.message.reply_photo(photo=bc_media, caption=preview_caption, reply_markup=user_markup, parse_mode="HTML")
            elif bc_media_type == "animation":
                await update.message.reply_animation(animation=bc_media, caption=preview_caption, reply_markup=user_markup, parse_mode="HTML")
            elif bc_media_type == "video":
                await update.message.reply_video(video=bc_media, caption=preview_caption, reply_markup=user_markup, parse_mode="HTML")
        except Exception:
            try:
                if bc_media_type == "photo":
                    await update.message.reply_photo(photo=bc_media, caption=preview_caption, reply_markup=user_markup)
                elif bc_media_type == "animation":
                    await update.message.reply_animation(animation=bc_media, caption=preview_caption, reply_markup=user_markup)
                elif bc_media_type == "video":
                    await update.message.reply_video(video=bc_media, caption=preview_caption, reply_markup=user_markup)
            except Exception:
                await update.message.reply_text(f"👁 Preview (media xato):\n\n{bc_text}", reply_markup=user_markup)
    else:
        preview_header = "👁 Ko'rinishi:\n─────────────────\n"
        preview_footer = "\n─────────────────"
        if bc_url and bc_btn_name:
            preview_footer += f"\n[ {escape_html(bc_btn_name)} ]"
        preview_footer += "\n─────────────────\n\nYuborishni tasdiqlaysizmi?"
        try:
            await update.message.reply_text(preview_header + bc_text + preview_footer, reply_markup=ReplyKeyboardRemove(), parse_mode="HTML")
        except Exception:
            await update.message.reply_text(preview_header + bc_text + preview_footer, reply_markup=ReplyKeyboardRemove())

    confirm_markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yuborish", callback_data="bc_yes"),
        InlineKeyboardButton("✏️ Qayta", callback_data="bc_edit"),
        InlineKeyboardButton("❌ Bekor", callback_data="bc_cancel"),
    ]])
    await update.message.reply_text("Tanlov:", reply_markup=confirm_markup)
    return BC_CONFIRM


async def _send_bc_message_to_user(bot, uid, bc_text, bc_media, bc_media_type, user_markup):
    if bc_media and bc_media_type:
        if bc_media_type == "photo":
            try:
                await bot.send_photo(chat_id=uid, photo=bc_media, caption=bc_text, reply_markup=user_markup, parse_mode="HTML")
            except Exception as e:
                if "parse" in str(e).lower() or "can't parse" in str(e).lower():
                    await bot.send_photo(chat_id=uid, photo=bc_media, caption=bc_text, reply_markup=user_markup)
                else:
                    raise
        elif bc_media_type == "animation":
            try:
                await bot.send_animation(chat_id=uid, animation=bc_media, caption=bc_text, reply_markup=user_markup, parse_mode="HTML")
            except Exception as e:
                if "parse" in str(e).lower() or "can't parse" in str(e).lower():
                    await bot.send_animation(chat_id=uid, animation=bc_media, caption=bc_text, reply_markup=user_markup)
                else:
                    raise
        elif bc_media_type == "video":
            try:
                await bot.send_video(chat_id=uid, video=bc_media, caption=bc_text, reply_markup=user_markup, parse_mode="HTML")
            except Exception as e:
                if "parse" in str(e).lower() or "can't parse" in str(e).lower():
                    await bot.send_video(chat_id=uid, video=bc_media, caption=bc_text, reply_markup=user_markup)
                else:
                    raise
    else:
        try:
            await bot.send_message(chat_id=uid, text=bc_text, reply_markup=user_markup, parse_mode="HTML")
        except Exception as e:
            if "parse" in str(e).lower() or "can't parse" in str(e).lower():
                await bot.send_message(chat_id=uid, text=bc_text, reply_markup=user_markup)
            else:
                raise


async def bc_confirm_callback(update, context):
    global _broadcast_active, _broadcast_status_message_id, _admin_sent_messages
    query = update.callback_query
    if query is None:
        return
    await query.answer()

    if update.effective_user.id != ADMIN_ID:
        return

    if query.data == "bc_cancel":
        await query.message.edit_reply_markup(reply_markup=None)
        await query.message.reply_text("❌ Bekor qilindi.", reply_markup=get_admin_menu_keyboard())
        return ConversationHandler.END

    if query.data == "bc_edit":
        await query.message.edit_reply_markup(reply_markup=None)
        await query.message.reply_text("Yangi matnni yuboring:", reply_markup=get_bc_cancel_keyboard())
        return BC_GET_TEXT

    if query.data == "bc_yes":
        if _broadcast_active:
            await query.answer("⚠️ Broadcast allaqachon faol! Kuting.", show_alert=True)
            return ConversationHandler.END

        _broadcast_active = True
        await query.message.edit_reply_markup(reply_markup=None)

        d = context.user_data
        bc_text = d.get("bc_text", "")
        bc_url = d.get("bc_url")
        bc_btn_name = d.get("bc_btn_name")
        bc_media = d.get("bc_media")
        bc_media_type = d.get("bc_media_type")

        user_markup = None
        if bc_url and bc_btn_name:
            user_markup = InlineKeyboardMarkup([[InlineKeyboardButton(bc_btn_name, url=bc_url)]])

        try:
            user_ids = get_all_user_ids()
        except Exception:
            _broadcast_active = False
            logger.exception("Foydalanuvchilar ro'yxatini olishda xato")
            await query.message.reply_text(SERVICE_UNAVAILABLE_TEXT)
            return ConversationHandler.END

        if not user_ids:
            _broadcast_active = False
            await query.message.reply_text("Bazada foydalanuvchilar topilmadi.", reply_markup=get_admin_menu_keyboard())
            return ConversationHandler.END

        total = len(user_ids)
        taxminiy = round(total * 0.09 / 60)
        chat_id = update.effective_chat.id

        try:
            preview_msg = await _send_bc_preview_to_admin(context.bot, chat_id, bc_text, bc_media, bc_media_type, user_markup)
            if preview_msg:
                if ADMIN_ID not in _admin_sent_messages:
                    _admin_sent_messages[ADMIN_ID] = []
                short_preview = bc_text[:50].replace("\n", " ")
                _admin_sent_messages[ADMIN_ID].append({
                    "msg_id": preview_msg.message_id,
                    "chat_id": chat_id,
                    "preview": short_preview,
                })
        except Exception:
            logger.exception("Admin preview xabarini yuborishda xato")

        status_msg = await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"📢 Yuborish boshlandi!\n"
                f"Jami (unikal): {total} ta foydalanuvchi\n"
                f"Taxminiy vaqt: ~{taxminiy} daqiqa\n\n"
                f"✅ Bot ishlashda davom etadi."
            ),
            reply_markup=get_broadcast_stop_keyboard(),
        )
        _broadcast_status_message_id = status_msg.message_id

        bot = context.bot

        async def do_send():
            global _broadcast_active, _broadcast_status_message_id
            sent = 0
            failed = 0
            blocked = 0

            for uid in user_ids:
                if not _broadcast_active:
                    break
                try:
                    await _send_bc_message_to_user(bot, int(uid), bc_text, bc_media, bc_media_type, user_markup)
                    sent += 1
                except Exception as e:
                    err = str(e).lower()
                    if any(k in err for k in ["blocked", "deactivated", "not found", "chat not found"]):
                        blocked += 1
                    else:
                        logger.warning(f"Xabar yuborishda xato (user_id={uid}): {e}")
                        failed += 1
                await asyncio.sleep(0.09)

            await _remove_broadcast_stop_button(bot, chat_id, _broadcast_status_message_id)
            stopped_early = not _broadcast_active
            _broadcast_active = False

            lines = ["⛔ Broadcast to'xtatildi!" if stopped_early else "✅ Broadcast tugadi!"]
            lines.append(f"Muvaffaqiyatli: {sent} ta")
            if blocked:
                lines.append(f"Bot bloklagan: {blocked} ta")
            if failed:
                lines.append(f"Boshqa xato: {failed} ta")

            try:
                await bot.send_message(chat_id=chat_id, text="\n".join(lines), reply_markup=get_admin_menu_keyboard())
            except Exception:
                pass

        asyncio.create_task(do_send())
        return ConversationHandler.END

    return BC_CONFIRM


async def _send_bc_preview_to_admin(bot, chat_id, bc_text, bc_media, bc_media_type, user_markup):
    if bc_media and bc_media_type:
        if bc_media_type == "photo":
            try:
                return await bot.send_photo(chat_id=chat_id, photo=bc_media, caption=bc_text, reply_markup=user_markup, parse_mode="HTML")
            except Exception:
                return await bot.send_photo(chat_id=chat_id, photo=bc_media, caption=bc_text, reply_markup=user_markup)
        elif bc_media_type == "animation":
            try:
                return await bot.send_animation(chat_id=chat_id, animation=bc_media, caption=bc_text, reply_markup=user_markup, parse_mode="HTML")
            except Exception:
                return await bot.send_animation(chat_id=chat_id, animation=bc_media, caption=bc_text, reply_markup=user_markup)
        elif bc_media_type == "video":
            try:
                return await bot.send_video(chat_id=chat_id, video=bc_media, caption=bc_text, reply_markup=user_markup, parse_mode="HTML")
            except Exception:
                return await bot.send_video(chat_id=chat_id, video=bc_media, caption=bc_text, reply_markup=user_markup)
    else:
        try:
            return await bot.send_message(chat_id=chat_id, text=bc_text, reply_markup=user_markup, parse_mode="HTML")
        except Exception:
            return await bot.send_message(chat_id=chat_id, text=bc_text, reply_markup=user_markup)


async def admin_broadcast_stop_callback(update, context):
    global _broadcast_active, _broadcast_status_message_id
    query = update.callback_query
    if query is None:
        return
    await query.answer("⛔ To'xtatilmoqda...", show_alert=False)

    if update.effective_user.id != ADMIN_ID:
        return

    _broadcast_active = False
    try:
        await query.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    _broadcast_status_message_id = None
    await query.message.reply_text("⛔ Ommaviy xabar to'xtatildi.", reply_markup=get_admin_menu_keyboard())


# ===================== O'CHIRISH =====================

async def admin_delete_menu(update, context):
    global _admin_sent_messages
    remember_user(update)
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return

    msgs = _admin_sent_messages.get(ADMIN_ID, [])
    if not msgs:
        await update.message.reply_text(
            "📭 O'chiriladigan xabar yo'q.\n\n"
            "Faqat /adminlik orqali yuborilgan xabarlar ko'rinadi.\n"
            "(Bot qayta ishga tushsa ro'yxat tozalanadi)",
            reply_markup=get_admin_menu_keyboard(),
        )
        return

    lines = ["🗑 Qaysi xabarni o'chirasiz?\n"]
    for i, m in enumerate(msgs, start=1):
        lines.append(f"{i}. {m['preview']}...")

    buttons = []
    for i, m in enumerate(msgs):
        buttons.append([InlineKeyboardButton(f"{i+1}. {m['preview'][:25]}...", callback_data=f"del_bc:{i}")])
    buttons.append([InlineKeyboardButton("🧹 Barchasini o'chirish", callback_data="del_bc:all")])
    buttons.append([InlineKeyboardButton("❌ Bekor", callback_data="del_bc:cancel")])

    await update.message.reply_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(buttons))


async def admin_delete_bc_callback(update, context):
    global _admin_sent_messages
    query = update.callback_query
    await query.answer()

    if update.effective_user.id != ADMIN_ID:
        return

    msgs = _admin_sent_messages.get(ADMIN_ID, [])
    action = query.data.split(":", 1)[1]

    if action == "cancel":
        await query.message.edit_reply_markup(reply_markup=None)
        return

    if action == "all":
        deleted = 0
        for m in msgs:
            try:
                await context.bot.delete_message(chat_id=m["chat_id"], message_id=m["msg_id"])
                deleted += 1
            except Exception as e:
                logger.warning(f"Xabarni o'chirishda xato: {e}")
        _admin_sent_messages[ADMIN_ID] = []
        await query.message.edit_reply_markup(reply_markup=None)
        await query.message.reply_text(f"🗑 {deleted} ta xabar o'chirildi.", reply_markup=get_admin_menu_keyboard())
        return

    try:
        idx = int(action)
    except ValueError:
        return

    if 0 <= idx < len(msgs):
        m = msgs[idx]
        try:
            await context.bot.delete_message(chat_id=m["chat_id"], message_id=m["msg_id"])
            _admin_sent_messages[ADMIN_ID].pop(idx)
            await query.message.edit_reply_markup(reply_markup=None)
            await query.message.reply_text("✅ Xabar o'chirildi.", reply_markup=get_admin_menu_keyboard())
        except Exception as e:
            await query.message.edit_reply_markup(reply_markup=None)
            await query.message.reply_text(
                f"⚠️ O'chirishda xato: {e}\n"
                "(Telegram 48 soatdan eski xabarlarni o'chirishga ruxsat bermaydi)",
                reply_markup=get_admin_menu_keyboard(),
            )
    else:
        await query.message.edit_reply_markup(reply_markup=None)


# ===================== ADMIN HELP & STAT =====================

async def admin_help(update, context):
    remember_user(update)
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return

    help_text = (
        "Admin buyruqlari:\n\n"
        "Kino qo'shish:\n"
        "  Video yoki fayl yuboring\n\n"
        "Tahrirlash:\n"
        "  /edit — kino ma'lumotlarini yangilash\n\n"
        "O'chirish:\n"
        "  /delete <kod>\n\n"
        "Jildlar:\n"
        "  /jild — yangi jild yaratish\n\n"
        "Seriallar:\n"
        "  /seriallist — barcha serial diapazonlari\n\n"
        "Kanal boshqaruvi:\n"
        "  /addchannel <link>\n"
        "  /removechannel\n"
        "  /kanallar — statistika\n\n"
        "Statistika:\n"
        "  /foydalanuvchi 777\n"
        "  /stat\n"
        "  /top\n\n"
        "Qidirish va ro'yxat:\n"
        "  /qidirish — kino nomini qidirish\n"
        "  /barchasi — barcha kinolar (foydalanuvchi)\n"
        "  /barchakino — barcha kinolar (admin, kod|nom)\n\n"
        "Ommaviy xabar:\n"
        "  /adminlik — xabar yuborish\n"
        "  /ochirish — xabarlarni o'chirish\n\n"
        "Sevimlilar:\n"
        "  /sevimli"
    )
    await update.message.reply_text(help_text, reply_markup=get_admin_menu_keyboard())


async def admin_stat(update, context):
    remember_user(update)
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return

    try:
        total_users = get_tracked_user_count()
        total_movies = run_db(lambda col: col.count_documents({}))
        total_folders = run_folders_db(lambda col: col.count_documents({}))
        total_series = run_series_db(lambda col: col.count_documents({}))
        last_code, next_code = get_last_and_next_movie_code()
        total_channels = run_channels_db(lambda col: col.count_documents({}))
    except Exception:
        logger.exception("Statistikani olishda xato")
        await reply_service_unavailable(update)
        return

    await update.message.reply_text(
        f"Bot statistikasi:\n\n"
        f"Foydalanuvchilar: {total_users} ta\n"
        f"Kinolar: {total_movies} ta\n"
        f"Jildlar: {total_folders} ta\n"
        f"Serial diapazonlari: {total_series} ta\n"
        f"Majburiy kanallar: {total_channels} ta\n"
        f"Oxirgi kiritilgan kod: {last_code}\n"
        f"Keyingi tavsiya kod: {next_code}"
    )


# ===================== VIDEO/DOCUMENT QABUL QILISH =====================

async def handle_video(update, context):
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
            f"Kod: {existing_movie['code']}\n"
            f"Nom: {existing_movie['nom']}\n\n"
            f"Boshqa kino faylini yuboring."
        )
        return ConversationHandler.END

    duration_seconds = update.message.video.duration or 0
    if duration_seconds > 0:
        auto_vaqt = seconds_to_hhmmss(duration_seconds)
        context.user_data["vaqt"] = auto_vaqt
        context.user_data["vaqt_auto"] = True
    else:
        context.user_data["vaqt"] = DEFAULT_VAQT
        context.user_data["vaqt_auto"] = False

    context.user_data.pop("vaqt_draft", None)
    context.user_data.pop("vaqt_locked", None)

    try:
        last_code, next_code = get_last_and_next_movie_code()
    except Exception:
        last_code, next_code = "?", "?"

    vaqt_info = f"\n⏱️ Davomiylik avtomatik aniqlandi: {context.user_data['vaqt']}" if context.user_data["vaqt_auto"] else ""
    await update.message.reply_text(
        f"Oxirgi kiritilgan kod: {last_code}\n"
        f"Tavsiya etilayotgan kod: {next_code}{vaqt_info}\n\n"
        f"Kodini kiriting yoki quyidagi tugmani bosing:",
        reply_markup=get_kod_suggestion_keyboard(next_code),
    )
    return KOD_VAQT


async def handle_document(update, context):
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return

    context.user_data["file_id"] = update.message.document.file_id
    context.user_data["file_type"] = "document"
    context.user_data["vaqt_auto"] = False
    context.user_data.pop("vaqt_draft", None)
    context.user_data.pop("vaqt_locked", None)

    try:
        existing_movie = get_movie_by_file_id(context.user_data["file_id"])
    except Exception:
        logger.exception("Dublikat faylni tekshirishda xato yuz berdi")
        await reply_service_unavailable(update)
        return ConversationHandler.END

    if existing_movie:
        await update.message.reply_text(
            f"⚠️ Bu fayl allaqachon bazada bor.\n"
            f"Kod: {existing_movie['code']}\n"
            f"Nom: {existing_movie['nom']}\n\n"
            f"Boshqa kino faylini yuboring."
        )
        return ConversationHandler.END

    try:
        last_code, next_code = get_last_and_next_movie_code()
    except Exception:
        last_code, next_code = "?", "?"

    await update.message.reply_text(
        f"Oxirgi kiritilgan kod: {last_code}\n"
        f"Tavsiya etilayotgan kod: {next_code}\n\n"
        f"Kodini kiriting yoki quyidagi tugmani bosing:",
        reply_markup=get_kod_suggestion_keyboard(next_code),
    )
    return KOD_VAQT


# ===================== CONVERSATION STATES =====================

async def get_kod_vaqt(update, context):
    d = context.user_data
    raw = update.message.text.strip()

    if raw == KEEP_PREVIOUS_TEXT:
        try:
            _, next_code = get_last_and_next_movie_code()
        except Exception:
            next_code = "?"
        await update.message.reply_text("Kodini kiriting:", reply_markup=get_kod_suggestion_keyboard(next_code))
        return KOD_VAQT

    code = raw
    if not code or not code.isdigit():
        try:
            _, next_code = get_last_and_next_movie_code()
        except Exception:
            next_code = "?"
        await update.message.reply_text(
            f"Kod faqat raqamlardan iborat bo'lsin.\nTavsiya: {next_code}",
            reply_markup=get_kod_suggestion_keyboard(next_code),
        )
        return KOD_VAQT

    try:
        exists = movie_exists(code)
    except Exception:
        logger.exception("Kino kodini tekshirishda xato yuz berdi")
        await reply_service_unavailable(update)
        return ConversationHandler.END

    if exists:
        try:
            _, next_code = get_last_and_next_movie_code()
        except Exception:
            next_code = "?"
        await update.message.reply_text(
            f"⚠️ {code} kodi allaqachon mavjud! Boshqa kod kiriting.\nTavsiya: {next_code}",
            reply_markup=get_kod_suggestion_keyboard(next_code),
        )
        return KOD_VAQT

    d["kod"] = code
    await update.message.reply_text("Kino nomini kiriting:", reply_markup=ReplyKeyboardRemove())
    return NOM


async def get_nom(update, context):
    context.user_data["nom"] = update.message.text.strip()
    await update.message.reply_text("Sifatini tanlang yoki qo'lda yozing:", reply_markup=get_sifat_keyboard())
    return SIFAT


async def get_sifat(update, context):
    raw = update.message.text.strip()
    if raw == KEEP_PREVIOUS_TEXT:
        context.user_data["sifat"] = context.user_data.get("last_sifat") or DEFAULT_SIFAT
    else:
        context.user_data["sifat"] = raw or DEFAULT_SIFAT
    await update.message.reply_text("Tilini tanlang yoki qo'lda yozing:", reply_markup=get_til_keyboard())
    return TIL


async def get_til(update, context):
    d = context.user_data
    raw = update.message.text.strip()
    if raw == KEEP_PREVIOUS_TEXT:
        d["til"] = d.get("last_til") or DEFAULT_TIL
    else:
        value = raw
        for prefix in ["🇺🇿 ", "🇷🇺 ", "🇬🇧 "]:
            if value.startswith(prefix):
                value = value[len(prefix):]
                break
        d["til"] = value or DEFAULT_TIL

    if d.get("vaqt_auto"):
        await send_confirm_prompt(update, d)
        return CONFIRM
    else:
        await update.message.reply_text(
            "Davomiyligini kiriting (masalan: 1:57:36 yoki - ):",
            reply_markup=ReplyKeyboardRemove(),
        )
        return VAQT


async def get_vaqt(update, context):
    d = context.user_data
    d["vaqt"] = update.message.text.strip() or DEFAULT_VAQT
    await send_confirm_prompt(update, d)
    return CONFIRM


async def confirm_save(update, context):
    choice = update.message.text.strip()
    if choice == CONFIRM_CANCEL_TEXT:
        await update.message.reply_text("❌ Bekor qilindi.", reply_markup=ReplyKeyboardRemove())
        return ConversationHandler.END
    if choice != CONFIRM_SAVE_TEXT:
        await update.message.reply_text("Iltimos, tugmadan birini tanlang.")
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
    d.pop("vaqt_locked", None)
    d.pop("vaqt_draft", None)
    d.pop("vaqt_auto", None)

    await update.message.reply_text("📁 Jildga saqlashni xohlaysizmi?", reply_markup=get_folder_choice_keyboard())
    return FOLDER_CHOICE


async def finish_movie_save(update, context, folder_note=None):
    d = context.user_data
    note_text = f"\n{folder_note}\n" if folder_note else "\n"
    await update.message.reply_text(
        f"✅ Saqlandi.\n\nKod: {d['kod']}\nNom: {d['nom']}\nSifat: {d['sifat']}\nTil: {d['til']}\nDavomiylik: {d['vaqt']}\n"
        f"{note_text}Keyingi kino uchun yana video yoki fayl yuboring.",
        reply_markup=ReplyKeyboardRemove(),
    )
    return ConversationHandler.END


def get_part_number_in_movies(movies, code):
    for idx, movie in enumerate(movies, start=1):
        if movie["code"] == code:
            return idx
    return None


async def save_to_folder_and_finish(update, context, folder_name):
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
    folder_note = f"Jild: {folder_name}\nQism: {part_number}/{len(movies)}" if part_number else f"Jild: {folder_name}"
    return await finish_movie_save(update, context, folder_note=folder_note)


async def handle_folder_choice(update, context):
    choice = update.message.text.strip()
    if choice == FOLDER_SKIP_TEXT:
        return await finish_movie_save(update, context)
    if choice == FOLDER_CREATE_TEXT:
        await update.message.reply_text("Yangi jild nomini yozing:", reply_markup=ReplyKeyboardRemove())
        return FOLDER_CREATE
    if choice == FOLDER_ADD_EXISTING_TEXT:
        try:
            folder_names = get_all_folder_names()
        except Exception:
            logger.exception("Jildlar ro'yxatini olishda xato yuz berdi")
            await reply_service_unavailable(update)
            return ConversationHandler.END
        if not folder_names:
            await update.message.reply_text("Hali jildlar yo'q.", reply_markup=get_folder_choice_keyboard())
            return FOLDER_CHOICE
        await update.message.reply_text("Jildni tanlang:", reply_markup=build_folder_list_keyboard(folder_names))
        return FOLDER_PICK
    await update.message.reply_text("Tugmalardan birini tanlang.", reply_markup=get_folder_choice_keyboard())
    return FOLDER_CHOICE


async def handle_folder_create(update, context):
    folder_name = update.message.text.strip()
    if not folder_name:
        await update.message.reply_text("Jild nomi bo'sh bo'lmasin. Qayta kiriting:")
        return FOLDER_CREATE
    try:
        exists = folder_exists_by_name(folder_name)
    except Exception:
        logger.exception("Jild nomini tekshirishda xato yuz berdi")
        await reply_service_unavailable(update)
        return ConversationHandler.END
    if exists:
        await update.message.reply_text("⚠️ Bu nomli jild bor. Boshqa nom kiriting:")
        return FOLDER_CREATE
    return await save_to_folder_and_finish(update, context, folder_name)


async def handle_folder_pick(update, context):
    value = update.message.text.strip()
    if value == FOLDER_BACK_TEXT:
        await update.message.reply_text("Jildga saqlashni xohlaysizmi?", reply_markup=get_folder_choice_keyboard())
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
        await update.message.reply_text("Ro'yxatdan jild tanlang.", reply_markup=build_folder_list_keyboard(folder_names))
        return FOLDER_PICK
    return await save_to_folder_and_finish(update, context, value)


# ===================== JILD =====================

async def jild_start(update, context):
    remember_user(update)
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        return ConversationHandler.END

    context.user_data["jild_codes"] = []
    await update.message.reply_text(
        "Jild yaratish boshlandi.\n"
        "Kino kodlarini yuboring (masalan: 9 yoki 9 10 11 yoki 9-16).\n"
        f"Tayyor bo'lganda {JILD_FINISH_TEXT} tugmasini bosing.",
        reply_markup=get_jild_codes_keyboard(),
    )
    return JILD_CODES


async def jild_get_codes(update, context):
    value = update.message.text.strip()
    d = context.user_data
    current_codes = set(d.get("jild_codes", []))

    if value == JILD_CLEAR_TEXT:
        d["jild_codes"] = []
        await update.message.reply_text("Kodlar ro'yxati tozalandi.", reply_markup=get_jild_codes_keyboard())
        return JILD_CODES

    if value == JILD_FINISH_TEXT:
        if not current_codes:
            await update.message.reply_text("Hali birorta kod kiritilmadi.", reply_markup=get_jild_codes_keyboard())
            return JILD_CODES
        d["jild_codes"] = sort_codes_for_folder(list(current_codes))
        await update.message.reply_text("Endi jild nomini yozing:", reply_markup=ReplyKeyboardRemove())
        return JILD_NAME

    parsed_codes, invalid_tokens = parse_codes_input(value)
    if not parsed_codes and invalid_tokens:
        await update.message.reply_text(
            f"Noto'g'ri qiymat(lar): {', '.join(invalid_tokens)}\nTo'g'ri misol: 9 yoki 9 10 11 yoki 9-16",
            reply_markup=get_jild_codes_keyboard(),
        )
        return JILD_CODES
    if not parsed_codes:
        await update.message.reply_text("Kodlarni kiriting.", reply_markup=get_jild_codes_keyboard())
        return JILD_CODES

    try:
        existing_codes = set(get_existing_movie_codes(parsed_codes))
    except Exception:
        logger.exception("Jild uchun kodlarni tekshirishda xato yuz berdi")
        await reply_service_unavailable(update)
        return ConversationHandler.END

    missing_codes = [code for code in parsed_codes if code not in existing_codes]
    new_codes = [code for code in parsed_codes if code in existing_codes and code not in current_codes]
    current_codes.update(existing_codes)
    d["jild_codes"] = sort_codes_for_folder(list(current_codes))

    message_lines = [
        f"✅ Qo'shildi: {len(new_codes)} ta",
        f"Jami yig'ilgan: {len(d['jild_codes'])} ta",
        f"Ro'yxat: {format_codes_for_text(d['jild_codes'])}",
    ]
    if missing_codes:
        message_lines.append(f"⚠️ Topilmagan: {', '.join(missing_codes)}")
    if invalid_tokens:
        message_lines.append(f"⚠️ Noto'g'ri: {', '.join(invalid_tokens)}")
    message_lines.append(f"Tayyor bo'lsa {JILD_FINISH_TEXT} bosing.")
    await update.message.reply_text("\n".join(message_lines), reply_markup=get_jild_codes_keyboard())
    return JILD_CODES


async def jild_get_name(update, context):
    folder_name = update.message.text.strip()
    if not folder_name:
        await update.message.reply_text("Jild nomi bo'sh bo'lmasin. Qayta yozing:")
        return JILD_NAME

    d = context.user_data
    codes = d.get("jild_codes", [])
    if not codes:
        await update.message.reply_text("Kodlar topilmadi. /jild qayta bosing.", reply_markup=ReplyKeyboardRemove())
        return ConversationHandler.END

    try:
        existed_before = folder_exists_by_name(folder_name)
        add_movies_to_folder(folder_name, codes)
        folder_movies = get_movies_for_folder(folder_name)
    except Exception:
        logger.exception("Jildni saqlashda xato yuz berdi")
        await reply_service_unavailable(update)
        return ConversationHandler.END

    d.pop("jild_codes", None)
    action_text = "yangilandi" if existed_before else "yaratildi"
    await update.message.reply_text(
        f"✅ Jild {action_text}.\nNomi: {folder_name}\nJilddagi kinolar: {len(folder_movies)} ta\n"
        f"Kodlar: {format_codes_for_text([movie['code'] for movie in folder_movies])}",
        reply_markup=ReplyKeyboardRemove(),
    )
    return ConversationHandler.END


async def cancel(update, context):
    user_id = update.effective_user.id if update.effective_user else None
    kb = get_admin_menu_keyboard() if user_id == ADMIN_ID else get_user_menu_keyboard()
    await update.message.reply_text("❌ Bekor qilindi.", reply_markup=kb)
    return ConversationHandler.END


# ===================== EDIT =====================

async def edit_start(update, context):
    remember_user(update)
    if update.message.from_user.id != ADMIN_ID:
        return
    await update.message.reply_text("Tahrirlash uchun kino kodini kiriting:")
    return EDIT_KOD


async def edit_get_kod(update, context):
    code = update.message.text.strip()
    try:
        data = get_movie(code)
    except Exception:
        await reply_service_unavailable(update)
        return ConversationHandler.END
    if not data:
        await update.message.reply_text(f"❌ {code} kodli kino topilmadi.")
        return ConversationHandler.END
    context.user_data['edit_code'] = code
    context.user_data['current_data'] = data
    await update.message.reply_text(
        f"Joriy ma'lumotlar:\nNom: {data['nom']}\nSifat: {data['sifat']}\n"
        f"Til: {data['til']}\nDavomiylik: {data['vaqt']}\n\nYangi nomni kiriting (bo'sh = o'zgarmas):"
    )
    return EDIT_NOM


async def edit_get_nom(update, context):
    new_nom = update.message.text.strip()
    context.user_data['nom'] = new_nom if new_nom else context.user_data['current_data']['nom']
    await update.message.reply_text("Yangi sifatni tanlang:", reply_markup=get_sifat_keyboard())
    return EDIT_SIFAT


async def edit_get_sifat(update, context):
    new_sifat = update.message.text.strip()
    if new_sifat and new_sifat != KEEP_PREVIOUS_TEXT:
        context.user_data['sifat'] = new_sifat
    else:
        context.user_data['sifat'] = context.user_data['current_data']['sifat']
    await update.message.reply_text("Yangi tilni tanlang:", reply_markup=get_til_keyboard())
    return EDIT_TIL


async def edit_get_til(update, context):
    new_til = update.message.text.strip()
    if new_til and new_til != KEEP_PREVIOUS_TEXT:
        for prefix in ["🇺🇿 ", "🇷🇺 ", "🇬🇧 "]:
            if new_til.startswith(prefix):
                new_til = new_til[len(prefix):]
                break
        context.user_data['til'] = new_til
    else:
        context.user_data['til'] = context.user_data['current_data']['til']
    await update.message.reply_text("Yangi davomiylikni kiriting (bo'sh = o'zgarmas):", reply_markup=ReplyKeyboardRemove())
    return EDIT_VAQT


async def edit_get_vaqt(update, context):
    new_vaqt = update.message.text.strip()
    context.user_data['vaqt'] = new_vaqt if new_vaqt else context.user_data['current_data']['vaqt']
    d = context.user_data
    data = {
        "type": d['current_data']['type'],
        "file_id": d['current_data']['file_id'],
        "nom": d['nom'],
        "sifat": d['sifat'],
        "til": d['til'],
        "vaqt": d['vaqt'],
    }
    try:
        save_movie(d['edit_code'], data)
    except Exception:
        logger.exception("Kinoni tahrirlashda xato yuz berdi")
        await reply_service_unavailable(update)
        return ConversationHandler.END

    await update.message.reply_text(
        f"✅ Yangilandi.\n\nKod: {d['edit_code']}\nNom: {d['nom']}\n"
        f"Sifat: {d['sifat']}\nTil: {d['til']}\nDavomiylik: {d['vaqt']}",
        reply_markup=get_admin_menu_keyboard(),
    )
    return ConversationHandler.END


# ===================== main() — HANDLER QO'SHISH ESLATMASI =====================
# main() funksiyangizda quyidagilarni qo'shing (mavjud handlerlarga QO'SHIMCHA):
#
#   application.add_handler(CommandHandler("barchakino", barchakino_admin))
#
#   application.add_handler(CallbackQueryHandler(
#       handle_admin_list_page_callback,
#       pattern="^adminlistpage:"
#   ))
#
#   application.add_handler(MessageHandler(
#       filters.Text(["📋 Barcha kinolar"]),
#       show_all_movies
#   ))
#
#   application.add_handler(MessageHandler(
#       filters.Text(["🔍 Qidirish"]),
#       search_start
#   ))
# ============================================================================
