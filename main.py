from __future__ import annotations

import asyncio
import importlib
import logging
import os
import re
import time
from typing import TYPE_CHECKING
import html

try:
    from keep_alive import keep_alive, set_health_state
except ImportError:
    from .keep_alive import keep_alive, set_health_state

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.environ.get("ADMIN_ID", "6102256074"))
DEFAULT_INSTAGRAM_URL = "https://www.instagram.com/kinotop.bot/"
INSTAGRAM_CHANNEL_URL = os.environ.get("INSTAGRAM_CHANNEL_URL", "https://www.instagram.com/kinoplay_uzz?igsh=MTd5am0xbG40ZzZ0Zg%3D%3D&utm_source=qr").strip() or DEFAULT_INSTAGRAM_URL

VERIFICATION_BOT_URL = os.environ.get("VERIFICATION_BOT_URL", "").strip()
VERIFICATION_WAIT_SECONDS = 15

ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "@Ulugbeck_dev").strip()
ADMIN_PHONE = os.environ.get("ADMIN_PHONE", "+998941444654").strip()

OPTIONAL_CHANNELS_ENV = os.environ.get("OPTIONAL_CHANNELS", "").strip()

def get_optional_channels():
    if not OPTIONAL_CHANNELS_ENV:
        return []
    channels = []
    for item in OPTIONAL_CHANNELS_ENV.split(","):
        item = item.strip()
        if "|" in item:
            parts = item.split("|", 1)
            channels.append({"link": parts[0].strip(), "title": parts[1].strip()})
        elif item:
            channels.append({"link": item, "title": "Kanalga o'tish"})
    return channels

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
MongoClient = None
PyMongoError = Exception

# ===================== SUBSCRIPTION CACHE =====================
_subscription_cache: dict[int, tuple[float, bool, list]] = {}
SUBSCRIPTION_CACHE_TTL = 180  # 3 daqiqa


def ensure_telegram_imports():
    global ApplicationBuilder, CallbackQueryHandler, CommandHandler, ContextTypes
    global ConversationHandler, MessageHandler, filters
    global InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove
    global ChatMemberHandler
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
SERVICE_UNAVAILABLE_TEXT = "Bot Sozlanmoqda Keyinroq Urunib ko'ring"
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

# Foydalanuvchi menyu tugmalari
USER_BTN_REKLAMA = "💰 Reklama narxlari"
USER_BTN_YANGI = "🎬 Yangi filmlar"
USER_BTN_SEVIMLI = "❤️ Sevimlilar"
USER_BTN_PROFIL = "👤 Profil"
USER_BTN_OBUNA = "📋 Kanallar"
USER_BTN_ALOQA = "📞 Aloqa"
USER_BTN_QIDIRUV = "🔍 Kino qidirish"

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
            serverSelectionTimeoutMS=3000,
            connectTimeoutMS=3000,
            socketTimeoutMS=10000,
            maxPoolSize=20,
            minPoolSize=3,
            waitQueueTimeoutMS=2000,
        )
        client.admin.command("ping")
        db = client["moviebot"]
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
    return run_channels_db(lambda col: list(col.find({"type": {"$ne": "optional"}}, {"_id": 0})))


def add_required_channel(link: str, channel_id: int, title: str, button_title: str = ""):
    run_channels_db(
        lambda col: col.update_one(
            {"channel_id": channel_id},
            {"$set": {"channel_id": channel_id, "link": link, "title": title, "button_title": button_title, "added_at": int(time.time())}},
            upsert=True,
        )
    )


def remove_required_channel(channel_id: int):
    run_channels_db(lambda col: col.delete_one({"channel_id": channel_id}))


def increment_channel_join_count(channel_id: int):
    try:
        run_channels_db(lambda col: col.update_one({"channel_id": channel_id}, {"$inc": {"join_count": 1}}))
    except Exception:
        pass


# ===================== IXTIYORIY KANAL DB =====================

def get_all_optional_channels_db():
    try:
        return list(run_channels_db(
            lambda col: col.find({"type": "optional"}, {"_id": 0}).sort("added_at", 1)
        ))
    except Exception:
        return []


def add_optional_channel_db(link: str, title: str, photo_id: str = ""):
    run_channels_db(
        lambda col: col.update_one(
            {"link": link, "type": "optional"},
            {"$set": {
                "link": link,
                "title": title,
                "type": "optional",
                "photo_id": photo_id,
                "added_at": int(time.time()),
            }},
            upsert=True,
        )
    )


def remove_optional_channel_db(link: str):
    run_channels_db(lambda col: col.delete_one({"link": link, "type": "optional"}))


# ===================== MOVIES DB =====================

def save_movie(code, data):
    run_db(lambda col: col.update_one({"code": code}, {"$set": {**data, "code": code}}, upsert=True))


def delete_movie_db(code):
    run_db(lambda col: col.delete_one({"code": code}))


def movie_exists(code):
    return run_db(lambda col: col.find_one({"code": code}, {"_id": 1}) is not None)


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


def get_latest_movies(limit: int = 10):
    def operation(col):
        pipeline = [
            {"$addFields": {"code_num": {"$convert": {"input": "$code", "to": "int", "onError": None, "onNull": None}}}},
            {"$match": {"code_num": {"$ne": None}}},
            {"$sort": {"_id": -1}},
            {"$limit": limit},
            {"$project": {"_id": 0, "code": 1, "nom": 1, "sifat": 1, "til": 1}},
        ]
        return list(col.aggregate(pipeline))
    return run_db(operation)


def parse_numeric_code(value):
    if not value or not value.isdigit():
        return None
    return int(value)


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
            {"$set": {"name": folder_name, "name_lower": folder_name.lower()}, "$addToSet": {"codes": code}, "$setOnInsert": {"created_at": int(time.time())}},
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
            {"$set": {"name": folder_name, "name_lower": folder_name.lower()}, "$addToSet": {"codes": {"$each": unique_codes}}, "$setOnInsert": {"created_at": int(time.time())}},
            upsert=True,
        )
    )


def get_existing_movie_codes(codes):
    if not codes:
        return []
    return run_db(lambda col: [item["code"] for item in col.find({"code": {"$in": codes}}, {"_id": 0, "code": 1})])


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
    raw = run_users_db(lambda col: [item["user_id"] for item in col.find({"is_admin": {"$ne": True}}, {"_id": 0, "user_id": 1})])
    result = []
    for uid in raw:
        try:
            result.append(int(uid))
        except Exception:
            pass
    return result


def search_movies_by_name(query: str, limit: int = 20):
    if not query or not query.strip():
        return []
    query = query.strip()
    regex_pattern = ".*".join(re.escape(ch) for ch in query)
    try:
        results = run_db(
            lambda col: list(col.find(
                {"nom": {"$regex": regex_pattern, "$options": "i"}},
                {"_id": 0, "code": 1, "nom": 1, "sifat": 1, "til": 1},
            ).limit(limit))
        )
        return results
    except Exception:
        return []


def get_all_movies_list(limit: int = 500):
    def operation(col):
        pipeline = [
            {"$addFields": {"code_num": {"$convert": {"input": "$code", "to": "int", "onError": None, "onNull": None}}}},
            {"$sort": {"code_num": 1, "code": 1}},
            {"$limit": limit},
            {"$project": {"_id": 0, "code": 1, "nom": 1, "sifat": 1, "til": 1, "code_num": 1}},
        ]
        return list(col.aggregate(pipeline))
    return run_db(operation)


# ===================== SEVIMLILAR =====================

def add_to_favorites(user_id, code):
    run_users_db(lambda col: col.update_one({"user_id": user_id}, {"$addToSet": {"favorites": code}}, upsert=True))


def remove_from_favorites(user_id, code):
    run_users_db(lambda col: col.update_one({"user_id": user_id}, {"$pull": {"favorites": code}}))


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
            {"user_id": user_id, "started_at": {"$exists": False}},
            {"$set": {"started_at": int(time.time())}},
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


def get_user_doc(user_id):
    try:
        return run_users_db(lambda col: col.find_one({"user_id": user_id}, {"_id": 0}))
    except Exception:
        return None


def get_user_view_count(user_id):
    try:
        doc = run_users_db(lambda col: col.find_one({"user_id": user_id}, {"view_count": 1, "_id": 0}))
        if doc:
            return doc.get("view_count", 0)
        return 0
    except Exception:
        return 0


def increment_user_view_count(user_id):
    try:
        run_users_db(lambda col: col.update_one({"user_id": user_id}, {"$inc": {"view_count": 1}}, upsert=True))
    except Exception:
        pass


def get_verification_keyboard():
    if not VERIFICATION_BOT_URL:
        return None
    return InlineKeyboardMarkup([[InlineKeyboardButton("✅ Botga o'tish", url=VERIFICATION_BOT_URL)]])


# ===================== KANAL SUBSCRIPTION — CACHE BILAN =====================

async def check_user_subscribed(bot, user_id: int):
    global _subscription_cache
    now = time.time()
    if user_id in _subscription_cache:
        cached_time, cached_result, cached_not_sub = _subscription_cache[user_id]
        if now - cached_time < SUBSCRIPTION_CACHE_TTL:
            return cached_result, cached_not_sub
    try:
        channels = get_all_required_channels()
    except Exception:
        return True, []
    if not channels:
        _subscription_cache[user_id] = (now, True, [])
        return True, []
    not_subscribed = []
    tasks = [_check_single_channel(bot, user_id, ch) for ch in channels]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for ch, result in zip(channels, results):
        if isinstance(result, bool) and not result:
            not_subscribed.append(ch)
    is_ok = len(not_subscribed) == 0
    _subscription_cache[user_id] = (now, is_ok, not_subscribed)
    return is_ok, not_subscribed


async def _check_single_channel(bot, user_id: int, ch: dict):
    try:
        member = await bot.get_chat_member(chat_id=ch["channel_id"], user_id=user_id)
        return member.status not in ("left", "kicked", "banned")
    except Exception:
        return True


def invalidate_subscription_cache(user_id: int):
    _subscription_cache.pop(user_id, None)


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
    if user_id and user_id != ADMIN_ID:
        increment_user_view_count(user_id)
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
            {"$set": {
                "user_id": user.id,
                "username": user.username or "",
                "first_name": user.first_name or "",
                "last_name": user.last_name or "",
                "is_admin": user.id == ADMIN_ID,
                "last_seen_at": int(time.time()),
            }},
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

def get_user_menu_keyboard():
    return ReplyKeyboardMarkup(
        [
            [USER_BTN_QIDIRUV],
            [USER_BTN_REKLAMA, USER_BTN_YANGI],
            [USER_BTN_SEVIMLI, USER_BTN_PROFIL],
            [USER_BTN_OBUNA, USER_BTN_ALOQA],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


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
    return ReplyKeyboardMarkup([[JILD_FINISH_TEXT, JILD_CLEAR_TEXT]], resize_keyboard=True, one_time_keyboard=False)


def get_kod_suggestion_keyboard(next_code):
    return ReplyKeyboardMarkup([[next_code, KEEP_PREVIOUS_TEXT]], resize_keyboard=True, one_time_keyboard=True)


def get_search_cancel_keyboard():
    return ReplyKeyboardMarkup(
        [["❌ Qidiruvni bekor qilish"]],
        resize_keyboard=True, one_time_keyboard=True,
    )


def get_admin_menu_keyboard():
    return ReplyKeyboardMarkup(
        [
            ["/edit", "/delete <kod>"],
            ["/jild", "/seriallist"],
            ["/addchannel <link>", "/removechannel"],
            ["/kanallar", "/ixtiyoriyobuna"],
            ["/ixtiyoriyochirish", "/foydalanuvchi 777"],
            ["/adminlik", "/ochirish"],
            ["/stat", "/top"],
            ["/sevimli", "/barchasi"],
            ["/help"],
        ],
        resize_keyboard=True, one_time_keyboard=False,
    )


def get_broadcast_stop_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⛔ Yuborishni to'xtatish", callback_data="stop_broadcast")]])


def get_bc_ask_button_keyboard():
    return ReplyKeyboardMarkup([["✅ Ha, tugma qo'shish", "❌ Yo'q, shunchaki yuborish"]], resize_keyboard=True, one_time_keyboard=True)


def get_bc_cancel_keyboard():
    return ReplyKeyboardMarkup([["❌ Bekor qilish"]], resize_keyboard=True, one_time_keyboard=True)


def get_bc_media_skip_keyboard():
    return ReplyKeyboardMarkup([["⏭ Media qo'shmasdan o'tish"]], resize_keyboard=True, one_time_keyboard=True)


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


# ===================== STATE RAQAMLARI =====================
KOD_VAQT, NOM, SIFAT, TIL, VAQT, CONFIRM, FOLDER_CHOICE, FOLDER_CREATE, FOLDER_PICK = range(9)
EDIT_KOD, EDIT_NOM, EDIT_SIFAT, EDIT_TIL, EDIT_VAQT = range(9, 14)
JILD_CODES, JILD_NAME = range(14, 16)
BC_GET_TEXT, BC_GET_MEDIA, BC_ASK_BUTTON, BC_GET_URL, BC_GET_BTN_NAME, BC_CONFIRM = range(16, 22)
ADDCH_GET_TITLE = 22
SEARCH_INPUT = 23
IXTIYORIY_LINK, IXTIYORIY_NOM, IXTIYORIY_RASM = range(24, 27)
# ===== POLL STATE RAQAMLARI =====
BC_POLL_QUESTION, BC_POLL_OPTIONS, BC_POLL_CONFIRM = range(27, 30)


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
            "🔍 Kino nomini yoki kodini qidirish uchun <b>🔍 Kino qidirish</b> tugmasini bosing!\n\n"
            "Masalan: 1 yoki 25 yoki Ronaldo",
            parse_mode="HTML",
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


# ===================== QIDIRUV CONVERSATION =====================

async def search_start(update, context):
    remember_user(update)
    user_id = update.message.from_user.id

    is_subscribed, not_subscribed = await check_user_subscribed(context.bot, user_id)
    if not is_subscribed:
        await send_subscribe_required_message(update.message, not_subscribed)
        return ConversationHandler.END

    await update.message.reply_text(
        "🔍 <b>Kino qidirish</b>\n\n"
        "Kino nomini yoki kodini yozing:\n\n"
        "Misol: <code>Ronaldo</code> yoki <code>25</code>",
        parse_mode="HTML",
        reply_markup=get_search_cancel_keyboard(),
    )
    return SEARCH_INPUT


async def search_get_input(update, context):
    text = update.message.text.strip()
    user_id = update.message.from_user.id

    if text == "❌ Qidiruvni bekor qilish":
        await update.message.reply_text(
            "❌ Qidiruv bekor qilindi.",
            reply_markup=get_user_menu_keyboard(),
        )
        return ConversationHandler.END

    if not text:
        await update.message.reply_text(
            "Kino nomi yoki kodi bo'sh bo'lmasin. Qayta yozing:",
            reply_markup=get_search_cancel_keyboard(),
        )
        return SEARCH_INPUT

    if text.isdigit():
        code = text
        try:
            result = await _find_and_send_by_code(update.message, code, user_id)
        except Exception:
            logger.exception("Kod bo'yicha qidirishda xato")
            await update.message.reply_text(
                SERVICE_UNAVAILABLE_TEXT,
                reply_markup=get_user_menu_keyboard(),
            )
            return ConversationHandler.END

        if result == "not_found":
            await update.message.reply_text(
                f"❌ <b>{code}</b> kodli kino topilmadi.\n\n"
                "Boshqa nom yoki kod yozing:",
                parse_mode="HTML",
                reply_markup=get_search_cancel_keyboard(),
            )
            return SEARCH_INPUT
        else:
            await update.message.reply_text(
                "Yana qidirish uchun nom yoki kod yozing:",
                reply_markup=get_search_cancel_keyboard(),
            )
            return SEARCH_INPUT

    if len(text) < 2:
        await update.message.reply_text(
            "⚠️ Kamida 2 ta harf yozing.",
            reply_markup=get_search_cancel_keyboard(),
        )
        return SEARCH_INPUT

    try:
        results = search_movies_by_name(text, limit=15)
    except Exception:
        logger.exception("Nom bo'yicha qidirishda xato")
        await update.message.reply_text(
            SERVICE_UNAVAILABLE_TEXT,
            reply_markup=get_user_menu_keyboard(),
        )
        return ConversationHandler.END

    if not results:
        await update.message.reply_text(
            f"🔍 <b>«{escape_html(text)}»</b> bo'yicha hech narsa topilmadi.\n\n"
            "Boshqa nom yoki kod yozing:",
            parse_mode="HTML",
            reply_markup=get_search_cancel_keyboard(),
        )
        return SEARCH_INPUT

    if len(results) == 1:
        movie = results[0]
        code = movie["code"]
        try:
            data = get_movie(code)
        except Exception:
            logger.exception("Kino olishda xato")
            await update.message.reply_text(
                SERVICE_UNAVAILABLE_TEXT,
                reply_markup=get_user_menu_keyboard(),
            )
            return ConversationHandler.END
        if data:
            increment_view_count(code)
            await send_movie_to_chat(update.message, code, data, user_id=user_id)
            await update.message.reply_text(
                "✅ Kino yuborildi! Yana qidirish uchun nom yoki kod yozing:",
                reply_markup=get_search_cancel_keyboard(),
            )
        return SEARCH_INPUT

    lines = [f"🔍 <b>«{escape_html(text)}»</b> bo'yicha {len(results)} ta kino topildi:\n"]
    buttons = []
    for movie in results:
        nom = movie.get("nom", "-")
        code = movie.get("code", "-")
        sifat = movie.get("sifat", "-")
        til = movie.get("til", "-")
        lines.append(f"🎬 {escape_html(nom)}\n   🎥 {sifat} | 🌐 {til} | 🆔 Kod: <b>{code}</b>\n")
        buttons.append([InlineKeyboardButton(f"🎬 {nom} [{code}]", callback_data=f"getmovie:{code}")])

    lines.append("👆 Quyidagi tugmalardan birini bosing yoki kodini yozing:")

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    return SEARCH_INPUT


async def _find_and_send_by_code(message, code: str, user_id: int) -> str:
    folder_data = get_folder_by_code(code)
    if folder_data is not None:
        folder_movies = get_movies_for_folder(folder_data["name"])
        if folder_movies:
            await send_folder_parts_prompt(message, folder_data, folder_movies)
            return "folder"

    series_data = get_series_range_by_code(code)
    if series_data is not None:
        movies = get_movies_in_range(series_data["start_code_num"], series_data["end_code_num"])
        if movies:
            await send_series_parts_prompt(message, series_data, movies)
            return "series"

    data = get_movie(code)
    if not data:
        return "not_found"

    increment_view_count(code)
    await send_movie_to_chat(message, code, data, user_id=user_id)
    return "sent"


async def search_cancel(update, context):
    await update.message.reply_text("❌ Qidiruv bekor qilindi.", reply_markup=get_user_menu_keyboard())
    return ConversationHandler.END


# ===================== GETMOVIE CALLBACK =====================

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
        logger.exception("Kino olishda xato")
        await query.message.reply_text(SERVICE_UNAVAILABLE_TEXT)
        return

    if not data:
        await query.message.reply_text(f"❌ {code} kodli kino topilmadi.")
        return

    increment_view_count(code)
    if user_id and user_id != ADMIN_ID:
        increment_user_view_count(user_id)
    await send_movie_to_chat(query.message, code, data, user_id=user_id)


# ===================== FOYDALANUVCHI MENYU HANDLERLARI =====================

async def handle_user_reklama(update, context):
    await update.message.reply_text(
        "Assalomu alaykum 👋\n"
        "Reklama narxlari 💰\n\n"
        "Telegram kanallar va guruhlar uchun:\n"
        "• 1 000 ta obunachi — 100 000 so'm 💸\n"
        "• 3 000 ta obunachi — 280 000 so'm 💸\n"
        "• 5 000 ta obunachi — 450 000 so'm 💸\n\n"
        "Instagram uchun 🌐:\n"
        "• 1 000 ta obunachi — 200 000 so'm 💸\n"
        "• 2 000 ta obunachi — 380 000 so'm 💸\n"
        "• 3 000 ta obunachi — 550 000 so'm 💸\n\n"
        "YouTube uchun 📲:\n"
        "• 1 000 ta obunachi — 250 000 so'm 💸\n"
        "• 2 000 ta obunachi — 450 000 so'm 💸\n\n"
        "✅ 100% jonli obunachilar (Uzb obunachilar)\n"
        "⚙️ Kanalga qarab narxlar o'zgarishi mumkin\n\n"
        "🛠 Zayafka kanal ham qilib beramiz: 10K — 600 000 so'm 💸\n\n"
        "Narxlar bilan tanishib keyin adminga yozing 📩",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✉️ Adminga yozish", url=f"https://t.me/{ADMIN_USERNAME.lstrip('@')}")]]),
    )


async def handle_user_yangi_filmlar(update, context):
    try:
        movies = get_latest_movies(limit=10)
    except Exception:
        logger.exception("Yangi kinolarni olishda xato")
        await reply_service_unavailable(update)
        return

    if not movies:
        await update.message.reply_text("🎬 Hali kino qo'shilmagan.", reply_markup=get_user_menu_keyboard())
        return

    lines = ["🎬 Oxirgi qo'shilgan kinolar:\n"]
    for i, movie in enumerate(movies, start=1):
        nom = movie.get("nom", "-")
        code = movie.get("code", "-")
        sifat = movie.get("sifat", "-")
        lines.append(f"{i}. {nom}  |  {sifat}  |  Kod: <b>{code}</b>")
    lines.append("\nKino olish uchun kodini yuboring yoki <b>🔍 Kino qidirish</b> tugmasini bosing 👆")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML", reply_markup=get_user_menu_keyboard())


async def handle_user_profil(update, context):
    user = update.message.from_user
    user_id = user.id

    try:
        user_doc = get_user_doc(user_id)
        fav_codes = get_favorites(user_id)
        view_count = get_user_view_count(user_id)
    except Exception:
        logger.exception("Profil ma'lumotlarini olishda xato")
        await reply_service_unavailable(update)
        return

    first_name = user.first_name or "-"
    last_name = user.last_name or ""
    username = f"@{user.username}" if user.username else "Yo'q"
    full_name = f"{first_name} {last_name}".strip()

    started_at = user_doc.get("started_at") if user_doc else None
    reg_date = time.strftime("%Y-%m-%d", time.localtime(started_at)) if started_at else "Noma'lum"
    last_seen = user_doc.get("last_seen_at") if user_doc else None
    last_seen_str = time.strftime("%Y-%m-%d %H:%M", time.localtime(last_seen)) if last_seen else "Noma'lum"

    await update.message.reply_text(
        f"👤 Sizning profilingiz\n\n"
        f"🆔 ID: <code>{user_id}</code>\n"
        f"📛 Ism: {escape_html(full_name)}\n"
        f"🔖 Username: {escape_html(username)}\n"
        f"📅 Ro'yxatdan o'tgan: {reg_date}\n"
        f"🕐 Oxirgi faollik: {last_seen_str}\n\n"
        f"🎬 Ko'rilgan kinolar: {view_count} ta\n"
        f"❤️ Sevimlilar: {len(fav_codes)} ta",
        parse_mode="HTML",
        reply_markup=get_user_menu_keyboard(),
    )


async def handle_user_obuna(update, context):
    try:
        optional_channels = get_all_optional_channels_db()
    except Exception:
        optional_channels = []

    if not optional_channels:
        await update.message.reply_text(
            "📋 Hozircha tavsiya kanallar yo'q.\n\nKino kodini yuboring va filmni oling!",
            reply_markup=get_user_menu_keyboard(),
        )
        return

    rows = []
    for ch in optional_channels:
        title = ch.get("title", "Kanalga o'tish")
        link = ch.get("link", "")
        rows.append([InlineKeyboardButton(f"📢 {title}", url=link)])

    markup = InlineKeyboardMarkup(rows)
    first_photo = optional_channels[0].get("photo_id", "") if optional_channels else ""
    if first_photo:
        await update.message.reply_photo(
            photo=first_photo,
            caption=(
                "📋 <b>Bizning kanallarimiz</b>\n\n"
                "Quyidagi kanallarga qo'shiling va eng so'nggi kinolardan xabardor bo'ling! 👇"
            ),
            parse_mode="HTML",
            reply_markup=markup,
        )
    else:
        await update.message.reply_text(
            "📋 <b>Bizning kanallarimiz</b>\n\n"
            "Quyidagi kanallarga qo'shiling va eng so'nggi kinolardan xabardor bo'ling! 👇",
            parse_mode="HTML",
            reply_markup=markup,
        )


async def handle_user_aloqa(update, context):
    admin_link = f"https://t.me/{ADMIN_USERNAME.lstrip('@')}"
    await update.message.reply_text(
        "📞 Aloqa\n\n"
        f"👤 Admin: {escape_html(ADMIN_USERNAME)}\n"
        f"📱 Telefon: {escape_html(ADMIN_PHONE)}\n\n"
        "Savol, taklif yoki muammo bo'lsa adminga yozing 👇",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✉️ Adminga yozish", url=admin_link)]]),
    )


# ===================== IXTIYORIY OBUNA CONVERSATION =====================

async def ixtiyoriy_obuna_start(update, context):
    remember_user(update)
    if update.message.from_user.id != ADMIN_ID:
        return ConversationHandler.END

    await update.message.reply_text(
        "📢 Ixtiyoriy kanal qo'shish\n\n"
        "1️⃣ Kanal linkini yuboring:\n"
        "Misol: https://t.me/kanalim\n\n"
        "❌ Bekor qilish uchun /cancel",
        reply_markup=ReplyKeyboardRemove(),
    )
    return IXTIYORIY_LINK


async def ixtiyoriy_get_link(update, context):
    link = update.message.text.strip()

    if link in ("❌ Bekor qilish", "/cancel"):
        await update.message.reply_text("❌ Bekor qilindi.", reply_markup=get_admin_menu_keyboard())
        return ConversationHandler.END

    if not (link.startswith("https://t.me/") or link.startswith("http://t.me/")):
        await update.message.reply_text(
            "⚠️ Noto'g'ri link formati.\n"
            "https://t.me/username shaklida yuboring:"
        )
        return IXTIYORIY_LINK

    context.user_data["ixt_link"] = link
    await update.message.reply_text(
        "2️⃣ Kanal nomini yuboring:\n"
        "Misol: 🎬 Kino Play\n\n"
        "❌ Bekor qilish uchun /cancel"
    )
    return IXTIYORIY_NOM


async def ixtiyoriy_get_nom(update, context):
    nom = update.message.text.strip()

    if nom in ("❌ Bekor qilish", "/cancel"):
        await update.message.reply_text("❌ Bekor qilindi.", reply_markup=get_admin_menu_keyboard())
        return ConversationHandler.END

    if not nom:
        await update.message.reply_text("Nom bo'sh bo'lmasin. Qayta yuboring:")
        return IXTIYORIY_NOM

    context.user_data["ixt_nom"] = nom
    await update.message.reply_text(
        "3️⃣ Kanal rasmi/logosini yuboring:\n"
        "(Ixtiyoriy — rasmsiz saqlash uchun pastdagi tugmani bosing)",
        reply_markup=ReplyKeyboardMarkup(
            [["⏭ Rasmsiz saqlash"]],
            resize_keyboard=True,
            one_time_keyboard=True,
        ),
    )
    return IXTIYORIY_RASM


async def ixtiyoriy_get_rasm(update, context):
    msg = update.message
    photo_id = ""

    if msg.text and msg.text.strip() in ("⏭ Rasmsiz saqlash", "❌ Bekor qilish", "/cancel"):
        if msg.text.strip() in ("❌ Bekor qilish", "/cancel"):
            await msg.reply_text("❌ Bekor qilindi.", reply_markup=get_admin_menu_keyboard())
            return ConversationHandler.END
        photo_id = ""
    elif msg.photo:
        photo_id = msg.photo[-1].file_id
    else:
        await msg.reply_text(
            "⚠️ Faqat rasm yuboring yoki tugmani bosing:",
            reply_markup=ReplyKeyboardMarkup(
                [["⏭ Rasmsiz saqlash"]],
                resize_keyboard=True,
                one_time_keyboard=True,
            ),
        )
        return IXTIYORIY_RASM

    link = context.user_data.get("ixt_link", "")
    nom = context.user_data.get("ixt_nom", "")

    try:
        add_optional_channel_db(link, nom, photo_id)
    except Exception:
        await msg.reply_text(SERVICE_UNAVAILABLE_TEXT, reply_markup=get_admin_menu_keyboard())
        return ConversationHandler.END

    photo_note = "✅ Rasm saqlandi" if photo_id else "📷 Rasmsiz saqlandi"
    await msg.reply_text(
        f"✅ Ixtiyoriy kanal qo'shildi!\n\n"
        f"📢 Nom: {nom}\n"
        f"🔗 Link: {link}\n"
        f"{photo_note}\n\n"
        f"Foydalanuvchilar '📋 Kanallar' tugmasida ko'radi.",
        reply_markup=get_admin_menu_keyboard(),
    )
    return ConversationHandler.END


# ===================== IXTIYORIY O'CHIRISH =====================

async def ixtiyoriy_remove_start(update, context):
    remember_user(update)
    if update.message.from_user.id != ADMIN_ID:
        return

    try:
        channels = get_all_optional_channels_db()
    except Exception:
        await reply_service_unavailable(update)
        return

    if not channels:
        await update.message.reply_text(
            "Hali ixtiyoriy kanal qo'shilmagan.\n\nQo'shish uchun /ixtiyoriyobuna",
            reply_markup=get_admin_menu_keyboard(),
        )
        return

    lines = ["📋 Ixtiyoriy kanallar:\n"]
    buttons = []
    for i, ch in enumerate(channels, 1):
        lines.append(f"{i}. {ch.get('title', '-')} — {ch.get('link', '-')}")
        buttons.append([InlineKeyboardButton(
            f"🗑 {ch.get('title', str(i))} ni o'chirish",
            callback_data=f"rm_ixt:{ch['link']}"
        )])
    buttons.append([InlineKeyboardButton("❌ Bekor", callback_data="rm_ixt:cancel")])

    await update.message.reply_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def ixtiyoriy_remove_callback(update, context):
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != ADMIN_ID:
        return

    action = query.data[len("rm_ixt:"):]
    if action == "cancel":
        await query.message.edit_reply_markup(reply_markup=None)
        return

    link = action
    try:
        channels = get_all_optional_channels_db()
        ch = next((c for c in channels if c["link"] == link), None)
        title = ch.get("title", link) if ch else link
        remove_optional_channel_db(link)
    except Exception:
        await query.message.reply_text("❌ O'chirishda xato.")
        return

    await query.message.edit_reply_markup(reply_markup=None)
    await query.message.reply_text(
        f"✅ '{title}' ixtiyoriy kanaldan o'chirildi.",
        reply_markup=get_admin_menu_keyboard(),
    )


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
        "📢 Nima yubormoqchisiz?",
        reply_markup=ReplyKeyboardMarkup(
            [["📝 Matn/Rasm yuborish", "📊 Sorovnoma yuborish"], ["❌ Bekor qilish"]],
            resize_keyboard=True,
            one_time_keyboard=True,
        ),
    )
    return BC_GET_TEXT


async def bc_get_text(update, context):
    text = update.message.text.strip()
    if text in ("❌ Bekor qilish", "/cancel"):
        await update.message.reply_text("❌ Bekor qilindi.", reply_markup=get_admin_menu_keyboard())
        return ConversationHandler.END

    # ===== POLL YO'LI =====
    if text == "📊 Sorovnoma yuborish":
        await update.message.reply_text(
            "📊 Sorovnoma savolini yozing:\n\n"
            "Misol: <b>Qaysi maktabda o'qiysiz?</b>\n\n"
            "❌ Bekor qilish uchun /cancel",
            parse_mode="HTML",
            reply_markup=get_bc_cancel_keyboard(),
        )
        return BC_POLL_QUESTION

    # ===== ODDIY MATN YO'LI =====
    if text == "📝 Matn/Rasm yuborish":
        await update.message.reply_text(
            "📝 Yubormoqchi bo'lgan matnni kiriting.\n"
            "Formatlash (HTML) ishlaydi:\n  <b>qalin matn</b>\n  <i>kursiv matn</i>\n  <code>kod</code>\n\n"
            "❌ Bekor qilish uchun /cancel",
            parse_mode="HTML",
            reply_markup=get_bc_cancel_keyboard(),
        )
        return BC_GET_TEXT

    # Agar admin to'g'ridan matn yozsa
    if not text:
        await update.message.reply_text("Matn bo'sh bo'lmasin. Qayta yuboring:")
        return BC_GET_TEXT

    context.user_data["bc_text"] = text
    context.user_data["bc_media"] = None
    context.user_data["bc_media_type"] = None
    await update.message.reply_text(
        "🖼 Xabarga rasm, GIF yoki video qo'shmoqchimisiz?\n\nRasm, GIF yoki video yuboring.\nYoki o'tkazib yuborish uchun tugmani bosing:",
        reply_markup=get_bc_media_skip_keyboard(),
    )
    return BC_GET_MEDIA


# ===================== POLL BROADCAST CONVERSATION =====================

async def bc_poll_get_question(update, context):
    """Poll savolini olish."""
    text = update.message.text.strip()
    if text in ("❌ Bekor qilish", "/cancel"):
        await update.message.reply_text("❌ Bekor qilindi.", reply_markup=get_admin_menu_keyboard())
        return ConversationHandler.END

    if not text:
        await update.message.reply_text("Savol bo'sh bo'lmasin. Qayta yozing:", reply_markup=get_bc_cancel_keyboard())
        return BC_POLL_QUESTION

    if len(text) > 300:
        await update.message.reply_text("⚠️ Savol 300 ta belgidan oshmasin. Qayta yozing:", reply_markup=get_bc_cancel_keyboard())
        return BC_POLL_QUESTION

    context.user_data["bc_poll_question"] = text
    context.user_data["bc_poll_options"] = []

    await update.message.reply_text(
        "✅ Savol qabul qilindi!\n\n"
        "📝 Endi javob variantlarini yuboring — <b>har birini alohida xabar</b> qilib yuboring.\n\n"
        "Kamida 2 ta, ko'pi bilan 10 ta variant bo'lishi mumkin.\n\n"
        "Variantlarni kiritib bo'lgach <b>✅ Tugatish</b> tugmasini bosing.\n\n"
        "❌ Bekor qilish uchun /cancel",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(
            [["✅ Tugatish"], ["❌ Bekor qilish"]],
            resize_keyboard=True,
            one_time_keyboard=False,
        ),
    )
    return BC_POLL_OPTIONS


async def bc_poll_get_options(update, context):
    """Poll variantlarini olish."""
    text = update.message.text.strip()

    if text in ("❌ Bekor qilish", "/cancel"):
        await update.message.reply_text("❌ Bekor qilindi.", reply_markup=get_admin_menu_keyboard())
        return ConversationHandler.END

    options = context.user_data.get("bc_poll_options", [])

    if text == "✅ Tugatish":
        if len(options) < 2:
            await update.message.reply_text(
                f"⚠️ Kamida 2 ta variant kerak. Hozir {len(options)} ta bor.\n\nDavom eting:",
                reply_markup=ReplyKeyboardMarkup(
                    [["✅ Tugatish"], ["❌ Bekor qilish"]],
                    resize_keyboard=True,
                    one_time_keyboard=False,
                ),
            )
            return BC_POLL_OPTIONS

        # Preview ko'rsatish
        question = context.user_data.get("bc_poll_question", "")
        options_text = "\n".join([f"  {i+1}. {opt}" for i, opt in enumerate(options)])
        await update.message.reply_text(
            f"📊 Sorovnoma ko'rinishi:\n\n"
            f"❓ <b>{escape_html(question)}</b>\n\n"
            f"{escape_html(options_text)}\n\n"
            f"🔒 Anonim ovoz berish\n"
            f"👁 Natijalar faqat adminga ko'rinadi\n\n"
            f"Yuborishni tasdiqlaysizmi?",
            parse_mode="HTML",
            reply_markup=ReplyKeyboardRemove(),
        )
        confirm_markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Yuborish", callback_data="bc_poll_yes"),
            InlineKeyboardButton("✏️ Qayta", callback_data="bc_poll_edit"),
            InlineKeyboardButton("❌ Bekor", callback_data="bc_cancel"),
        ]])
        await update.message.reply_text("Tanlov:", reply_markup=confirm_markup)
        return BC_POLL_CONFIRM

    # Variant qo'shish
    if not text:
        await update.message.reply_text("Variant bo'sh bo'lmasin.")
        return BC_POLL_OPTIONS

    if len(text) > 100:
        await update.message.reply_text("⚠️ Variant 100 ta belgidan oshmasin. Qayta yozing:")
        return BC_POLL_OPTIONS

    if len(options) >= 10:
        await update.message.reply_text(
            "⚠️ Ko'pi bilan 10 ta variant bo'lishi mumkin.\n✅ Tugatish tugmasini bosing.",
            reply_markup=ReplyKeyboardMarkup(
                [["✅ Tugatish"], ["❌ Bekor qilish"]],
                resize_keyboard=True,
                one_time_keyboard=False,
            ),
        )
        return BC_POLL_OPTIONS

    options.append(text)
    context.user_data["bc_poll_options"] = options

    await update.message.reply_text(
        f"✅ Variant qo'shildi: <b>{escape_html(text)}</b>\n"
        f"Jami: {len(options)} ta variant\n\n"
        f"Yana variant yuboring yoki ✅ Tugatish tugmasini bosing.",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(
            [["✅ Tugatish"], ["❌ Bekor qilish"]],
            resize_keyboard=True,
            one_time_keyboard=False,
        ),
    )
    return BC_POLL_OPTIONS


async def bc_poll_confirm_callback(update, context):
    """Poll broadcast tasdiqlash."""
    global _broadcast_active, _broadcast_status_message_id
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

    if query.data == "bc_poll_edit":
        await query.message.edit_reply_markup(reply_markup=None)
        context.user_data["bc_poll_options"] = []
        await query.message.reply_text(
            "Yangi savol yozing:\n\n❌ Bekor qilish uchun /cancel",
            reply_markup=get_bc_cancel_keyboard(),
        )
        return BC_POLL_QUESTION

    if query.data == "bc_poll_yes":
        await query.message.edit_reply_markup(reply_markup=None)

        question = context.user_data.get("bc_poll_question", "")
        options = context.user_data.get("bc_poll_options", [])

        try:
            user_ids = get_all_user_ids()
        except Exception:
            await query.message.reply_text(SERVICE_UNAVAILABLE_TEXT)
            return ConversationHandler.END

        if not user_ids:
            await query.message.reply_text("Bazada foydalanuvchilar topilmadi.", reply_markup=get_admin_menu_keyboard())
            return ConversationHandler.END

        total = len(user_ids)
        taxminiy = round(total * 0.09 / 60)
        chat_id = update.effective_chat.id

        # Admin o'zi uchun preview
        try:
            await context.bot.send_poll(
                chat_id=chat_id,
                question=question,
                options=options,
                is_anonymous=True,
                allows_multiple_answers=False,
            )
        except Exception:
            logger.exception("Admin preview poll yuborishda xato")

        status_msg = await context.bot.send_message(
            chat_id=chat_id,
            text=f"📊 Sorovnoma yuborish boshlandi!\nJami: {total} ta foydalanuvchi\nTaxminiy vaqt: ~{taxminiy} daqiqa",
            reply_markup=get_broadcast_stop_keyboard(),
        )
        _broadcast_status_message_id = status_msg.message_id
        _broadcast_active = True
        bot = context.bot

        async def do_send_poll():
            global _broadcast_active, _broadcast_status_message_id
            sent = 0
            failed = 0
            blocked = 0
            for uid in user_ids:
                if not _broadcast_active:
                    break
                try:
                    await bot.send_poll(
                        chat_id=int(uid),
                        question=question,
                        options=options,
                        is_anonymous=True,
                        allows_multiple_answers=False,
                    )
                    sent += 1
                except Exception as e:
                    err = str(e).lower()
                    if any(k in err for k in ["blocked", "deactivated", "not found", "chat not found"]):
                        blocked += 1
                    else:
                        failed += 1
                await asyncio.sleep(0.09)

            await _remove_broadcast_stop_button(bot, chat_id, _broadcast_status_message_id)
            stopped_early = not _broadcast_active
            _broadcast_active = False
            lines = ["⛔ Broadcast to'xtatildi!" if stopped_early else "✅ Sorovnoma yuborish tugadi!"]
            lines.append(f"Muvaffaqiyatli: {sent} ta")
            if blocked:
                lines.append(f"Bot bloklagan: {blocked} ta")
            if failed:
                lines.append(f"Boshqa xato: {failed} ta")
            try:
                await bot.send_message(chat_id=chat_id, text="\n".join(lines), reply_markup=get_admin_menu_keyboard())
            except Exception:
                pass

        asyncio.create_task(do_send_poll())
        return ConversationHandler.END

    return BC_POLL_CONFIRM


# ===================== ODDIY BROADCAST (mavjud) =====================

async def bc_get_media(update, context):
    msg = update.message
    if msg.text and msg.text.strip() in ("⏭ Media qo'shmasdan o'tish", "❌ Bekor qilish", "/cancel"):
        if msg.text.strip() in ("❌ Bekor qilish", "/cancel"):
            await msg.reply_text("❌ Bekor qilindi.", reply_markup=get_admin_menu_keyboard())
            return ConversationHandler.END
        context.user_data["bc_media"] = None
        context.user_data["bc_media_type"] = None
        await msg.reply_text("🔗 Xabarga inline tugma qo'shmoqchimisiz?", reply_markup=get_bc_ask_button_keyboard())
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
        await msg.reply_text("⚠️ Faqat rasm, GIF yoki video yuboring.", reply_markup=get_bc_media_skip_keyboard())
        return BC_GET_MEDIA

    await msg.reply_text(f"✅ {media_label} qabul qilindi!\n\n🔗 Xabarga inline tugma qo'shmoqchimisiz?", reply_markup=get_bc_ask_button_keyboard())
    return BC_ASK_BUTTON


async def bc_ask_button(update, context):
    choice = update.message.text.strip()
    if choice in ("❌ Bekor qilish", "/cancel"):
        await update.message.reply_text("❌ Bekor qilindi.", reply_markup=get_admin_menu_keyboard())
        return ConversationHandler.END
    if choice == "✅ Ha, tugma qo'shish":
        await update.message.reply_text("🌐 Tugma URL manzilini yuboring:\nMisol: https://t.me/kanalim", reply_markup=get_bc_cancel_keyboard())
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
        await update.message.reply_text("⚠️ URL noto'g'ri. https:// bilan boshlang.", reply_markup=get_bc_cancel_keyboard())
        return BC_GET_URL
    context.user_data["bc_url"] = url
    await update.message.reply_text("✏️ Tugma ustida ko'rinadigan matnni yuboring:\nMisol: 📢 Kanalga o'tish", reply_markup=get_bc_cancel_keyboard())
    return BC_GET_BTN_NAME


async def bc_get_btn_name(update, context):
    btn_name = update.message.text.strip()
    if btn_name in ("❌ Bekor qilish", "/cancel"):
        await update.message.reply_text("❌ Bekor qilindi.", reply_markup=get_admin_menu_keyboard())
        return ConversationHandler.END
    if not btn_name:
        await update.message.reply_text("Tugma nomi bo'sh bo'lmasin.")
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
            await update.message.reply_text(f"👁 Ko'rinishi (media preview xato):\n\n{bc_text}", reply_markup=user_markup)
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
                if "parse" in str(e).lower():
                    await bot.send_photo(chat_id=uid, photo=bc_media, caption=bc_text, reply_markup=user_markup)
                else:
                    raise
        elif bc_media_type == "animation":
            try:
                await bot.send_animation(chat_id=uid, animation=bc_media, caption=bc_text, reply_markup=user_markup, parse_mode="HTML")
            except Exception as e:
                if "parse" in str(e).lower():
                    await bot.send_animation(chat_id=uid, animation=bc_media, caption=bc_text, reply_markup=user_markup)
                else:
                    raise
        elif bc_media_type == "video":
            try:
                await bot.send_video(chat_id=uid, video=bc_media, caption=bc_text, reply_markup=user_markup, parse_mode="HTML")
            except Exception as e:
                if "parse" in str(e).lower():
                    await bot.send_video(chat_id=uid, video=bc_media, caption=bc_text, reply_markup=user_markup)
                else:
                    raise
    else:
        try:
            await bot.send_message(chat_id=uid, text=bc_text, reply_markup=user_markup, parse_mode="HTML")
        except Exception as e:
            if "parse" in str(e).lower():
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
            await query.message.reply_text(SERVICE_UNAVAILABLE_TEXT)
            return ConversationHandler.END

        if not user_ids:
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
                _admin_sent_messages[ADMIN_ID].append({
                    "msg_id": preview_msg.message_id,
                    "chat_id": chat_id,
                    "preview": bc_text[:50].replace("\n", " "),
                })
        except Exception:
            logger.exception("Admin preview yuborishda xato")

        status_msg = await context.bot.send_message(
            chat_id=chat_id,
            text=f"📢 Yuborish boshlandi!\nJami: {total} ta foydalanuvchi\nTaxminiy vaqt: ~{taxminiy} daqiqa",
            reply_markup=get_broadcast_stop_keyboard(),
        )
        _broadcast_status_message_id = status_msg.message_id
        _broadcast_active = True
        bot = context.bot

        async def do_send():
            global _broadcast_active, _broadcast_status_message_id
            sent = 0; failed = 0; blocked = 0
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
            "📭 O'chiriladigan xabar yo'q.\n\n(Bot qayta ishga tushsa ro'yxat tozalanadi)",
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
            await query.message.reply_text(f"⚠️ O'chirishda xato: {e}", reply_markup=get_admin_menu_keyboard())
    else:
        await query.message.edit_reply_markup(reply_markup=None)


# ===================== ADMIN HELP & STAT =====================

async def admin_help(update, context):
    remember_user(update)
    if update.message.from_user.id != ADMIN_ID:
        return
    help_text = (
        "Admin buyruqlari:\n\n"
        "Kino qo'shish: Video yoki fayl yuboring\n"
        "Tahrirlash: /edit\n"
        "O'chirish: /delete <kod>\n"
        "Jildlar: /jild\n"
        "Seriallar: /seriallist\n"
        "Kanal boshqaruvi: /addchannel <link>, /removechannel, /kanallar\n"
        "Ixtiyoriy kanallar: /ixtiyoriyobuna, /ixtiyoriyochirish\n"
        "Statistika: /foydalanuvchi 777, /stat, /top, /barchasi\n"
        "Ommaviy xabar: /adminlik, /ochirish\n"
        "Sevimlilar: /sevimli\n\n"
        "Broadcast: /adminlik — matn yoki sorovnoma yuborish\n"
        "Sorovnoma: /adminlik → '📊 Sorovnoma yuborish' → savol → variantlar"
    )
    await update.message.reply_text(help_text, reply_markup=get_admin_menu_keyboard())


async def admin_stat(update, context):
    remember_user(update)
    if update.message.from_user.id != ADMIN_ID:
        return
    try:
        total_users = get_tracked_user_count()
        total_movies = run_db(lambda col: col.count_documents({}))
        total_folders = run_folders_db(lambda col: col.count_documents({}))
        total_series = run_series_db(lambda col: col.count_documents({}))
        last_code, next_code = get_last_and_next_movie_code()
        total_channels = run_channels_db(lambda col: col.count_documents({"type": {"$ne": "optional"}}))
        total_optional = run_channels_db(lambda col: col.count_documents({"type": "optional"}))
    except Exception:
        await reply_service_unavailable(update)
        return
    await update.message.reply_text(
        f"Bot statistikasi:\n\n"
        f"Foydalanuvchilar: {total_users} ta\n"
        f"Kinolar: {total_movies} ta\n"
        f"Jildlar: {total_folders} ta\n"
        f"Serial diapazonlari: {total_series} ta\n"
        f"Majburiy kanallar: {total_channels} ta\n"
        f"Ixtiyoriy kanallar: {total_optional} ta\n"
        f"Oxirgi kiritilgan kod: {last_code}\n"
        f"Keyingi tavsiya kod: {next_code}"
    )


async def admin_all_movies(update, context):
    remember_user(update)
    if update.message.from_user.id != ADMIN_ID:
        return
    try:
        movies = get_all_movies_list(limit=500)
    except Exception:
        await reply_service_unavailable(update)
        return
    if not movies:
        await update.message.reply_text("Bazada hali kino yo'q.")
        return
    lines = [f"🎬 Barcha kinolar ({len(movies)} ta):\n"]
    for i, movie in enumerate(movies, start=1):
        lines.append(f"{i}. {movie.get('nom', '-')}  |  Kod: {movie.get('code', '-')}")
    text = "\n".join(lines)
    if len(text) <= 4096:
        await update.message.reply_text(text)
    else:
        chunks = []
        chunk_lines = []
        chunk_len = 0
        for line in lines:
            if chunk_len + len(line) + 1 > 4000:
                chunks.append("\n".join(chunk_lines))
                chunk_lines = [line]
                chunk_len = len(line)
            else:
                chunk_lines.append(line)
                chunk_len += len(line) + 1
        if chunk_lines:
            chunks.append("\n".join(chunk_lines))
        for chunk in chunks:
            await update.message.reply_text(chunk)


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
        await reply_service_unavailable(update)
        return ConversationHandler.END
    if existing_movie:
        await update.message.reply_text(
            f"⚠️ Bu fayl allaqachon bazada bor.\nKod: {existing_movie['code']}\nNom: {existing_movie['nom']}"
        )
        return ConversationHandler.END
    duration_seconds = update.message.video.duration or 0
    if duration_seconds > 0:
        context.user_data["vaqt"] = seconds_to_hhmmss(duration_seconds)
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
    vaqt_info = f"\n⏱️ Davomiylik: {context.user_data['vaqt']}" if context.user_data["vaqt_auto"] else ""
    await update.message.reply_text(
        f"Oxirgi kod: {last_code}\nTavsiya: {next_code}{vaqt_info}\n\nKodini kiriting:",
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
        await reply_service_unavailable(update)
        return ConversationHandler.END
    if existing_movie:
        await update.message.reply_text(
            f"⚠️ Bu fayl allaqachon bazada bor.\nKod: {existing_movie['code']}\nNom: {existing_movie['nom']}"
        )
        return ConversationHandler.END
    try:
        last_code, next_code = get_last_and_next_movie_code()
    except Exception:
        last_code, next_code = "?", "?"
    await update.message.reply_text(
        f"Oxirgi kod: {last_code}\nTavsiya: {next_code}\n\nKodini kiriting:",
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
        await update.message.reply_text(f"Kod faqat raqam bo'lsin. Tavsiya: {next_code}", reply_markup=get_kod_suggestion_keyboard(next_code))
        return KOD_VAQT
    try:
        exists = movie_exists(code)
    except Exception:
        await reply_service_unavailable(update)
        return ConversationHandler.END
    if exists:
        try:
            _, next_code = get_last_and_next_movie_code()
        except Exception:
            next_code = "?"
        await update.message.reply_text(f"⚠️ {code} kodi allaqachon mavjud! Tavsiya: {next_code}", reply_markup=get_kod_suggestion_keyboard(next_code))
        return KOD_VAQT
    d["kod"] = code
    await update.message.reply_text("Kino nomini kiriting:", reply_markup=ReplyKeyboardRemove())
    return NOM


async def get_nom(update, context):
    context.user_data["nom"] = update.message.text.strip()
    await update.message.reply_text("Sifatini tanlang:", reply_markup=get_sifat_keyboard())
    return SIFAT


async def get_sifat(update, context):
    raw = update.message.text.strip()
    context.user_data["sifat"] = (context.user_data.get("last_sifat") or DEFAULT_SIFAT) if raw == KEEP_PREVIOUS_TEXT else (raw or DEFAULT_SIFAT)
    await update.message.reply_text("Tilini tanlang:", reply_markup=get_til_keyboard())
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
        await update.message.reply_text("Davomiyligini kiriting (masalan: 1:57:36 yoki - ):", reply_markup=ReplyKeyboardRemove())
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
    data = {"type": d["file_type"], "file_id": d["file_id"], "nom": d["nom"], "sifat": d["sifat"], "til": d["til"], "vaqt": d["vaqt"]}
    try:
        save_movie(code, data)
    except Exception:
        await reply_service_unavailable(update)
        return ConversationHandler.END
    d["last_sifat"] = d["sifat"]
    d["last_til"] = d["til"]
    d.pop("vaqt_locked", None)
    d.pop("vaqt_draft", None)
    d.pop("vaqt_auto", None)
    await update.message.reply_text("📁 Jildga saqlashni xohlaysizmi?", reply_markup=get_folder_choice_keyboard())
    return FOLDER_CHOICE


async def finish_movie_save(update, context, folder_note=None):
    d = context.user_data
    note_text = f"\n{folder_note}\n" if folder_note else "\n"
    await update.message.reply_text(
        f"✅ Saqlandi.\n\nKod: {d['kod']}\nNom: {d['nom']}\nSifat: {d['sifat']}\nTil: {d['til']}\nDavomiylik: {d['vaqt']}\n{note_text}"
        f"Keyingi kino uchun yana video yoki fayl yuboring.",
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
        await update.message.reply_text("Jild nomi bo'sh bo'lmasin.")
        return FOLDER_CREATE
    try:
        exists = folder_exists_by_name(folder_name)
    except Exception:
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
        await reply_service_unavailable(update)
        return ConversationHandler.END
    if value not in folder_names:
        await update.message.reply_text("Ro'yxatdan jild tanlang.", reply_markup=build_folder_list_keyboard(folder_names))
        return FOLDER_PICK
    return await save_to_folder_and_finish(update, context, value)


# ===================== JILD =====================

async def jild_start(update, context):
    remember_user(update)
    if update.message.from_user.id != ADMIN_ID:
        return ConversationHandler.END
    context.user_data["jild_codes"] = []
    await update.message.reply_text(
        f"Jild yaratish boshlandi.\nKino kodlarini yuboring.\nTayyor bo'lganda {JILD_FINISH_TEXT} tugmasini bosing.",
        reply_markup=get_jild_codes_keyboard(),
    )
    return JILD_CODES


async def jild_get_codes(update, context):
    value = update.message.text.strip()
    d = context.user_data
    current_codes = set(d.get("jild_codes", []))

    if value == JILD_CLEAR_TEXT:
        d["jild_codes"] = []
        await update.message.reply_text("Kodlar tozalandi.", reply_markup=get_jild_codes_keyboard())
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
        await update.message.reply_text(f"Noto'g'ri: {', '.join(invalid_tokens)}", reply_markup=get_jild_codes_keyboard())
        return JILD_CODES
    if not parsed_codes:
        await update.message.reply_text("Kodlarni kiriting.", reply_markup=get_jild_codes_keyboard())
        return JILD_CODES

    try:
        existing_codes = set(get_existing_movie_codes(parsed_codes))
    except Exception:
        await reply_service_unavailable(update)
        return ConversationHandler.END

    missing_codes = [c for c in parsed_codes if c not in existing_codes]
    new_codes = [c for c in parsed_codes if c in existing_codes and c not in current_codes]
    current_codes.update(existing_codes)
    d["jild_codes"] = sort_codes_for_folder(list(current_codes))

    lines = [f"✅ Qo'shildi: {len(new_codes)} ta", f"Jami: {len(d['jild_codes'])} ta", f"Ro'yxat: {format_codes_for_text(d['jild_codes'])}"]
    if missing_codes:
        lines.append(f"⚠️ Topilmagan: {', '.join(missing_codes)}")
    if invalid_tokens:
        lines.append(f"⚠️ Noto'g'ri: {', '.join(invalid_tokens)}")
    lines.append(f"Tayyor bo'lsa {JILD_FINISH_TEXT} tugmasini bosing.")
    await update.message.reply_text("\n".join(lines), reply_markup=get_jild_codes_keyboard())
    return JILD_CODES


async def jild_get_name(update, context):
    folder_name = update.message.text.strip()
    if not folder_name:
        await update.message.reply_text("Jild nomi bo'sh bo'lmasin.")
        return JILD_NAME
    d = context.user_data
    codes = d.get("jild_codes", [])
    if not codes:
        await update.message.reply_text("Kodlar topilmadi. /jild buyrug'ini qayta ishga tushiring.", reply_markup=ReplyKeyboardRemove())
        return ConversationHandler.END
    try:
        existed_before = folder_exists_by_name(folder_name)
        add_movies_to_folder(folder_name, codes)
        folder_movies = get_movies_for_folder(folder_name)
    except Exception:
        await reply_service_unavailable(update)
        return ConversationHandler.END
    d.pop("jild_codes", None)
    action_text = "yangilandi" if existed_before else "yaratildi"
    await update.message.reply_text(
        f"✅ Jild {action_text}.\nNomi: {folder_name}\nJilddagi kinolar: {len(folder_movies)} ta",
        reply_markup=ReplyKeyboardRemove(),
    )
    return ConversationHandler.END


async def cancel(update, context):
    await update.message.reply_text("❌ Bekor qilindi.", reply_markup=get_admin_menu_keyboard())
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
        f"Joriy: Nom: {data['nom']} | Sifat: {data['sifat']} | Til: {data['til']} | Vaqt: {data['vaqt']}\n\nYangi nomni kiriting (bo'sh — saqlash):"
    )
    return EDIT_NOM


async def edit_get_nom(update, context):
    new_nom = update.message.text.strip()
    context.user_data['nom'] = new_nom if new_nom else context.user_data['current_data']['nom']
    await update.message.reply_text("Yangi sifatni tanlang:", reply_markup=get_sifat_keyboard())
    return EDIT_SIFAT


async def edit_get_sifat(update, context):
    new_sifat = update.message.text.strip()
    context.user_data['sifat'] = new_sifat if (new_sifat and new_sifat != KEEP_PREVIOUS_TEXT) else context.user_data['current_data']['sifat']
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
    await update.message.reply_text("Yangi davomiylikni kiriting (bo'sh — saqlash):", reply_markup=ReplyKeyboardRemove())
    return EDIT_VAQT


async def edit_get_vaqt(update, context):
    new_vaqt = update.message.text.strip()
    d = context.user_data
    d['vaqt'] = new_vaqt if new_vaqt else d['current_data']['vaqt']
    code = d['edit_code']
    data = {"type": d['current_data']['type'], "file_id": d['current_data']['file_id'], "nom": d['nom'], "sifat": d['sifat'], "til": d['til'], "vaqt": d['vaqt']}
    try:
        save_movie(code, data)
    except Exception:
        await reply_service_unavailable(update)
        return ConversationHandler.END
    await update.message.reply_text("✅ Tahrirlandi!", reply_markup=get_admin_menu_keyboard())
    return ConversationHandler.END


# ===================== DELETE / FOYDALANUVCHI =====================

async def delete_movie(update, context):
    if update.message.from_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Ishlatish: /delete <kod>")
        return
    code = context.args[0]
    try:
        if not movie_exists(code):
            await update.message.reply_text(f"❌ {code} kodli kino topilmadi.")
            return
        delete_movie_db(code)
    except Exception:
        await reply_service_unavailable(update)
        return
    await update.message.reply_text(f"🗑️ {code} kodli kino o'chirildi.")


async def show_user_count(update, context):
    if update.message.from_user.id != ADMIN_ID:
        return
    if not context.args or context.args[0] != "777":
        await update.message.reply_text("Ishlatish: /foydalanuvchi 777")
        return
    try:
        total_users = get_tracked_user_count()
    except Exception:
        await reply_service_unavailable(update)
        return
    await update.message.reply_text(f"Foydalanuvchilar soni: {total_users}")


async def list_series_ranges(update, context):
    remember_user(update)
    if update.message.from_user.id != ADMIN_ID:
        return
    try:
        ranges = get_all_series_ranges()
    except Exception:
        await reply_service_unavailable(update)
        return
    if not ranges:
        await update.message.reply_text("Hali birorta ham serial diapazon saqlanmagan.")
        return
    lines = ["Serial diapazonlari:"]
    for item in ranges:
        lines.append(f"{item['start_code_num']}-{item['end_code_num']} | {item['title']}")
    await update.message.reply_text("\n".join(lines))


# ===================== KANAL ADMIN =====================

async def admin_add_channel(update, context):
    remember_user(update)
    if update.message.from_user.id != ADMIN_ID:
        return ConversationHandler.END
    if not context.args:
        await update.message.reply_text(
            "📢 Kanal qo'shish:\n\nPublic: /addchannel https://t.me/kanal_username\n"
            "Private: /addchannel https://t.me/+InviteLink -1001234567890\n\n"
            "⚠️ Bot kanalning ADMINISTRATORI bo'lishi shart!"
        )
        return ConversationHandler.END
    link = context.args[0].strip()
    if not (link.startswith("https://t.me/") or link.startswith("http://t.me/")):
        await update.message.reply_text("❌ Noto'g'ri link. t.me linki bo'lishi kerak.")
        return ConversationHandler.END
    context.user_data["addch_link"] = link
    context.user_data["addch_channel_id_arg"] = context.args[1] if len(context.args) >= 2 else None
    await update.message.reply_text("✏️ Tugmada ko'rinadigan kanal nomini yozing:", reply_markup=ReplyKeyboardRemove())
    return ADDCH_GET_TITLE


async def addch_get_title(update, context):
    button_title = update.message.text.strip()
    if button_title in ("❌ Bekor qilish", "/cancel"):
        await update.message.reply_text("❌ Bekor qilindi.", reply_markup=get_admin_menu_keyboard())
        return ConversationHandler.END
    if not button_title:
        await update.message.reply_text("Nom bo'sh bo'lmasin.")
        return ADDCH_GET_TITLE
    d = context.user_data
    link = d["addch_link"]
    channel_id_arg = d.get("addch_channel_id_arg")
    processing_msg = await update.message.reply_text("⏳ Kanal tekshirilmoqda...")
    if channel_id_arg:
        try:
            channel_id = int(channel_id_arg)
        except ValueError:
            await processing_msg.edit_text("❌ Kanal ID raqam bo'lishi kerak.")
            return ConversationHandler.END
        try:
            chat = await context.bot.get_chat(channel_id)
            title = chat.title or str(channel_id)
        except Exception as e:
            await processing_msg.edit_text(f"❌ Kanal topilmadi: {e}")
            return ConversationHandler.END
        add_required_channel(link, channel_id, title, button_title)
        await processing_msg.edit_text(f"✅ Kanal qo'shildi!\n📢 {title}\n🔘 {button_title}\n🆔 {channel_id}")
        return ConversationHandler.END
    if "/+" in link:
        await processing_msg.edit_text("⚠️ Private link uchun ID ham yozing:\n/addchannel https://t.me/+Link -1001234567890")
        return ConversationHandler.END
    try:
        username = link.rstrip("/").split("/")[-1]
        if not username.startswith("@"):
            username = "@" + username
        chat = await context.bot.get_chat(username)
        channel_id = chat.id
        title = chat.title or username
        add_required_channel(link, channel_id, title, button_title)
        await processing_msg.edit_text(f"✅ Kanal qo'shildi!\n📢 {title}\n🔘 {button_title}\n🆔 {channel_id}")
    except Exception as e:
        await processing_msg.edit_text(f"❌ Xato: {e}\nBot kanal admini bo'lishi shart!")
    return ConversationHandler.END


async def admin_remove_channel(update, context):
    remember_user(update)
    if update.message.from_user.id != ADMIN_ID:
        return
    try:
        channels = get_all_required_channels()
    except Exception:
        await reply_service_unavailable(update)
        return
    if not channels:
        await update.message.reply_text("Hali majburiy kanal qo'shilmagan.")
        return
    lines = ["📋 Majburiy kanallar:\n"]
    for i, ch in enumerate(channels, 1):
        btn_label = ch.get("button_title") or ch.get("title", "-")
        lines.append(f"{i}. {btn_label} — {ch.get('join_count', 0)} ta kirish")
    buttons = [[InlineKeyboardButton(f"🗑 {ch.get('button_title') or ch.get('title', str(ch['channel_id']))} ni o'chirish", callback_data=f"rmchannel:{ch['channel_id']}")] for ch in channels]
    buttons.append([InlineKeyboardButton("❌ Bekor", callback_data="rmchannel:cancel")])
    await update.message.reply_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(buttons))


async def admin_remove_channel_callback(update, context):
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != ADMIN_ID:
        return
    action = query.data.split(":", 1)[1]
    if action == "cancel":
        await query.message.edit_reply_markup(reply_markup=None)
        return
    try:
        channel_id = int(action)
    except ValueError:
        return
    try:
        channels = get_all_required_channels()
        ch = next((c for c in channels if c["channel_id"] == channel_id), None)
        title = (ch.get("button_title") or ch.get("title", str(channel_id))) if ch else str(channel_id)
        remove_required_channel(channel_id)
    except Exception:
        await query.message.reply_text("❌ O'chirishda xato.")
        return
    await query.message.edit_reply_markup(reply_markup=None)
    await query.message.reply_text(f"✅ '{title}' kanali o'chirildi.", reply_markup=get_admin_menu_keyboard())


async def admin_channels_stat(update, context):
    remember_user(update)
    if update.message.from_user.id != ADMIN_ID:
        return
    try:
        channels = get_all_required_channels()
    except Exception:
        await reply_service_unavailable(update)
        return
    if not channels:
        await update.message.reply_text("Hali majburiy kanal qo'shilmagan.")
        return
    lines = ["📊 Kanallar statistikasi:\n"]
    total_joins = 0
    for i, ch in enumerate(channels, 1):
        join_count = ch.get("join_count", 0)
        total_joins += join_count
        lines.append(f"{i}. 📢 {ch.get('title', '-')}\n   🔘 {ch.get('button_title') or ch.get('title', '-')}\n   🔗 {ch.get('link', '-')}\n   👥 {join_count} ta kirish\n")
    lines.append(f"─────────────────\nJami: {total_joins} ta")
    await update.message.reply_text("\n".join(lines))


async def handle_check_subscription_callback(update, context):
    remember_user(update)
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id

    invalidate_subscription_cache(user_id)

    is_subscribed, not_subscribed = await check_user_subscribed(context.bot, user_id)
    if is_subscribed:
        await query.message.edit_reply_markup(reply_markup=None)
        await query.message.reply_text(
            "✅ Rahmat! Obuna tasdiqlandi.\n\nEndi kino kodini yuboring yoki <b>🔍 Kino qidirish</b> tugmasini bosing.",
            parse_mode="HTML",
            reply_markup=get_user_menu_keyboard(),
        )
    else:
        try:
            await query.message.edit_reply_markup(reply_markup=get_subscribe_keyboard(not_subscribed))
        except Exception:
            pass
        await query.answer("⚠️ Siz hali barcha kanallarga obuna bo'lmadingiz!", show_alert=True)


async def handle_channel_join_update(update, context):
    if update.chat_member is None:
        return
    new_member = update.chat_member.new_chat_member
    old_member = update.chat_member.old_chat_member
    was_member = old_member.status in ("member", "administrator", "creator")
    is_member = new_member.status in ("member", "administrator", "creator")
    if not was_member and is_member:
        channel_id = update.chat_member.chat.id
        increment_channel_join_count(channel_id)


# ===================== CALLBACKS =====================

async def handle_series_part_callback(update, context):
    remember_user(update)
    query = update.callback_query
    if query is None or query.message is None:
        return
    await query.answer()
    code = query.data[len(SERIES_CALLBACK_PREFIX):]
    user_id = update.effective_user.id if update.effective_user else None
    try:
        data = get_movie(code)
    except Exception:
        await reply_service_unavailable(update)
        return
    if not data:
        await query.message.reply_text(f"❌ {code} kodli kino topilmadi.")
        return
    increment_view_count(code)
    if user_id and user_id != ADMIN_ID:
        increment_user_view_count(user_id)
    await send_movie_to_chat(query.message, code, data, user_id=user_id)


async def handle_favorite_callback(update, context):
    remember_user(update)
    query = update.callback_query
    if query is None:
        return
    await query.answer()
    user_id = update.effective_user.id
    code = query.data[len("fav:"):]
    try:
        in_fav = is_favorite(user_id, code)
        if in_fav:
            remove_from_favorites(user_id, code)
            new_text = "🤍 Sevimlilarga qo'shish"
            notice = "Sevimlilardan olib tashlandi."
        else:
            add_to_favorites(user_id, code)
            new_text = "❤️ Sevimlilardan chiqarish"
            notice = "❤️ Sevimlilarga qo'shildi!"
    except Exception:
        await query.answer("Xato yuz berdi.", show_alert=True)
        return
    try:
        old_markup = query.message.reply_markup
        if old_markup:
            new_rows = []
            for row in old_markup.inline_keyboard:
                new_row = []
                for btn in row:
                    if btn.callback_data == query.data:
                        new_row.append(InlineKeyboardButton(new_text, callback_data=query.data))
                    else:
                        new_row.append(btn)
                new_rows.append(new_row)
            await query.message.edit_reply_markup(InlineKeyboardMarkup(new_rows))
    except Exception:
        pass
    await query.answer(notice, show_alert=True)


# ===================== SEVIMLILAR =====================

async def show_favorites(update, context):
    remember_user(update)
    user_id = update.message.from_user.id
    try:
        fav_codes = get_favorites(user_id)
    except Exception:
        await reply_service_unavailable(update)
        return
    if not fav_codes:
        await update.message.reply_text(
            "Sevimlilar ro'yxatingiz hali bo'sh.\n\nKino ko'rganingizda 🤍 tugmani bosib qo'shing!",
            reply_markup=get_user_menu_keyboard(),
        )
        return
    try:
        movies_info = run_db(lambda col: list(col.find({"code": {"$in": fav_codes}}, {"_id": 0, "code": 1, "nom": 1})))
    except Exception:
        await reply_service_unavailable(update)
        return
    code_to_nom = {m["code"]: m.get("nom", "-") for m in movies_info}
    lines = [f"❤️ Sevimli kinolaringiz ({len(fav_codes)} ta):\n"]
    for i, code in enumerate(fav_codes, start=1):
        nom = code_to_nom.get(code, "Noma'lum")
        lines.append(f"{i}. {nom}  |  Kod: {code}")
    lines.append("\nKino olish uchun kodini yuboring.")
    await update.message.reply_text("\n".join(lines), reply_markup=get_user_menu_keyboard())


# ===================== KO'RILISH SONI =====================

def increment_view_count(code):
    try:
        run_db(lambda col: col.update_one({"code": code}, {"$inc": {"views": 1}}))
    except Exception:
        pass


async def admin_top_movies(update, context):
    remember_user(update)
    if update.message.from_user.id != ADMIN_ID:
        return
    limit = 20
    if context.args:
        try:
            limit = min(int(context.args[0]), 200)
        except ValueError:
            pass
    try:
        top_movies = run_db(lambda col: list(col.aggregate([
            {"$match": {"views": {"$gt": 0}}},
            {"$sort": {"views": -1}},
            {"$limit": limit},
            {"$project": {"_id": 0, "code": 1, "nom": 1, "views": 1}},
        ])))
    except Exception:
        await reply_service_unavailable(update)
        return
    if not top_movies:
        await update.message.reply_text("Hali birorta ham kino ko'rilmagan.")
        return
    lines = [f"Eng ko'p ko'rilgan {len(top_movies)} ta kino:\n"]
    for i, movie in enumerate(top_movies, start=1):
        lines.append(f"{i}. {movie.get('nom', '-')}  |  Kod: {movie.get('code', '-')}  |  {movie.get('views', 0)} marta")
    text = "\n".join(lines)
    if len(text) <= 4096:
        await update.message.reply_text(text)
    else:
        chunks = []
        chunk = ""
        for line in lines:
            if len(chunk) + len(line) + 1 > 4096:
                chunks.append(chunk)
                chunk = line
            else:
                chunk += "\n" + line if chunk else line
        if chunk:
            chunks.append(chunk)
        for chunk in chunks:
            await update.message.reply_text(chunk)


# ===================== ASOSIY MESSAGE HANDLER =====================

async def handle_message(update, context):
    remember_user(update)
    user_id = update.message.from_user.id
    text = update.message.text.strip()

    if user_id != ADMIN_ID:
        if text == USER_BTN_REKLAMA:
            await handle_user_reklama(update, context)
            return
        if text == USER_BTN_YANGI:
            await handle_user_yangi_filmlar(update, context)
            return
        if text == USER_BTN_SEVIMLI:
            await show_favorites(update, context)
            return
        if text == USER_BTN_PROFIL:
            await handle_user_profil(update, context)
            return
        if text == USER_BTN_OBUNA:
            await handle_user_obuna(update, context)
            return
        if text == USER_BTN_ALOQA:
            await handle_user_aloqa(update, context)
            return

        if VERIFICATION_BOT_URL:
            started_at = get_user_started_at(user_id)
            if started_at is None:
                await update.message.reply_text(
                    "⚠️ Botdan foydalanish uchun avval quyidagi botga o'ting va /start bosing:",
                    reply_markup=get_verification_keyboard(),
                )
                return
            elapsed = int(time.time()) - started_at
            if elapsed < VERIFICATION_WAIT_SECONDS:
                remaining = VERIFICATION_WAIT_SECONDS - elapsed
                await update.message.reply_text(f"⏳ Iltimos, yana {remaining} soniya kuting.")
                return

        is_subscribed, not_subscribed = await check_user_subscribed(context.bot, user_id)
        if not is_subscribed:
            await send_subscribe_required_message(update.message, not_subscribed)
            return

    if text.isdigit():
        code = text
        try:
            folder_data = get_folder_by_code(code)
        except Exception:
            await reply_service_unavailable(update)
            return
        if folder_data is not None:
            try:
                folder_movies = get_movies_for_folder(folder_data["name"])
            except Exception:
                await reply_service_unavailable(update)
                return
            if folder_movies:
                await send_folder_parts_prompt(update.message, folder_data, folder_movies)
                return
        try:
            series_data = get_series_range_by_code(code)
        except Exception:
            await reply_service_unavailable(update)
            return
        if series_data is not None:
            try:
                movies = get_movies_in_range(series_data["start_code_num"], series_data["end_code_num"])
            except Exception:
                await reply_service_unavailable(update)
                return
            if movies:
                await send_series_parts_prompt(update.message, series_data, movies)
                return
        try:
            data = get_movie(code)
        except Exception:
            await reply_service_unavailable(update)
            return
        if not data:
            await update.message.reply_text(
                f"❌ {code} kodli kino topilmadi.\n\n🔍 <b>Kino qidirish</b> tugmasini bosing!",
                parse_mode="HTML",
                reply_markup=get_user_menu_keyboard() if user_id != ADMIN_ID else None,
            )
            return
        increment_view_count(code)
        await send_movie_to_chat(update.message, code, data, user_id=user_id)
        return

    if len(text) < 2:
        await update.message.reply_text(
            "🔍 Kino kodini (raqam) yoki <b>🔍 Kino qidirish</b> tugmasini bosing.",
            parse_mode="HTML",
            reply_markup=get_user_menu_keyboard() if user_id != ADMIN_ID else None,
        )
        return

    try:
        results = search_movies_by_name(text, limit=15)
    except Exception:
        await reply_service_unavailable(update)
        return

    if not results:
        await update.message.reply_text(
            f"🔍 <b>«{escape_html(text)}»</b> bo'yicha hech narsa topilmadi.\n\n"
            "Boshqa nom yoki kod kiriting.",
            parse_mode="HTML",
            reply_markup=get_user_menu_keyboard() if user_id != ADMIN_ID else None,
        )
        return

    if len(results) == 1:
        movie = results[0]
        code = movie["code"]
        try:
            data = get_movie(code)
        except Exception:
            await reply_service_unavailable(update)
            return
        if data:
            increment_view_count(code)
            await send_movie_to_chat(update.message, code, data, user_id=user_id)
        return

    lines = [f"🔍 <b>«{escape_html(text)}»</b> bo'yicha {len(results)} ta kino topildi:\n"]
    buttons = []
    for movie in results:
        nom = movie.get("nom", "-")
        code = movie.get("code", "-")
        sifat = movie.get("sifat", "-")
        til = movie.get("til", "-")
        lines.append(f"🎬 {escape_html(nom)}\n   🎥 {sifat} | 🌐 {til} | 🆔 Kod: <b>{code}</b>\n")
        buttons.append([InlineKeyboardButton(f"🎬 {nom} [{code}]", callback_data=f"getmovie:{code}")])
    lines.append("👆 Tugmadan birini bosing yoki kodini yozing:")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML", reply_markup=InlineKeyboardMarkup(buttons))


# ===================== BUILD APPLICATION =====================

def build_application():
    ensure_telegram_imports()
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN topilmadi.")

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_error_handler(log_error)

    # Kino qo'shish conversation
    conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.VIDEO & filters.User(ADMIN_ID), handle_video),
            MessageHandler(filters.Document.ALL & filters.User(ADMIN_ID), handle_document),
        ],
        states={
            KOD_VAQT:      [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_kod_vaqt)],
            NOM:           [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_nom)],
            SIFAT:         [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_sifat)],
            TIL:           [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_til)],
            VAQT:          [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), get_vaqt)],
            CONFIRM:       [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), confirm_save)],
            FOLDER_CHOICE: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), handle_folder_choice)],
            FOLDER_CREATE: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), handle_folder_create)],
            FOLDER_PICK:   [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), handle_folder_pick)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        per_message=False,
        allow_reentry=True,
    )

    # Tahrirlash conversation
    edit_conv = ConversationHandler(
        entry_points=[CommandHandler("edit", edit_start)],
        states={
            EDIT_KOD:   [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), edit_get_kod)],
            EDIT_NOM:   [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), edit_get_nom)],
            EDIT_SIFAT: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), edit_get_sifat)],
            EDIT_TIL:   [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), edit_get_til)],
            EDIT_VAQT:  [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), edit_get_vaqt)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    # Jild conversation
    jild_conv = ConversationHandler(
        entry_points=[CommandHandler("jild", jild_start)],
        states={
            JILD_CODES: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), jild_get_codes)],
            JILD_NAME:  [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), jild_get_name)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    # Broadcast conversation — matn/rasm VA poll bitta conversation ichida
    broadcast_conv = ConversationHandler(
        entry_points=[CommandHandler("adminlik", admin_broadcast_start)],
        states={
            # Birinchi tanlov
            BC_GET_TEXT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), bc_get_text),
            ],
            # Oddiy broadcast yo'li
            BC_GET_MEDIA: [
                MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), bc_get_media),
                MessageHandler(filters.PHOTO & filters.User(ADMIN_ID), bc_get_media),
                MessageHandler(filters.ANIMATION & filters.User(ADMIN_ID), bc_get_media),
                MessageHandler(filters.VIDEO & filters.User(ADMIN_ID), bc_get_media),
            ],
            BC_ASK_BUTTON:   [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), bc_ask_button)],
            BC_GET_URL:      [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), bc_get_url)],
            BC_GET_BTN_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), bc_get_btn_name)],
            BC_CONFIRM:      [CallbackQueryHandler(bc_confirm_callback, pattern="^bc_")],
            # Poll yo'li
            BC_POLL_QUESTION: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), bc_poll_get_question)],
            BC_POLL_OPTIONS:  [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), bc_poll_get_options)],
            BC_POLL_CONFIRM:  [CallbackQueryHandler(bc_poll_confirm_callback, pattern="^(bc_poll_yes|bc_poll_edit|bc_cancel)$")],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    # AddChannel conversation
    addchannel_conv = ConversationHandler(
        entry_points=[CommandHandler("addchannel", admin_add_channel)],
        states={
            ADDCH_GET_TITLE: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), addch_get_title)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    # Ixtiyoriy obuna conversation
    ixtiyoriy_conv = ConversationHandler(
        entry_points=[CommandHandler("ixtiyoriyobuna", ixtiyoriy_obuna_start)],
        states={
            IXTIYORIY_LINK: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), ixtiyoriy_get_link)],
            IXTIYORIY_NOM:  [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), ixtiyoriy_get_nom)],
            IXTIYORIY_RASM: [
                MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_ID), ixtiyoriy_get_rasm),
                MessageHandler(filters.PHOTO & filters.User(ADMIN_ID), ixtiyoriy_get_rasm),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    # Qidiruv conversation
    search_conv = ConversationHandler(
        entry_points=[
            MessageHandler(
                filters.TEXT & filters.Regex(f"^{re.escape(USER_BTN_QIDIRUV)}$") & ~filters.User(ADMIN_ID),
                search_start,
            )
        ],
        states={
            SEARCH_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.User(ADMIN_ID), search_get_input),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", search_cancel),
            MessageHandler(
                filters.TEXT & filters.Regex("^❌ Qidiruvni bekor qilish$") & ~filters.User(ADMIN_ID),
                search_cancel,
            ),
        ],
        allow_reentry=True,
        per_message=False,
    )

    # Commandlar
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("delete", delete_movie))
    app.add_handler(CommandHandler("foydalanuvchi", show_user_count))
    app.add_handler(CommandHandler("seriallist", list_series_ranges))
    app.add_handler(CommandHandler("sevimli", show_favorites))
    app.add_handler(CommandHandler("top", admin_top_movies))
    app.add_handler(CommandHandler("help", admin_help))
    app.add_handler(CommandHandler("stat", admin_stat))
    app.add_handler(CommandHandler("ochirish", admin_delete_menu))
    app.add_handler(CommandHandler("removechannel", admin_remove_channel))
    app.add_handler(CommandHandler("kanallar", admin_channels_stat))
    app.add_handler(CommandHandler("barchasi", admin_all_movies))
    app.add_handler(CommandHandler("ixtiyoriyochirish", ixtiyoriy_remove_start))

    # Conversation handlerlar — search_conv BIRINCHI bo'lishi SHART
    app.add_handler(search_conv)
    app.add_handler(conv)
    app.add_handler(edit_conv)
    app.add_handler(jild_conv)
    app.add_handler(broadcast_conv)
    app.add_handler(addchannel_conv)
    app.add_handler(ixtiyoriy_conv)

    # Callback handlerlar
    app.add_handler(CallbackQueryHandler(handle_check_subscription_callback, pattern="^check_subscription$"))
    app.add_handler(CallbackQueryHandler(admin_remove_channel_callback, pattern="^rmchannel:"))
    app.add_handler(CallbackQueryHandler(handle_series_part_callback, pattern=f"^{SERIES_CALLBACK_PREFIX}"))
    app.add_handler(CallbackQueryHandler(handle_favorite_callback, pattern="^fav:"))
    app.add_handler(CallbackQueryHandler(admin_broadcast_stop_callback, pattern="^stop_broadcast$"))
    app.add_handler(CallbackQueryHandler(admin_delete_bc_callback, pattern="^del_bc:"))
    app.add_handler(CallbackQueryHandler(handle_getmovie_callback, pattern="^getmovie:"))
    app.add_handler(CallbackQueryHandler(ixtiyoriy_remove_callback, pattern="^rm_ixt:"))

    # Kanal join tracking
    app.add_handler(ChatMemberHandler(handle_channel_join_update, ChatMemberHandler.CHAT_MEMBER))

    # Oxirgi handlerlar
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
            app.run_polling(
                bootstrap_retries=-1,
                allowed_updates=["message", "callback_query", "chat_member"],
                drop_pending_updates=True,
            )
            logger.warning("Polling to'xtadi. 5 soniyadan keyin qayta ishga tushadi.")
            set_health_state(bot="stopped")
        except Exception as exc:
            logger.exception("Bot ishida xato yuz berdi.")
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
