import asyncio
import logging
import json
import os
import re
import base64
import time
import uuid
from io import BytesIO
from datetime import datetime, timedelta
from typing import Optional, Union
from aiohttp import web
from aiohttp.web import Response
import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    WebAppInfo,
    ReplyKeyboardMarkup,
    KeyboardButton,
    LabeledPrice,
    PreCheckoutQuery,
)
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from telethon import TelegramClient
from telethon.errors import UsernameInvalidError, UsernameNotOccupiedError
from telethon.sessions import StringSession

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============ НАСТРОЙКИ ============
# Домен: Jetstoreapp.ru
# ВАЖНО: токен бота ДОЛЖЕН задаваться только через переменную окружения BOT_TOKEN.
# Никаких дефолтных значений в коде быть не должно, чтобы не утек секретный токен.
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError(
        "BOT_TOKEN не задан. Установите переменную окружения BOT_TOKEN "
        "(например, в Railway/Render) перед запуском бота."
    )
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "6928639672,5235957477").split(",") if x.strip()]
WEB_APP_URL = os.getenv("WEB_APP_URL", "https://jetstoreapp.ru")
ADM_WEB_APP_URL = os.getenv("ADM_WEB_APP_URL", "https://jetstoreapp.ru/html/admin.html")

# Группа/чат, куда слать уведомления о продаже звёзд
SELL_STARS_NOTIFY_CHAT_ID = int(os.getenv("SELL_STARS_NOTIFY_CHAT_ID", "0") or "0")
TON_NOTIFY_CHAT_ID = int(os.getenv("TON_NOTIFY_CHAT_ID", "0") or "0")

# Курс выплаты за 1 звезду (RUB), используем тот же, что в мини-аппе
STAR_BUY_RATE_RUB = float(os.getenv("STAR_BUY_RATE_RUB", "0.65") or "0.65")
# Цена покупки 1 звезды (RUB) — для конвертации звёзд в рубли при оплате через CryptoBot
STAR_PRICE_RUB = float(os.getenv("STAR_PRICE_RUB", "1.37") or "1.37")
# Цены на Premium в рублях (по умолчанию совпадают с мини‑аппом)
PREMIUM_PRICES_RUB = {
    3: float(os.getenv("PREMIUM_PRICE_3M", "983") or "983"),
    6: float(os.getenv("PREMIUM_PRICE_6M", "1311") or "1311"),
    12: float(os.getenv("PREMIUM_PRICE_12M", "2377") or "2377"),
}

# Заказы на продажу звёзд из мини-приложения: order_id -> { user_id, username, first_name, last_name, stars_amount, method, payout_* }
# После successful_payment по payload "sell_stars:order_id" отправляем уведомление и удаляем запись
PENDING_SELL_STARS_ORDERS: dict[str, dict] = {}

# Реферальная система
# referrals_data.json: user_id(str) -> {
#   "parent1": str|None, "parent2": str|None, "parent3": str|None,
#   "referrals_l1": [str], "referrals_l2": [str], "referrals_l3": [str],
#   "earned_rub": float, "volume_rub": float
# }
REFERRALS: dict[str, dict] = {}
REFERRALS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "referrals_data.json")

# Ключ для шифрования ID в реферальной ссылке (XOR + base62 = короткая ссылка)
REFERRAL_ENC_KEY = (os.getenv("REFERRAL_ENC_KEY", "jet_ref_2024_secret") or "").encode()[:32].ljust(32, b"0")
_B62_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


def _ref_secret_int() -> int:
    """Секретное число для XOR (из ключа)."""
    h = sum((b << (i % 56)) for i, b in enumerate(REFERRAL_ENC_KEY or b"0")) & 0xFFFFFFFFFFFFFFFF
    return h or 0x5A5A5A5A5A5A5A5A


def _b62_encode(n: int) -> str:
    if n <= 0:
        return "0"
    s = []
    base = 62
    while n:
        s.append(_B62_ALPHABET[n % base])
        n //= base
    return "".join(reversed(s))


def _b62_decode(s: str) -> int:
    n = 0
    for c in s:
        idx = _B62_ALPHABET.find(c)
        if idx < 0:
            raise ValueError("invalid base62")
        n = n * 62 + idx
    return n


def _encrypt_ref_id(user_id: int) -> str:
    """Шифрует user_id для реферальной ссылки (XOR + base62 = короче)."""
    try:
        secret = _ref_secret_int()
        x = (user_id ^ secret) & 0xFFFFFFFFFFFFFFFF
        return _b62_encode(x)
    except Exception:
        return str(user_id)


def _decrypt_ref_id(enc: str) -> Optional[int]:
    """Расшифровывает ref-параметр. При ошибке — пробует int(enc) для старых ссылок."""
    if not enc:
        return None
    try:
        x = _b62_decode(enc)
        secret = _ref_secret_int()
        return (x ^ secret) & 0xFFFFFFFFFFFFFFFF
    except Exception:
        try:
            return int(enc)
        except (ValueError, TypeError):
            return None


# Чат, куда слать заявки на вывод реферальных средств
REFERRAL_WITHDRAW_CHAT_ID = int(os.getenv("REFERRAL_WITHDRAW_CHAT_ID", "0") or "0")

# ============ USERBOT (Telethon / MTProto) ============
# Чтобы искать любого пользователя по @username без /start, нужен userbot:
# - TELEGRAM_API_ID (int)
# - TELEGRAM_API_HASH (str)
# - TELEGRAM_STRING_SESSION (str)  ← строковая сессия Telethon (получается один раз)
#
# ВАЖНО: userbot работает под аккаунтом Telegram (не ботом).
def _read_json_file(path: str) -> dict:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f) or {}
    except Exception as e:
        logger.warning(f"Не удалось прочитать JSON {path}: {e}")
    return {}


def _save_json_file(path: str, data: dict) -> None:
    """Безопасная запись JSON на диск."""
    try:
        tmp_path = path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)
    except Exception as e:
        logger.warning(f"Не удалось сохранить JSON {path}: {e}")


def _load_referrals_sync() -> None:
    """Синхронная загрузка рефералов из JSON (fallback)."""
    global REFERRALS
    if REFERRALS:
        return
    try:
        if os.path.exists(REFERRALS_FILE):
            data = _read_json_file(REFERRALS_FILE)
            if isinstance(data, dict):
                REFERRALS = data
                return
    except Exception as e:
        logger.warning(f"Не удалось загрузить реферальные данные: {e}")
    REFERRALS = {}


def _save_referrals_sync() -> None:
    """Синхронное сохранение рефералов в JSON (fallback)."""
    try:
        _save_json_file(REFERRALS_FILE, REFERRALS)
    except Exception as e:
        logger.warning(f"Не удалось сохранить реферальные данные: {e}")


async def _load_referrals() -> None:
    """Загружаем реферальные данные (PostgreSQL или JSON)."""
    global REFERRALS
    if REFERRALS:
        return
    try:
        import db as _db
        if _db.is_enabled():
            REFERRALS = await _db.ref_load_all()
            return
    except Exception as e:
        logger.warning(f"Ошибка загрузки рефералов из БД: {e}")
    _load_referrals_sync()


async def _save_referrals() -> None:
    """Сохраняем реферальные данные (PostgreSQL или JSON)."""
    try:
        import db as _db
        if _db.is_enabled():
            for uid, data in REFERRALS.items():
                await _db.ref_save(uid, data)
            return
    except Exception as e:
        logger.warning(f"Ошибка сохранения рефералов в БД: {e}")
    _save_referrals_sync()


async def _get_or_create_ref_user(user_id: int | str) -> dict:
    """Возвращает (и при необходимости создаёт) реферальную запись пользователя."""
    await _load_referrals()
    uid = str(user_id)
    if uid not in REFERRALS:
        REFERRALS[uid] = {
            "parent1": None,
            "parent2": None,
            "parent3": None,
            "referrals_l1": [],
            "referrals_l2": [],
            "referrals_l3": [],
            "earned_rub": 0.0,
            "volume_rub": 0.0,
        }
    return REFERRALS[uid]


async def _process_referral_start(user_id: int, start_text: str | None) -> Optional[int]:
    """
    Обработка /start с параметром вида `ref_<id>`.
    Прописываем трёхуровневую иерархию: parent1/2/3 + списки рефералов.
    """
    if not start_text:
        return None
    try:
        parts = (start_text or "").strip().split(maxsplit=1)
        if len(parts) < 2:
            return None
        arg = parts[1].strip()
        if not arg.startswith("ref_"):
            return
        inviter_raw = arg[4:].strip()
        if not inviter_raw:
            return None
        inviter_id = _decrypt_ref_id(inviter_raw)
        if inviter_id is None:
            return None
    except Exception:
        return None

    if inviter_id == user_id:
        # Нельзя приглашать самого себя
        return None

    # Загружаем/создаём записи
    await _load_referrals()
    u = await _get_or_create_ref_user(user_id)

    # Если уже есть parent1 — не переписываем привязку
    if u.get("parent1"):
        return None

    inviter = await _get_or_create_ref_user(inviter_id)
    parent1 = str(inviter_id)
    parent2 = inviter.get("parent1")
    parent3 = inviter.get("parent2")

    uid_str = str(user_id)
    u["parent1"] = parent1
    u["parent2"] = parent2
    u["parent3"] = parent3

    # Добавляем в списки рефералов уровней
    if uid_str not in inviter["referrals_l1"]:
        inviter["referrals_l1"].append(uid_str)

    if parent2:
        p2 = await _get_or_create_ref_user(parent2)
        if uid_str not in p2["referrals_l2"]:
            p2["referrals_l2"].append(uid_str)

    if parent3:
        p3 = await _get_or_create_ref_user(parent3)
        if uid_str not in p3["referrals_l3"]:
            p3["referrals_l3"].append(uid_str)

    await _save_referrals()
    return inviter_id

TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "0") or "0")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "") or ""

def _read_text_file(path: str) -> str:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return (f.read() or "").strip()
    except Exception as e:
        logger.warning(f"Не удалось прочитать {path}: {e}")
    return ""

def _get_env_clean(name: str) -> str:
    v = os.getenv(name, "")
    if not v:
        return ""
    return v.strip().strip('"').strip("'").strip()

# Берём сессию из переменной окружения или из файла telethon_session.txt (рядом с bot.py)
_session_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "telethon_session.txt")
_cfg_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "telethon_config.json")
_cfg = _read_json_file(_cfg_file)

def _cfg_get(name: str, default: str = "") -> str:
    try:
        v = _cfg.get(name, default)
        if v is None:
            return default
        return str(v).strip()
    except Exception:
        return default

# Если env не задан — берём из telethon_config.json
if TELEGRAM_API_ID <= 0:
    try:
        TELEGRAM_API_ID = int(_cfg_get("api_id", "0") or "0")
    except Exception:
        TELEGRAM_API_ID = 0
if not TELEGRAM_API_HASH:
    TELEGRAM_API_HASH = _cfg_get("api_hash", "")

TELEGRAM_STRING_SESSION = (
    _get_env_clean("TELEGRAM_STRING_SESSION")
    or _get_env_clean("TELETHON_STRING_SESSION")
    or _cfg_get("string_session", "")
    or _read_text_file(_session_file)
)

# ============ DonateHub (Steam пополнение) ============
# Спека: https://donatehub.ru/swagger.json (basePath: /api)
# Авторизация: получить токен POST /api/token, далее header Authorization: "TOKEN <token>"
DONATEHUB_BASE = "https://donatehub.ru/api"
_donatehub_cfg_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "donatehub_config.json")
_donatehub_cfg = _read_json_file(_donatehub_cfg_file)

def _donatehub_cfg_get(name: str, default: str = "") -> str:
    try:
        v = _donatehub_cfg.get(name, default)
        if v is None:
            return default
        return str(v).strip()
    except Exception:
        return default

DONATEHUB_USERNAME = _get_env_clean("DONATEHUB_USERNAME") or _donatehub_cfg_get("username", "")
DONATEHUB_PASSWORD = _get_env_clean("DONATEHUB_PASSWORD") or _donatehub_cfg_get("password", "")
DONATEHUB_2FA_CODE = _get_env_clean("DONATEHUB_2FA_CODE") or _donatehub_cfg_get("code", "")

_donatehub_token: Optional[str] = None
_donatehub_token_ts: float = 0.0

def _cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "*",
    }

def _json_response(payload: Union[dict, list], status: int = 200):
    return Response(
        text=json.dumps(payload, ensure_ascii=False),
        status=status,
        content_type="application/json",
        charset="utf-8",
        headers=_cors_headers(),
    )

async def _donatehub_get_token(force: bool = False) -> str:
    global _donatehub_token, _donatehub_token_ts
    if not force and _donatehub_token and (time.time() - _donatehub_token_ts) < 20 * 60:
        return _donatehub_token

    if not DONATEHUB_USERNAME or not DONATEHUB_PASSWORD:
        raise RuntimeError("DonateHub credentials are missing (donatehub_config.json or env)")

    body = {"username": DONATEHUB_USERNAME, "password": DONATEHUB_PASSWORD}
    if DONATEHUB_2FA_CODE:
        body["code"] = DONATEHUB_2FA_CODE

    async with aiohttp.ClientSession() as session:
        async with session.post(f"{DONATEHUB_BASE}/token", json=body) as resp:
            data = await resp.json(content_type=None)
            if resp.status != 200:
                raise RuntimeError(f"DonateHub token error: {resp.status}: {data}")
            token = data.get("token")
            if not token:
                raise RuntimeError(f"DonateHub token missing in response: {data}")
            _donatehub_token = token
            _donatehub_token_ts = time.time()
            return token

async def _donatehub_request(method: str, path: str, *, params=None, json_body=None) -> dict:
    token = await _donatehub_get_token()
    url = f"{DONATEHUB_BASE}{path}"
    headers = {"Authorization": f"TOKEN {token}"}

    async with aiohttp.ClientSession() as session:
        async with session.request(method, url, params=params, json=json_body, headers=headers) as resp:
            data = await resp.json(content_type=None)
            if resp.status == 401:
                # пробуем обновить токен один раз
                token = await _donatehub_get_token(force=True)
                headers["Authorization"] = f"TOKEN {token}"
                async with session.request(method, url, params=params, json=json_body, headers=headers) as resp2:
                    data2 = await resp2.json(content_type=None)
                    if resp2.status >= 400:
                        raise RuntimeError(f"DonateHub error {resp2.status}: {data2}")
                    return data2
            if resp.status >= 400:
                raise RuntimeError(f"DonateHub error {resp.status}: {data}")
            return data

async def _donatehub_get_steam_course() -> dict:
    return await _donatehub_request("GET", "/steam_course")

async def _convert_to_usd(amount_local: float, currency: str) -> tuple[float, dict]:
    course = await _donatehub_get_steam_course()
    currency = (currency or "RUB").upper()
    if currency == "RUB":
        rate = float(course.get("USD_RUB"))
    elif currency == "UAH":
        rate = float(course.get("USD_UAH"))
    elif currency == "KZT":
        rate = float(course.get("USD_KZT"))
    else:
        rate = float(course.get("USD_RUB"))
        currency = "RUB"

    if rate <= 0:
        raise RuntimeError("Invalid steam course rate")
    amount_usd = round(float(amount_local) / rate, 2)
    return amount_usd, {"currency": currency, "rate": rate, "course": course}

telethon_client: Optional[TelegramClient] = None

# простой кэш: username -> (ts, payload)
_tg_lookup_cache: dict[str, tuple[float, dict]] = {}
_TG_CACHE_TTL_SEC = 10 * 60

# ============ БАЗА ДАННЫХ ============

class Database:
    def __init__(self):
        self.users_data = {}
        self.content_data = {
            'welcome_text_ru': '👋 <b>Добро пожаловать в Jet Store!</b>\n⚡ Покупай и управляй цифровыми товарами прямо в Telegram.\n \nВыберите действие:',
            'welcome_text_en': '👋 <b>Welcome to Jet Store!</b>\n\nChoose action:',
            'welcome_photo': None,
            'about_text_ru': '''<b>🌟 О сервисе Jet Store</b>

Мы предоставляем:
• ⭐️ <b>Покупку звёзд</b>
• 🎡 <b>Участие в рулетке</b>
• 🗂️ <b>Каталог цифровых товаров</b>''',
            'about_text_en': '''<b>🌟 About Jet Store Service</b>

We provide:
• ⭐️ <b>Star purchase</b>
• 🎡 <b>Roulette participation</b>
• 🗂️ <b>Digital goods catalog</b>''',
            'notifications': []
        }
        self.admins = set(ADMIN_IDS)  # Админы ТОЛЬКО из кода
        logger.info(f"Админы из кода: {self.admins}")
    
    def is_admin(self, user_id: int) -> bool:
        """Проверка прав администратора - ТОЛЬКО из кода ADMIN_IDS"""
        return user_id in ADMIN_IDS
    
    def add_user(self, user_id, user_data):
        """Добавляем пользователя"""
        if user_id not in self.users_data:
            self.users_data[user_id] = {
                'id': user_id,
                'username': user_data.get('username'),
                'first_name': user_data.get('first_name'),
                'last_name': user_data.get('last_name'),
                'language': user_data.get('language', 'ru'),
                'is_premium': user_data.get('is_premium', False),
                'registration_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'last_activity': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'notifications_enabled': True,
                'balance': 0,
                'purchases': []
            }
            return True
        return False
    
    def update_user_activity(self, user_id):
        """Обновляем время активности"""
        if user_id in self.users_data:
            self.users_data[user_id]['last_activity'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def get_user(self, user_id):
        """Получаем данные пользователя"""
        return self.users_data.get(user_id)
    
    def set_user_language(self, user_id, language):
        """Устанавливаем язык пользователя"""
        if user_id in self.users_data:
            self.users_data[user_id]['language'] = language
            return True
        return False
    
    def get_user_language(self, user_id):
        """Получаем язык пользователя"""
        user = self.get_user(user_id)
        return user.get('language', 'ru') if user else 'ru'
    
    def get_all_users(self):
        """Получаем всех пользователей"""
        return list(self.users_data.keys())
    
    def get_users_count(self):
        """Количество пользователей"""
        return len(self.users_data)
    
    def get_active_users(self, days=7):
        """Активные пользователи за N дней"""
        active_users = []
        cutoff_date = datetime.now().timestamp() - (days * 24 * 60 * 60)
        
        for user_id, user_data in self.users_data.items():
            try:
                last_activity = datetime.strptime(user_data['last_activity'], '%Y-%m-%d %H:%M:%S').timestamp()
                if last_activity > cutoff_date:
                    active_users.append(user_id)
            except:
                continue
        return active_users
    
    def update_balance(self, user_id, amount):
        """Обновляем баланс пользователя"""
        if user_id in self.users_data:
            self.users_data[user_id]['balance'] = self.users_data[user_id].get('balance', 0) + amount
            return True
        return False
    
    def get_balance(self, user_id):
        """Получаем баланс пользователя"""
        if user_id in self.users_data:
            return self.users_data[user_id].get('balance', 0)
        return 0
    
    # Контент функции
    def update_content(self, key, value):
        """Обновляем контент"""
        self.content_data[key] = value
    
    def get_content(self, key, default=None):
        """Получаем контент"""
        return self.content_data.get(key, default)
    
    def add_notification(self, notification):
        """Добавляем уведомление в историю"""
        if 'notifications' not in self.content_data:
            self.content_data['notifications'] = []
        self.content_data['notifications'].append(notification)
        if len(self.content_data['notifications']) > 50:
            self.content_data['notifications'] = self.content_data['notifications'][-50:]
    
    def get_notifications(self, limit=10):
        """Получаем последние уведомления"""
        notifications = self.content_data.get('notifications', [])
        return notifications[-limit:]
    
    def get_admins(self):
        return list(ADMIN_IDS)

# ============ ИНИЦИАЛИЗАЦИЯ ============

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
db = Database()

async def init_telethon():
    """Инициализация userbot-клиента (Telethon)."""
    global telethon_client
    if TELEGRAM_API_ID <= 0 or not TELEGRAM_API_HASH or not TELEGRAM_STRING_SESSION:
        logger.warning(
            "Telethon не настроен. Для поиска любого @username без /start задайте "
            "TELEGRAM_API_ID / TELEGRAM_API_HASH / TELEGRAM_STRING_SESSION"
        )
        telethon_client = None
        return

    telethon_client = TelegramClient(
        StringSession(TELEGRAM_STRING_SESSION),
        TELEGRAM_API_ID,
        TELEGRAM_API_HASH
    )
    await telethon_client.connect()
    if not await telethon_client.is_user_authorized():
        logger.error("Telethon: сессия не авторизована. Нужна корректная TELEGRAM_STRING_SESSION.")
        await telethon_client.disconnect()
        telethon_client = None
        return

    logger.info("✅ Telethon userbot подключен и авторизован")

def _data_url_from_bytes(image_bytes: bytes) -> str:
    # Telegram чаще отдаёт jpeg, но может быть и png/webp; ставим jpeg по умолчанию
    b64 = base64.b64encode(image_bytes).decode("ascii")
    return f"data:image/jpeg;base64,{b64}"

async def lookup_user_via_telethon(username: str) -> Optional[dict]:
    """Возвращает {username, firstName, lastName, avatar} для любого @username через userbot."""
    global telethon_client
    if not telethon_client:
        return None

    clean = username.lstrip("@").strip()
    if not clean:
        return None

    # cache
    now = time.time()
    cached = _tg_lookup_cache.get(clean.lower())
    if cached and (now - cached[0]) < _TG_CACHE_TTL_SEC:
        return cached[1]

    try:
        entity = await telethon_client.get_entity(clean)
    except (UsernameInvalidError, UsernameNotOccupiedError):
        return None
    except Exception as e:
        logger.error(f"Telethon lookup error for @{clean}: {e}")
        return None

    first_name = getattr(entity, "first_name", "") or ""
    last_name = getattr(entity, "last_name", "") or ""
    uname = getattr(entity, "username", None) or clean

    avatar_data_url = None
    try:
        # Правильный способ получить байты фото профиля
        image_bytes = await telethon_client.download_profile_photo(entity, file=bytes)
        if image_bytes:
            avatar_data_url = _data_url_from_bytes(image_bytes)
    except Exception as e:
        logger.warning(f"Telethon avatar download failed for @{clean}: {e}")

    payload = {
        "username": uname,
        "firstName": first_name,
        "lastName": last_name,
        "avatar": avatar_data_url
    }
    _tg_lookup_cache[clean.lower()] = (now, payload)
    return payload

# ============ СОСТОЯНИЯ ============

class UserStates(StatesGroup):
    choosing_language = State()

class AdminStates(StatesGroup):
    waiting_welcome_text = State()
    waiting_welcome_photo = State()
    waiting_about_text = State()
    waiting_notification_text = State()
    waiting_notification_photo = State()
    waiting_user_balance = State()


class SellStarsStates(StatesGroup):
    waiting_amount = State()

# ============ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ============

def is_admin(user_id: int) -> bool:
    """Проверка прав администратора - ТОЛЬКО из кода"""
    return db.is_admin(user_id)

def get_main_menu(language: str = 'ru'):
    """Главное меню на выбранном языке"""
    if language == 'en':
        keyboard = [
            [
                InlineKeyboardButton(text="📈 Trade on jet", web_app=WebAppInfo(url=WEB_APP_URL)),
            ],
            [
                InlineKeyboardButton(text="ℹ️ About us", callback_data="about_info")
            ]
        ]
    else:
        keyboard = [
            [
                InlineKeyboardButton(text="📈 Торговля на Jet", web_app=WebAppInfo(url=WEB_APP_URL)),
            ],

            
            [
                InlineKeyboardButton(text="📰 Подписаться на канал", url="https://t.me/JetStoreApp"),
            ],


            [
                InlineKeyboardButton(text="ℹ️ О нас", callback_data="about_info")
            ]
        ]
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_about_menu(language: str = 'ru'):
    """Меню 'О нас' на выбранном языке"""
    if language == 'en':
        keyboard = [
            [
                InlineKeyboardButton(text="📞 Support", url="https://t.me/your_support"),
                InlineKeyboardButton(text="📢 Info channel", url="https://t.me/your_channel")
            ],
            [
                InlineKeyboardButton(text="📜 User agreement", 
                                   web_app=WebAppInfo(url=f"{WEB_APP_URL}/agreement")),
            ],
            [
                InlineKeyboardButton(text="🔒 Privacy policy", 
                                   web_app=WebAppInfo(url=f"{WEB_APP_URL}/privacy")),
            ],
            [
                InlineKeyboardButton(text="🔙 Back", callback_data="back_to_main")
            ]
        ]
    else:
        keyboard = [
            [
                InlineKeyboardButton(text="📞 Поддержка", url="https://t.me/ваш_поддержка"),
                InlineKeyboardButton(text="📢 Наш канал", url="https://t.me/ваш_канал")
            ],
            [
                InlineKeyboardButton(text="📜 Пользовательское соглашение", 
                                   web_app=WebAppInfo(url=f"{WEB_APP_URL}/agreement")),
            ],
            [
                InlineKeyboardButton(text="🔒 Политика конфиденциальности", 
                                   web_app=WebAppInfo(url=f"{WEB_APP_URL}/privacy")),
            ],
            [
                InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")
            ]
        ]
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_admin_menu():
    """Меню админки"""
    keyboard = [
        [
            InlineKeyboardButton(text="admin", web_app=WebAppInfo(url=ADM_WEB_APP_URL)),
        ],

        [
            InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats"),
            InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users")
        ],
        [
            InlineKeyboardButton(text="✏️ Приветствие", callback_data="admin_welcome"),
            InlineKeyboardButton(text="🖼️ Изменить фото", callback_data="admin_photo")
        ],
        [
            InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_notification"),
            InlineKeyboardButton(text="ℹ️ О нас", callback_data="admin_about")
        ],
        [
            InlineKeyboardButton(text="👑 Админы", callback_data="admin_admins"),
            InlineKeyboardButton(text="💰 Балансы", callback_data="admin_balance")
        ],
        [
            InlineKeyboardButton(text="🔙 В меню", callback_data="back_to_main")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_language_keyboard():
    """Клавиатура для выбора языка"""
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🇷🇺 Русский")],
            [KeyboardButton(text="🇺🇸 English")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    return keyboard

# ============ КОМАНДА /START (без выбора языка) ============

@dp.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    """Стартовое приветствие без выбора языка"""
    user = message.from_user
    
    # Регистрируем пользователя (если ещё нет) с языком по умолчанию ru
    user_data = db.get_user(user.id)
    if not user_data:
        db.add_user(user.id, {
            'username': user.username,
            'first_name': user.first_name,
            'last_name': user.last_name,
            'language': 'ru',
            'is_premium': getattr(user, 'is_premium', False) or False
        })
    else:
        db.update_user_activity(user.id)

    # Обработка реферального старта: /start ref_<id>
    inviter_id: Optional[int] = None
    try:
        inviter_id = await _process_referral_start(user.id, message.text or "")
    except Exception as e:
        logger.warning(f"Ошибка обработки реферального старта /start: {e}")

    # ТЕСТОВОЕ уведомление пригласившему о новом реферале
    if inviter_id:
        try:
            inviter_chat_id = int(inviter_id)
            ref_user = message.from_user
            ref_line = ref_user.username and f"@{ref_user.username}" or (ref_user.full_name or str(ref_user.id))
            text = (
                "👥 <b>Новый реферал (тестовое уведомление)</b>\n\n"
                f"Пользователь: {ref_line}\n"
                f"ID: <code>{ref_user.id}</code>\n"
                "Перешёл по вашей реферальной ссылке."
            )
            await bot.send_message(inviter_chat_id, text, parse_mode="HTML")
        except Exception as e:
            logger.warning(f"Не удалось отправить тестовое уведомление о реферале пригласившему {inviter_id}: {e}")

    username_display = user.username and f"@{user.username}" or user.first_name or "друг"

    text = (
        "Добро пожаловать в <b>Jet Store</b>! 🚀\n"
        f"Привет, <b>{username_display}</b>!\n\n"
        "⚡ Покупай и управляй цифровыми товарами прямо в Telegram.\n\n"
        "Выбери действие:"
    )

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Открыть приложение⭐️",
                    web_app=WebAppInfo(url=WEB_APP_URL)
                )
            ],
            [
                InlineKeyboardButton(
                    text="Подписаться на канал⭐️",
                    url="https://t.me/JetStoreApp"
                )
            ]
        ]
    )

    await message.answer(text, reply_markup=keyboard, parse_mode="HTML")


# ============ ПРОДАЖА ЗВЁЗД ЗА STARS ============

@dp.message(Command("sellstars"))
async def cmd_sell_stars(message: types.Message, state: FSMContext):
    """Запуск продажи звёзд: просим ввести количество"""
    await state.set_state(SellStarsStates.waiting_amount)
    await message.answer(
        "💫 <b>Продажа звёзд</b>\n\n"
        "Введите количество звёзд, которые хотите продать.\n"
        "Например: <code>500</code>",
        parse_mode="HTML"
    )


@dp.message(SellStarsStates.waiting_amount)
async def process_sell_stars_amount(message: types.Message, state: FSMContext):
    """Обрабатываем введённое количество звёзд и выставляем счёт в Stars"""
    text = (message.text or "").strip().replace(" ", "")
    if not text.isdigit():
        await message.answer("❌ Введите целое число — количество звёзд, например: 500")
        return

    stars = int(text)
    if stars <= 0:
        await message.answer("❌ Количество звёзд должно быть больше 0")
        return

    # Примерная сумма выплаты в рублях
    payout_rub = stars * STAR_BUY_RATE_RUB

    await state.clear()

    prices = [LabeledPrice(label="Продажа звёзд", amount=stars)]

    await message.answer_invoice(
        title="Продажа Telegram Stars",
        description=(
            f"Вы продаёте {stars} ⭐ Telegram Stars.\n\n"
            f"Примерная выплата: <b>{payout_rub:.2f} ₽</b> по курсу {STAR_BUY_RATE_RUB} ₽ за 1 ⭐."
        ),
        payload=f"sellstars:{stars}",
        provider_token="1744374395:TEST:36675594277e9de887a6",
        currency="XTR",
        prices=prices,
        max_tip_amount=0,
        need_name=False,
        need_phone_number=False,
        need_email=False,
        need_shipping_address=False,
        is_flexible=False,
        reply_markup=None
    )


@dp.pre_checkout_query()
async def process_pre_checkout_query(pre_checkout_query: PreCheckoutQuery):
    """Подтверждаем оплату Stars перед списанием"""
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)


@dp.message(F.successful_payment)
async def process_successful_payment(message: types.Message):
    """Обработка успешной оплаты Stars"""
    sp = message.successful_payment
    if not sp:
        return

    if sp.currency != "XTR":
        return

    payload = sp.invoice_payload or ""
    user = message.from_user

    # Продажа звёзд из мини-приложения (sell_stars:order_id) — есть данные выплаты
    if payload.startswith("sell_stars:"):
        order_id = payload.split(":", 1)[1].strip()
        order = PENDING_SELL_STARS_ORDERS.pop(order_id, None)
        stars = sp.total_amount
        payout_rub = stars * STAR_BUY_RATE_RUB
        seller_username = f"@{user.username}" if user.username else (user.full_name or str(user.id))

        notify_text = (
            "‼️ <b>Новая продажа звёзд</b>\n\n"
            f"Продавец: {seller_username}\n"
            f"ID: <code>{user.id}</code>\n"
            f"Имя: {user.first_name or ''} {user.last_name or ''}\n"
            f"Продано звёзд: <b>{stars}</b> ⭐\n"
            f"Сумма выплаты: <b>{payout_rub:.2f} ₽</b>\n"
        )
        if order:
            method = order.get("method") or "wallet"
            notify_text += "\n<b>Выплата:</b> "
            if method == "wallet":
                notify_text += f"Кошелёк\nАдрес: <code>{order.get('wallet_address') or '—'}</code>\n"
                if order.get("wallet_memo"):
                    notify_text += f"Memo: <code>{order['wallet_memo']}</code>\n"
            elif method == "sbp":
                notify_text += f"СБП\nТелефон: <code>{order.get('sbp_phone') or '—'}</code>\nБанк: {order.get('sbp_bank') or '—'}\n"
            elif method == "card":
                notify_text += f"Карта\nНомер: <code>{order.get('card_number') or '—'}</code>\nБанк: {order.get('card_bank') or '—'}\n"

        await message.answer(
            "✅ Оплата звёздами получена!\n\n"
            f"Мы выплатим тебе примерно <b>{payout_rub:.2f} ₽</b> за {stars} ⭐.\n"
            "Ожидай обработки заявки.",
            parse_mode="HTML"
        )
        if SELL_STARS_NOTIFY_CHAT_ID:
            try:
                await bot.send_message(SELL_STARS_NOTIFY_CHAT_ID, notify_text, parse_mode="HTML")
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление о продаже звёзд: {e}")
        return

    # Продажа звёзд из чата (sellstars:amount) — без данных выплаты
    if payload.startswith("sellstars:"):
        try:
            stars = int(payload.split(":", 1)[1])
        except Exception:
            stars = sp.total_amount

        payout_rub = stars * STAR_BUY_RATE_RUB
        seller_username = f"@{user.username}" if user.username else (user.full_name or str(user.id))

        notify_text = (
            "‼️ <b>Новая продажа звёзд</b>\n\n"
            f"Продавец: {seller_username}\n"
            f"ID: <code>{user.id}</code>\n"
            f"Продано звёзд: <b>{stars}</b> ⭐\n"
            f"Сумма выплаты: <b>{payout_rub:.2f} ₽</b>\n"
        )

        await message.answer(
            "✅ Оплата звёздами получена!\n\n"
            f"Мы выплатим тебе примерно <b>{payout_rub:.2f} ₽</b> за {stars} ⭐.\n"
            "Ожидай обработки заявки.",
            parse_mode="HTML"
        )

        if SELL_STARS_NOTIFY_CHAT_ID:
            try:
                await bot.send_message(SELL_STARS_NOTIFY_CHAT_ID, notify_text, parse_mode="HTML")
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление о продаже звёзд в чат {SELL_STARS_NOTIFY_CHAT_ID}: {e}")
        return

# ============ ПОКАЗ ГЛАВНОГО МЕНЮ ============

async def show_main_menu(message: types.Message, language: str):
    """Показать главное меню на выбранном языке"""
    user_id = message.from_user.id
    
    # Получаем текст приветствия
    if language == 'en':
        welcome_text = db.get_content('welcome_text_en', '👋 <b>Welcome to Jet Store!</b>\n\nChoose action:')
    else:
        welcome_text = db.get_content('welcome_text_ru', '👋 <b>Добро пожаловать в Jet Store!</b>\n\nВыберите действие:')
    
    welcome_photo = db.get_content('welcome_photo')
    
    keyboard = get_main_menu(language)
    
    # Отправляем приветствие
    if welcome_photo:
        try:
            await message.answer_photo(
                photo=welcome_photo,
                caption=welcome_text,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Ошибка отправки фото: {e}")
            await message.answer(
                text=welcome_text,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
    else:
        await message.answer(
            text=welcome_text,
            reply_markup=keyboard,
            parse_mode="HTML"
        )

# ============ КОМАНДА /ADMIN ============

@dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    """Админ панель"""
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        return
    
    stats_text = (
        f"⚙️ <b>Панель администратора</b>\n\n"
        f"📊 Статистика:\n"
        f"• Всего пользователей: {db.get_users_count()}\n"
        f"• Активных за 7 дней: {len(db.get_active_users(7))}\n"
        f"• Администраторов: {len(ADMIN_IDS)}\n\n"
        f"🆔 Ваш ID: <code>{user_id}</code>\n"
        f"👑 Ваш статус: Администратор ✅"
    )
    
    await message.answer(
        stats_text,
        reply_markup=get_admin_menu(),
        parse_mode="HTML"
    )

# ============ АДМИН ПАНЕЛЬ ============

@dp.callback_query(F.data == "admin_panel")
async def admin_panel(callback_query: types.CallbackQuery):
    """Открыть админ панель"""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("⛔ Нет прав администратора", show_alert=True)
        return
    
    await cmd_admin(callback_query.message)
    await callback_query.answer()

# ============ СТАТИСТИКА ============

@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback_query: types.CallbackQuery):
    """Статистика"""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("⛔ Нет прав администратора", show_alert=True)
        return
    
    total_users = db.get_users_count()
    active_7 = len(db.get_active_users(7))
    active_30 = len(db.get_active_users(30))
    
    stats_text = (
        f"📊 <b>Статистика бота</b>\n\n"
        f"👥 <b>Пользователи:</b>\n"
        f"• Всего: {total_users}\n"
        f"• Активных за 7 дней: {active_7}\n"
        f"• Активных за 30 дней: {active_30}\n\n"
        f"📈 <b>Активность:</b>\n"
        f"• Уведомлений отправлено: {len(db.get_notifications())}\n"
        f"• Админов: {len(ADMIN_IDS)}"
    )
    
    await callback_query.message.answer(
        text=stats_text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_stats")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_panel")]
            ]
        )
    )
    await callback_query.answer()

# ============ УПРАВЛЕНИЕ ПРИВЕТСТВИЕМ ============

@dp.callback_query(F.data == "admin_welcome")
async def admin_welcome(callback_query: types.CallbackQuery):
    """Управление приветствием"""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("⛔ Нет прав администратора", show_alert=True)
        return
    
    welcome_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🇷🇺 Русский текст", callback_data="edit_welcome_ru"),
                InlineKeyboardButton(text="🇺🇸 English текст", callback_data="edit_welcome_en")
            ],
            [
                InlineKeyboardButton(text="🔙 Назад", callback_data="admin_panel")
            ]
        ]
    )
    
    await callback_query.message.answer(
        "📝 <b>Управление приветствием</b>\n\n"
        "Выберите, какой текст редактировать:",
        reply_markup=welcome_keyboard,
        parse_mode="HTML"
    )
    await callback_query.answer()

@dp.callback_query(F.data.startswith("edit_welcome_"))
async def edit_welcome(callback_query: types.CallbackQuery, state: FSMContext):
    """Редактировать приветствие"""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("⛔ Нет прав администратора", show_alert=True)
        return
    
    language = callback_query.data.split("_")[-1]
    
    if language == 'ru':
        current_text = db.get_content('welcome_text_ru', 'Приветствие не настроено')
        lang_name = "русском"
    else:
        current_text = db.get_content('welcome_text_en', 'Welcome not configured')
        lang_name = "английском"
    
    await callback_query.message.answer(
        f"✏️ <b>Редактирование приветствия на {lang_name}</b>\n\n"
        f"Текущий текст:\n{current_text}\n\n"
        f"Отправьте новый текст (можно использовать HTML разметку):",
        parse_mode="HTML"
    )
    
    await state.update_data(edit_language=language)
    await state.set_state(AdminStates.waiting_welcome_text)
    await callback_query.answer()

@dp.message(AdminStates.waiting_welcome_text)
async def save_welcome_text(message: types.Message, state: FSMContext):
    """Сохранить текст приветствия"""
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Нет прав администратора")
        await state.clear()
        return
    
    data = await state.get_data()
    language = data.get('edit_language', 'ru')
    
    db.update_content(f'welcome_text_{language}', message.html_text)
    
    # Сохраняем в историю
    db.add_notification({
        'type': 'welcome_update',
        'admin_id': message.from_user.id,
        'admin_name': message.from_user.first_name,
        'text': f'Обновлен текст приветствия ({language})',
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })
    
    await message.answer(f"✅ Текст приветствия на {language} обновлен!")
    await state.clear()

# ============ УПРАВЛЕНИЕ ФОТО ============

@dp.callback_query(F.data == "admin_photo")
async def admin_photo(callback_query: types.CallbackQuery):
    """Управление фото"""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("⛔ Нет прав администратора", show_alert=True)
        return
    
    photo_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🖼️ Загрузить фото", callback_data="upload_photo")
            ],
            [
                InlineKeyboardButton(text="🗑️ Удалить фото", callback_data="remove_photo")
            ],
            [
                InlineKeyboardButton(text="🔙 Назад", callback_data="admin_panel")
            ]
        ]
    )
    
    current_photo = db.get_content('welcome_photo')
    status = "✅ Установлено" if current_photo else "❌ Не установлено"
    
    await callback_query.message.answer(
        f"🖼️ <b>Управление фото</b>\n\n"
        f"Статус: {status}",
        reply_markup=photo_keyboard,
        parse_mode="HTML"
    )
    await callback_query.answer()

@dp.callback_query(F.data == "upload_photo")
async def upload_photo(callback_query: types.CallbackQuery, state: FSMContext):
    """Загрузить фото"""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("⛔ Нет прав администратора", show_alert=True)
        return
    
    await callback_query.message.answer(
        "🖼️ <b>Отправьте новое фото для приветствия:</b>\n\n"
        "• Фото должно быть хорошего качества\n"
        "• Рекомендуемый размер: 1080x1920\n"
        "• Формат: JPEG, PNG",
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.waiting_welcome_photo)
    await callback_query.answer()

@dp.message(AdminStates.waiting_welcome_photo)
async def save_welcome_photo(message: types.Message, state: FSMContext):
    """Сохранить фото приветствия"""
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Нет прав администратора")
        await state.clear()
        return
    
    if not message.photo:
        await message.answer("❌ Пожалуйста, отправьте фото")
        return
    
    # Получаем file_id самого большого фото
    photo = message.photo[-1]
    file_id = photo.file_id
    
    db.update_content('welcome_photo', file_id)
    
    # Сохраняем в историю
    db.add_notification({
        'type': 'photo_update',
        'admin_id': message.from_user.id,
        'admin_name': message.from_user.first_name,
        'text': 'Обновлено фото приветствия',
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })
    
    await message.answer("✅ Фото приветствия обновлено!")
    
    # Показываем превью
    await message.answer_photo(
        photo=file_id,
        caption="👁️ <b>Превью нового фото:</b>",
        parse_mode="HTML"
    )
    
    await state.clear()

@dp.callback_query(F.data == "remove_photo")
async def remove_photo(callback_query: types.CallbackQuery):
    """Удалить фото"""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("⛔ Нет прав администратора", show_alert=True)
        return
    
    db.update_content('welcome_photo', None)
    
    # Сохраняем в историю
    db.add_notification({
        'type': 'photo_remove',
        'admin_id': callback_query.from_user.id,
        'admin_name': callback_query.from_user.first_name,
        'text': 'Удалено фото приветствия',
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })
    
    await callback_query.message.answer("✅ Фото приветствия удалено!")
    await callback_query.answer()

# ============ РАССЫЛКА ============

@dp.callback_query(F.data == "admin_notification")
async def admin_notification(callback_query: types.CallbackQuery):
    """Рассылка уведомлений"""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("⛔ Нет прав администратора", show_alert=True)
        return
    
    notification_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📢 Отправить уведомление", callback_data="send_notification")
            ],
            [
                InlineKeyboardButton(text="🔙 Назад", callback_data="admin_panel")
            ]
        ]
    )
    
    await callback_query.message.answer(
        f"📢 <b>Управление уведомлениями</b>\n\n"
        f"👥 Всего пользователей: {db.get_users_count()}",
        reply_markup=notification_keyboard,
        parse_mode="HTML"
    )
    await callback_query.answer()

@dp.callback_query(F.data == "send_notification")
async def send_notification(callback_query: types.CallbackQuery, state: FSMContext):
    """Отправить уведомление"""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("⛔ Нет прав администратора", show_alert=True)
        return
    
    await callback_query.message.answer(
        "📢 <b>Введите текст уведомления:</b>\n\n"
        "Можно использовать HTML разметку.\n"
        "Уведомление будет отправлено всем пользователям.",
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.waiting_notification_text)
    await callback_query.answer()

@dp.message(AdminStates.waiting_notification_text)
async def process_notification_text(message: types.Message, state: FSMContext):
    """Обработать текст уведомления"""
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Нет прав администратора")
        await state.clear()
        return
    
    notification_text = message.html_text
    
    # Сохраняем текст в состоянии
    await state.update_data(notification_text=notification_text)
    
    confirm_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Отправить всем", callback_data="confirm_notification"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_notification")
            ]
        ]
    )
    
    await message.answer(
        f"📢 <b>Подтверждение отправки:</b>\n\n"
        f"{notification_text[:200]}...\n\n"
        f"👥 Будет отправлено: <b>{db.get_users_count()}</b> пользователям",
        reply_markup=confirm_keyboard,
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "confirm_notification")
async def confirm_notification(callback_query: types.CallbackQuery, state: FSMContext):
    """Подтверждение отправки уведомления"""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("⛔ Нет прав администратора", show_alert=True)
        return
    
    data = await state.get_data()
    notification_text = data.get('notification_text')
    
    if not notification_text:
        await callback_query.answer("❌ Текст уведомления не найден", show_alert=True)
        return
    
    await callback_query.message.edit_text("🔄 <b>Начинаю рассылку...</b>", parse_mode="HTML")
    
    users = db.get_all_users()
    total = len(users)
    successful = 0
    failed = 0
    
    # Отправляем уведомления
    for i, user_id in enumerate(users, 1):
        try:
            await bot.send_message(
                chat_id=user_id,
                text=notification_text,
                parse_mode="HTML"
            )
            successful += 1
            
            # Обновляем прогресс каждые 20 отправок
            if i % 20 == 0:
                progress = int((i / total) * 100)
                await callback_query.message.edit_text(
                    f"🔄 <b>Рассылка в процессе...</b>\n\n"
                    f"📊 Прогресс: {progress}%\n"
                    f"✅ Успешно: {successful}\n"
                    f"❌ Ошибок: {failed}",
                    parse_mode="HTML"
                )
            
            # Небольшая задержка
            await asyncio.sleep(0.1)
            
        except Exception as e:
            logger.error(f"Ошибка отправки пользователю {user_id}: {e}")
            failed += 1
    
    # Сохраняем в историю
    db.add_notification({
        'type': 'notification',
        'admin_id': callback_query.from_user.id,
        'admin_name': callback_query.from_user.first_name,
        'text': f'Рассылка: {notification_text[:50]}...',
        'total': total,
        'successful': successful,
        'failed': failed,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })
    
    # Итоговый отчет
    report_text = (
        f"✅ <b>Рассылка завершена!</b>\n\n"
        f"📊 <b>Отчет:</b>\n"
        f"• Всего пользователей: {total}\n"
        f"• Успешно отправлено: {successful}\n"
        f"• Не удалось отправить: {failed}\n\n"
        f"📅 Отправлено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    
    await callback_query.message.edit_text(
        report_text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🔙 В админку", callback_data="admin_panel")]]
        )
    )
    
    await state.clear()
    await callback_query.answer()

# ============ УПРАВЛЕНИЕ АДМИНАМИ ============

@dp.callback_query(F.data == "admin_admins")
async def admin_admins(callback_query: types.CallbackQuery):
    """Управление админами"""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("⛔ Нет прав администратора", show_alert=True)
        return
    
    admins_text = "👑 <b>Список администраторов (из кода):</b>\n\n"
    
    for i, admin_id in enumerate(ADMIN_IDS, 1):
        try:
            admin_user = await bot.get_chat(admin_id)
            admins_text += f"{i}. {admin_user.first_name} (@{admin_user.username}) - <code>{admin_id}</code>\n"
        except:
            admins_text += f"{i}. ID: <code>{admin_id}</code> (пользователь не найден)\n"
    
    admins_text += f"\nℹ️ Чтобы добавить админа, измените код:\n<code>ADMIN_IDS = [{', '.join(str(admin) for admin in ADMIN_IDS)}]</code>"
    
    admins_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔙 Назад", callback_data="admin_panel")
            ]
        ]
    )
    
    await callback_query.message.answer(
        admins_text,
        reply_markup=admins_keyboard,
        parse_mode="HTML"
    )
    await callback_query.answer()

# ============ КНОПКА "О НАС" ============

@dp.callback_query(F.data == "about_info")
async def show_about(callback_query: types.CallbackQuery):
    """Раздел 'О нас'"""
    user_id = callback_query.from_user.id
    language = db.get_user_language(user_id)
    
    if language == 'en':
        about_text = db.get_content('about_text_en', 'Information about service...')
    else:
        about_text = db.get_content('about_text_ru', 'Информация о сервисе...')
    
    await callback_query.message.answer(
        text=about_text,
        reply_markup=get_about_menu(language),
        parse_mode="HTML"
    )
    await callback_query.answer()

# ============ ПРОФИЛЬ ============


# ============ БОНУСЫ ============

# ============ НАЗАД В ГЛАВНОЕ МЕНЮ ============

@dp.callback_query(F.data == "back_to_main")
async def back_to_main(callback_query: types.CallbackQuery):
    """Возврат в главное меню"""
    user_id = callback_query.from_user.id
    language = db.get_user_language(user_id)
    await show_main_menu(callback_query.message, language)
    await callback_query.answer()

# ============ ОТМЕНА РАССЫЛКИ ============

@dp.callback_query(F.data == "cancel_notification")
async def cancel_notification(callback_query: types.CallbackQuery, state: FSMContext):
    """Отмена рассылки"""
    await state.clear()
    await callback_query.message.answer("❌ Рассылка отменена")
    await callback_query.answer()

# ============ КОМАНДА /ID ============


# ============ КОМАНДА /USERS ============

@dp.message(Command("users"))
async def cmd_users(message: types.Message):
    """Показать количество пользователей (только для админов)"""
    if not is_admin(message.from_user.id):
        await message.answer("⛔ У вас нет прав администратора")
        return
    
    total_users = db.get_users_count()
    active_users = len(db.get_active_users(7))
    
    await message.answer(
        f"👥 <b>Статистика пользователей:</b>\n\n"
        f"• Всего пользователей: {total_users}\n"
        f"• Активных за 7 дней: {active_users}\n"
        f"• Неактивных: {total_users - active_users}",
        parse_mode="HTML"
    )

# ============ HTTP API ДЛЯ ПОЛУЧЕНИЯ ДАННЫХ ПОЛЬЗОВАТЕЛЯ ============

def _get_username_from_request(request) -> str:
    """Надёжно извлекаем username из query (aiohttp по-разному парсит в зависимости от клиента)."""
    username = ""
    # 1) rel_url.query — стандартный способ в aiohttp
    try:
        q = getattr(request, "rel_url", None) and getattr(request.rel_url, "query", None)
        if q and hasattr(q, "get"):
            username = (q.get("username") or "").strip()
    except Exception:
        pass
    # 2) request.query (если есть)
    if not username:
        try:
            q = getattr(request, "query", None)
            if q and hasattr(q, "get"):
                username = (q.get("username") or "").strip()
        except Exception:
            pass
    # 3) Парсим сырую query_string через parse_qs
    if not username and getattr(request, "query_string", None):
        try:
            from urllib.parse import parse_qs, unquote
            raw = (request.query_string or "").strip()
            if raw:
                parsed = parse_qs(raw, keep_blank_values=False)
                vals = parsed.get("username", [])
                if vals:
                    username = (vals[0] or "").strip()
            if not username:
                decoded = unquote(raw)
                if "username=" in decoded:
                    username = decoded.split("username=", 1)[1].split("&", 1)[0].strip()
        except Exception:
            pass
    return username or ""


async def get_telegram_user_handler(request):
    """HTTP эндпоинт для получения данных пользователя Telegram по username"""
    try:
        username = _get_username_from_request(request)

        if not username:
            return Response(
                text=json.dumps({'error': 'bad_request', 'message': 'username is required'}),
                status=400,
                content_type='application/json',
                headers={
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, OPTIONS',
                    'Access-Control-Allow-Headers': '*'
                }
            )
        
        # Убираем @ если есть
        clean_username = username.lstrip('@').strip()
        if not clean_username:
            return Response(
                text=json.dumps({'error': 'bad_request', 'message': 'username is required'}),
                status=400,
                content_type='application/json',
                headers={
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, OPTIONS',
                    'Access-Control-Allow-Headers': '*'
                }
            )
        logger.info(f"API /api/telegram/user: username={clean_username!r}, telethon_connected={telethon_client is not None}")
        
        # 1) Пробуем через userbot (Telethon) — так можно «из всего Telegram»
        telethon_data = await lookup_user_via_telethon(clean_username)
        if telethon_data:
            return Response(
                text=json.dumps(telethon_data, ensure_ascii=False),
                content_type='application/json',
                charset='utf-8',
                headers={
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, OPTIONS',
                    'Access-Control-Allow-Headers': '*'
                }
            )

        # 2) Fallback: Bot API (работает только если пользователь доступен для бота)
        try:
            chat = await bot.get_chat(f'@{clean_username}')
        except Exception as e:
            logger.error(f"BotAPI get_chat failed for {clean_username}: {e}")
            return Response(
                text=json.dumps({
                    'error': 'not_found',
                    'message': 'Пользователь не найден. Убедитесь, что указан верный @username.',
                    'details': str(e)
                }, ensure_ascii=False),
                status=404,
                content_type='application/json',
                headers={
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, OPTIONS',
                    'Access-Control-Allow-Headers': '*'
                }
            )
        
        # Пытаемся получить аватарку
        avatar_url = None
        try:
            # Пробуем получить фото профиля
            photos = await bot.get_user_profile_photos(chat.id, limit=1)
            if photos.total_count > 0 and photos.photos:
                # Берем самое большое фото
                photo = photos.photos[0][-1]  # Последний элемент - самое большое фото
                file = await bot.get_file(photo.file_id)
                # Формируем URL для скачивания
                avatar_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"
        except Exception as e:
            logger.warning(f"Не удалось получить аватарку для {clean_username}: {e}")
            # Если не получилось - оставляем None
        
        # Формируем ответ
        result = {
            'username': chat.username or clean_username,
            'firstName': chat.first_name or '',
            'lastName': chat.last_name or '',
            'avatar': avatar_url
        }
        
        return Response(
            text=json.dumps(result, ensure_ascii=False),
            content_type='application/json',
            charset='utf-8',
            headers={
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, OPTIONS',
                'Access-Control-Allow-Headers': '*'
            }
        )
        
    except Exception as e:
        logger.exception(f"Ошибка в get_telegram_user_handler: {e}")
        return Response(
            text=json.dumps({'error': 'internal_error', 'message': 'Ошибка сервера. Попробуйте позже.'}),
            status=500,
            content_type='application/json',
            headers={
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, OPTIONS',
                'Access-Control-Allow-Headers': '*'
            }
        )

def setup_http_server():
    """Настройка HTTP сервера для API"""
    @web.middleware
    async def error_middleware(request, handler):
        try:
            return await handler(request)
        except Exception as e:
            logger.exception(f"HTTP error on {request.method} {request.path_qs}: {e}")
            return Response(
                text=json.dumps({"error": "internal_error", "details": str(e)}, ensure_ascii=False),
                status=500,
                content_type="application/json",
                charset="utf-8",
                headers={
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET, OPTIONS",
                    "Access-Control-Allow-Headers": "*",
                },
            )

    app = web.Application(middlewares=[error_middleware])
    # Хранилище заказов Fragment.com (через сайт fragment.com/api по cookies+hash)
    # Заказы Fragment.com (через сайт fragment.com/api по cookies+hash)
    # order_id -> meta (type, recipient, quantity, created_at)
    app["fragment_site_orders"] = {}
    # TON-оплата через Tonkeeper: order_id -> { amount_nanoton, amount_ton, amount_rub, purchase, user_id, created_at }
    app["ton_orders"] = {}
    # event_id уже использованных входящих TON-переводов (при проверке по сумме без комментария)
    app["ton_verified_event_ids"] = set()
    # CryptoBot: invoice_id -> meta (context, user_id, purchase, amount_rub, created_at, delivered)
    app["cryptobot_orders"] = {}
    # Preflight для CORS
    app.router.add_route('OPTIONS', '/api/telegram/user', lambda r: Response(status=204, headers={
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': '*'
    }))
    # DonateHub preflight
    app.router.add_route('OPTIONS', '/api/donatehub/steam/topup', lambda r: Response(status=204, headers=_cors_headers()))
    app.router.add_route('OPTIONS', '/api/donatehub/order/{id}', lambda r: Response(status=204, headers=_cors_headers()))

    app.router.add_get('/api/telegram/user', get_telegram_user_handler)

    CRYPTOBOT_USDT_AMOUNT = float(os.getenv("CRYPTOBOT_USDT_AMOUNT", "1") or "1")

    async def api_config_handler(request):
        """Публичная конфигурация для фронтенда (бот, домен, CryptoBot USDT)"""
        try:
            me = await bot.get_me()
            bot_username = me.username or "JetStoreApp_bot"
            cfg = {
                "bot_username": bot_username,
                "web_app_url": WEB_APP_URL,
                "domain": "jetstoreapp.ru",
                "cryptobot_usdt_amount": CRYPTOBOT_USDT_AMOUNT,
            }
            return _json_response(cfg)
        except Exception as e:
            logger.error(f"/api/config error: {e}")
            return _json_response({
                "bot_username": "JetStoreApp_bot",
                "web_app_url": WEB_APP_URL,
                "domain": "jetstoreapp.ru",
                "cryptobot_usdt_amount": CRYPTOBOT_USDT_AMOUNT,
            })

    app.router.add_get('/api/config', api_config_handler)

    async def ton_rate_handler(request):
        """Курс TON→RUB через CoinPaprika (прокси для обхода CORS в Telegram WebView)."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("https://api.coinpaprika.com/v1/tickers/ton-toncoin?quotes=RUB") as resp:
                    data = await resp.json(content_type=None) if resp.content_type else {}
            rub_price = None
            if data and data.get("quotes") and data["quotes"].get("RUB"):
                rub_price = float(data["quotes"]["RUB"].get("price", 0) or 0)
            if not rub_price or rub_price <= 0:
                return _json_response({"TON": 600, "RUB_TON": 1 / 600})
            rub_ton = 1 / rub_price
            return _json_response({"TON": round(rub_price, 2), "RUB_TON": round(rub_ton, 8)})
        except Exception as e:
            logger.warning(f"TON rate fetch error: {e}")
            return _json_response({"TON": 600, "RUB_TON": 1 / 600})

    app.router.add_get('/api/ton-rate', ton_rate_handler)
    app.router.add_route('OPTIONS', '/api/ton-rate', lambda r: Response(status=204, headers=_cors_headers()))

    TON_PAYMENT_ADDRESS = {"value": (os.getenv("TON_PAYMENT_ADDRESS") or "").strip()}

    async def _get_ton_rate_rub() -> float:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("https://api.coinpaprika.com/v1/tickers/ton-toncoin?quotes=RUB") as resp:
                    data = await resp.json(content_type=None) if resp.content_type else {}
            if data and data.get("quotes") and data["quotes"].get("RUB"):
                p = float(data["quotes"]["RUB"].get("price", 0) or 0)
                if p > 0:
                    return round(p, 2)
        except Exception as e:
            logger.warning(f"TON rate for create-order: {e}")
        return 600.0

    async def ton_create_order_handler(request):
        addr = TON_PAYMENT_ADDRESS.get("value") or ""
        if not addr:
            return _json_response({"error": "not_configured", "message": "TON_PAYMENT_ADDRESS не задан"}, status=503)
        # Нормализуем адрес: TON Connect требует user-friendly адрес (EQ.../UQ...), raw 0:... не подходит.
        addr = str(addr).strip()
        # Если случайно передали ссылку ton://transfer/... — вытащим адрес.
        if addr.startswith("ton://transfer/"):
            addr = addr[len("ton://transfer/") :]
            addr = addr.split("?")[0].strip()
        if addr.startswith("https://") and "/transfer/" in addr:
            # Tonkeeper transfer link format: https://app.tonkeeper.com/transfer/<addr>?amount=...
            try:
                addr = addr.split("/transfer/", 1)[1].split("?", 1)[0].strip()
            except Exception:
                pass
        # raw → user-friendly через TonCenter
        if re.match(r"^(-1|0):[0-9a-fA-F]{32,64}$", addr):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        "https://toncenter.com/api/v2/packAddress",
                        params={"address": addr}
                    ) as resp:
                        data = await resp.json(content_type=None) if resp.content_type else {}
                packed = data.get("result") if isinstance(data, dict) and data.get("ok") else None
                if packed:
                    addr = str(packed).strip()
            except Exception as e:
                logger.warning(f"TON_PAYMENT_ADDRESS packAddress error: {e}")
        # Валидация: base64url 48 символов, обычно начинается с EQ/UQ
        if not re.match(r"^[A-Za-z0-9_-]{48}$", addr) or not (addr.startswith("EQ") or addr.startswith("UQ")):
            return _json_response({
                "error": "bad_config",
                "message": "TON_PAYMENT_ADDRESS должен быть user-friendly адресом вида EQ.../UQ... (48 символов) или raw 0:... (он будет упакован автоматически)"
            }, status=503)
        try:
            body = await request.json()
        except Exception:
            return _json_response({"error": "bad_request", "message": "Invalid JSON"}, status=400)
        amount_rub = float(body.get("amount_rub") or body.get("amount") or 0)
        if amount_rub <= 0:
            return _json_response({"error": "bad_request", "message": "amount_rub должен быть > 0"}, status=400)
        rate = await _get_ton_rate_rub()
        amount_ton = round(amount_rub / rate, 4)
        if amount_ton <= 0:
            return _json_response({"error": "bad_request", "message": "Сумма в TON слишком мала"}, status=400)
        amount_nanoton = int(round(amount_ton * 1e9))
        import uuid
        order_id = str(uuid.uuid4()).replace("-", "")[:24]
        purchase = body.get("purchase") or {}
        user_id = body.get("user_id") or (purchase.get("userId") if isinstance(purchase.get("userId"), str) else None) or "unknown"
        ton_orders = request.app.get("ton_orders") or {}
        ton_orders[order_id] = {
            "amount_nanoton": amount_nanoton,
            "amount_ton": amount_ton,
            "amount_rub": amount_rub,
            "purchase": purchase,
            "user_id": user_id,
            "created_at": time.time(),
        }
        request.app["ton_orders"] = ton_orders
        comment = order_id
        return _json_response({
            "success": True,
            "order_id": order_id,
            "payment_address": addr,
            "amount_ton": amount_ton,
            "amount_nanoton": amount_nanoton,
            "comment": comment,
            "ton_rate_rub": rate,
        })

    app.router.add_post("/api/ton/create-order", ton_create_order_handler)
    app.router.add_route("OPTIONS", "/api/ton/create-order", lambda r: Response(status=204, headers=_cors_headers()))

    async def ton_notify_handler(request):
        """Уведомление в рабочую группу о заявке на покупку TON (ручная обработка)."""
        if not TON_NOTIFY_CHAT_ID:
            return _json_response({"error": "not_configured", "message": "TON_NOTIFY_CHAT_ID не задан"}, status=503)
        try:
            body = await request.json()
        except Exception:
            return _json_response({"error": "bad_request", "message": "Invalid JSON"}, status=400)

        purchase = body.get("purchase") or {}
        method = (body.get("method") or "").strip()
        total_rub = body.get("total_rub") or body.get("totalAmount") or 0
        base_rub = body.get("base_rub") or body.get("baseAmount") or 0
        invoice_id = body.get("invoice_id") or None
        order_id = body.get("order_id") or None
        buyer = body.get("buyer") or {}

        wallet = (purchase.get("wallet") or "").strip()
        network = (purchase.get("network") or "").strip()
        ton_amount = purchase.get("ton_amount") or purchase.get("tonAmount") or purchase.get("amount_ton") or 0

        if not wallet or not network or not ton_amount:
            return _json_response({"error": "bad_request", "message": "wallet, network, ton_amount обязательны"}, status=400)

        try:
            ton_amount = float(ton_amount)
        except Exception:
            ton_amount = 0
        if ton_amount <= 0:
            return _json_response({"error": "bad_request", "message": "ton_amount должен быть > 0"}, status=400)

        try:
            total_rub = float(total_rub or 0)
        except Exception:
            total_rub = 0.0
        try:
            base_rub = float(base_rub or 0)
        except Exception:
            base_rub = 0.0

        buyer_id = (buyer.get("id") or buyer.get("user_id") or buyer.get("userId") or "").strip()
        buyer_username = buyer.get("username") or ""
        buyer_name = " ".join([str(buyer.get("first_name") or "").strip(), str(buyer.get("last_name") or "").strip()]).strip()
        buyer_line = ""
        if buyer_username:
            buyer_line = f"@{buyer_username}"
        elif buyer_name:
            buyer_line = buyer_name
        elif buyer_id:
            buyer_line = buyer_id
        else:
            buyer_line = "—"

        text = (
            "🟦 <b>Заявка: покупка TON</b>\n\n"
            f"Покупатель: {buyer_line}\n"
            + (f"ID: <code>{buyer_id}</code>\n" if buyer_id else "")
            + f"Сеть: <b>{network}</b>\n"
            + f"Кошелёк: <code>{wallet}</code>\n"
            + f"TON: <b>{ton_amount}</b>\n"
            + f"Оплата: <b>{total_rub:.2f} ₽</b>\n"
            + (f"Метод: <b>{method}</b>\n" if method else "")
            + (f"invoice_id: <code>{invoice_id}</code>\n" if invoice_id else "")
            + (f"order_id: <code>{order_id}</code>\n" if order_id else "")
        )

        try:
            await bot.send_message(TON_NOTIFY_CHAT_ID, text, parse_mode="HTML")
        except Exception as e:
            logger.error(f"TON notify send failed (chat={TON_NOTIFY_CHAT_ID}): {e}")
            return _json_response({"success": False, "error": "send_failed", "message": "Не удалось отправить уведомление в группу"}, status=502)

        return _json_response({"success": True})

    app.router.add_post("/api/ton/notify", ton_notify_handler)
    app.router.add_route("OPTIONS", "/api/ton/notify", lambda r: Response(status=204, headers=_cors_headers()))

    # ======== РЕФЕРАЛЬНАЯ СИСТЕМА (API) ========

    async def referral_purchase_handler(request):
        """
        Начисление реферального дохода с покупки пользователя.
        JSON: { "user_id": "...", "amount_rub": 123.45 }
        """
        try:
            body = await request.json()
        except Exception:
            return _json_response({"error": "bad_request", "message": "Invalid JSON"}, status=400)

        user_id = body.get("user_id")
        amount_rub = body.get("amount_rub") or body.get("amount")
        try:
            if user_id is None:
                raise ValueError("user_id required")
            uid = str(int(str(user_id).strip()))
            amount = float(amount_rub or 0)
        except Exception:
            return _json_response({"error": "bad_request", "message": "user_id(int) и amount_rub(number) обязательны"}, status=400)

        if amount <= 0:
            return _json_response({"error": "bad_request", "message": "amount_rub должен быть > 0"}, status=400)

        # Обновляем объёмы и доходы по цепочке 1–3 уровень
        await _load_referrals()
        user_ref = await _get_or_create_ref_user(uid)
        parent1 = user_ref.get("parent1")
        parent2 = user_ref.get("parent2")
        parent3 = user_ref.get("parent3")

        # Пользователь сам объёмом рефералов не считается, объём идёт наверх
        for pid, percent in (
            (parent1, 0.15),
            (parent2, 0.20),
            (parent3, 0.25),
        ):
            if not pid:
                continue
            pref = await _get_or_create_ref_user(pid)
            pref["volume_rub"] = float(pref.get("volume_rub") or 0.0) + amount
            bonus = amount * percent
            pref["earned_rub"] = float(pref.get("earned_rub") or 0.0) + bonus

        await _save_referrals()
        return _json_response({"success": True})

    app.router.add_post("/api/referral/purchase", referral_purchase_handler)
    app.router.add_route("OPTIONS", "/api/referral/purchase", lambda r: Response(status=204, headers=_cors_headers()))

    async def referral_stats_handler(request):
        """
        Статистика реферальной программы для пользователя.
        GET /api/referral/stats?user_id=...
        """
        user_id = request.rel_url.query.get("user_id", "").strip()
        if not user_id:
            return _json_response({"error": "bad_request", "message": "user_id required"}, status=400)
        try:
            uid = str(int(user_id))
        except Exception:
            uid = str(user_id)

        await _load_referrals()
        ref = await _get_or_create_ref_user(uid)

        # Подсчитываем количество рефералов по уровням
        lvl1 = len(ref.get("referrals_l1") or [])
        lvl2 = len(ref.get("referrals_l2") or [])
        lvl3 = len(ref.get("referrals_l3") or [])
        total_refs = lvl1 + lvl2 + lvl3

        earned_rub = float(ref.get("earned_rub") or 0.0)
        volume_rub = float(ref.get("volume_rub") or 0.0)

        # Курс TON (RUB за 1 TON)
        ton_rate = await _get_ton_rate_rub()
        if ton_rate <= 0:
            ton_rate = 600.0
        earned_ton = round(earned_rub / ton_rate, 6) if earned_rub > 0 else 0.0
        volume_ton = round(volume_rub / ton_rate, 6) if volume_rub > 0 else 0.0

        # Уровни JetRefs по объёму в TON:
        # 1 уровень: 0–4999.99 TON
        # 2 уровень: 5000–14999.99 TON
        # 3 уровень: 15000+ TON
        L2_TON = 5000.0
        L3_TON = 15000.0
        max_level = 3
        if volume_ton >= L3_TON:
            level = 3
            progress_percent = 100
            to_next_volume_rub = 0.0
            remaining_ton = 0.0
        else:
            if volume_ton >= L2_TON:
                level = 2
                base = L2_TON
                target = L3_TON
            else:
                level = 1
                base = 0.0
                target = L2_TON
            span = max(1.0, target - base)
            done = max(0.0, volume_ton - base)
            progress_percent = int(round(min(1.0, done / span) * 100))
            remaining_ton = max(0.0, target - volume_ton)
            to_next_volume_rub = remaining_ton * ton_rate

        to_next_volume_ton = round(remaining_ton, 6) if volume_ton < L3_TON else 0.0
        payload = {
            "user_id": uid,
            "earned_rub": round(earned_rub, 2),
            "earned_ton": earned_ton,
            "volume_rub": round(volume_rub, 2),
            "volume_ton": volume_ton,
            "referrals_level1": lvl1,
            "referrals_level2": lvl2,
            "referrals_level3": lvl3,
            "total_referrals": total_refs,
            "level": level,
            "max_level": max_level,
            "progress_percent": progress_percent,
            "to_next_volume_rub": round(to_next_volume_rub, 2),
            "to_next_volume_ton": to_next_volume_ton,
            "ton_rate_rub": ton_rate,
        }
        return _json_response(payload)

    app.router.add_get("/api/referral/stats", referral_stats_handler)
    app.router.add_route("OPTIONS", "/api/referral/stats", lambda r: Response(status=204, headers=_cors_headers()))

    async def referral_link_handler(request):
        """
        Реферальная ссылка с зашифрованным ID.
        GET /api/referral/link?user_id=...
        """
        user_id = request.rel_url.query.get("user_id", "").strip()
        if not user_id:
            return _json_response({"error": "bad_request", "message": "user_id required"}, status=400)
        try:
            uid = int(user_id)
        except (ValueError, TypeError):
            return _json_response({"error": "bad_request", "message": "user_id must be integer"}, status=400)
        try:
            me = await bot.get_me()
            bot_username = me.username or "JetStoreApp_bot"
        except Exception:
            bot_username = "JetStoreApp_bot"
        ref_code = _encrypt_ref_id(uid)
        url = f"https://t.me/{bot_username}?start=ref_{ref_code}"
        return _json_response({"url": url})

    app.router.add_get("/api/referral/link", referral_link_handler)
    app.router.add_route("OPTIONS", "/api/referral/link", lambda r: Response(status=204, headers=_cors_headers()))

    async def referral_withdraw_handler(request):
        """
        Создание заявки на вывод реферальных средств.
        JSON: { "user_id", "amount_rub", "method", "details" }
        """
        try:
            body = await request.json()
        except Exception:
            return _json_response({"error": "bad_request", "message": "Invalid JSON"}, status=400)

        user_id = body.get("user_id")
        amount_rub = body.get("amount_rub") or body.get("amount")
        method = (body.get("method") or "").strip()
        details = (body.get("details") or "").strip()

        try:
            if user_id is None:
                raise ValueError("user_id required")
            uid = str(int(str(user_id).strip()))
            amount = float(amount_rub or 0)
        except Exception:
            return _json_response({"error": "bad_request", "message": "user_id(int) и amount_rub(number) обязательны"}, status=400)

        if amount <= 0:
            return _json_response({"error": "bad_request", "message": "amount_rub должен быть > 0"}, status=400)

        # Минимальная сумма вывода: эквивалент 25 TON в рублях
        try:
            ton_rate = await _get_ton_rate_rub()
        except Exception:
            ton_rate = 600.0
        min_ton = 25.0
        min_rub = float(min_ton * (ton_rate or 600.0))
        if amount + 1e-6 < min_rub:
            return _json_response(
                {
                    "error": "too_small",
                    "message": f"Минимальная сумма вывода 25 TON (~{min_rub:.2f} ₽)",
                    "min_ton": min_ton,
                    "min_rub": round(min_rub, 2),
                },
                status=400,
            )

        await _load_referrals()
        ref = await _get_or_create_ref_user(uid)
        current_balance = float(ref.get("earned_rub") or 0.0)
        if amount > current_balance + 1e-6:
            return _json_response(
                {"error": "insufficient_funds", "message": "Недостаточно реферальных средств", "current_balance_rub": round(current_balance, 2)},
                status=400,
            )

        ref["earned_rub"] = current_balance - amount
        await _save_referrals()

        # Пытаемся отправить уведомление в рабочую группу
        if REFERRAL_WITHDRAW_CHAT_ID:
            try:
                # Получаем пользователя через бота (чтоб взять username / имя)
                try:
                    tg_user = await bot.get_chat(int(uid))
                except Exception:
                    tg_user = None
                username = getattr(tg_user, "username", None) if tg_user else None
                first_name = getattr(tg_user, "first_name", None) if tg_user else None
                last_name = getattr(tg_user, "last_name", None) if tg_user else None
                line = username and f"@{username}" or (first_name or "") + (" " + last_name if last_name else "")
                if not line:
                    line = uid

                text = (
                    "💸 <b>Новая заявка на вывод реферальных средств</b>\n\n"
                    f"Пользователь: {line}\n"
                    f"ID: <code>{uid}</code>\n"
                    f"Сумма вывода: <b>{amount:.2f} ₽</b>\n"
                    f"Метод: <b>{method or 'не указан'}</b>\n"
                    f"Реквизиты:\n<code>{details or 'не указаны'}</code>\n\n"
                    f"Остаток по реф.балансу: <b>{ref['earned_rub']:.2f} ₽</b>"
                )
                await bot.send_message(REFERRAL_WITHDRAW_CHAT_ID, text, parse_mode="HTML")
            except Exception as e:
                logger.error(f"Не удалось отправить заявку на вывод реферальных средств: {e}")

        return _json_response({"success": True, "new_balance_rub": round(ref["earned_rub"], 2)})

    app.router.add_post("/api/referral/withdraw", referral_withdraw_handler)
    app.router.add_route("OPTIONS", "/api/referral/withdraw", lambda r: Response(status=204, headers=_cors_headers()))

    # Продажа звёзд из мини-приложения: создать счёт XTR и сохранить данные выплаты
    async def sellstars_create_invoice_handler(request):
        try:
            body = await request.json()
        except Exception:
            return _json_response({"error": "bad_request", "message": "Invalid JSON"}, status=400)

        telegram_id = body.get("telegram_id") or body.get("user_id")
        if telegram_id is None:
            return _json_response({"error": "bad_request", "message": "telegram_id обязателен"}, status=400)
        try:
            telegram_id = int(telegram_id)
        except (TypeError, ValueError):
            return _json_response({"error": "bad_request", "message": "telegram_id должен быть числом"}, status=400)

        stars_amount = body.get("stars_amount")
        try:
            stars_amount = int(stars_amount) if stars_amount is not None else 0
        except (TypeError, ValueError):
            stars_amount = 0
        if stars_amount < 100:
            return _json_response({"error": "bad_request", "message": "Минимум 100 звёзд"}, status=400)
        if stars_amount > 50000:
            return _json_response({"error": "bad_request", "message": "Максимум 50 000 звёзд"}, status=400)

        method = (body.get("method") or "wallet").strip().lower()
        if method not in ("wallet", "sbp", "card"):
            return _json_response({"error": "bad_request", "message": "method: wallet, sbp или card"}, status=400)

        order_id = str(uuid.uuid4())
        payout_rub = round(stars_amount * STAR_BUY_RATE_RUB, 2)

        order_data = {
            "user_id": telegram_id,
            "username": (body.get("username") or "").strip(),
            "first_name": (body.get("first_name") or "").strip(),
            "last_name": (body.get("last_name") or "").strip(),
            "stars_amount": stars_amount,
            "method": method,
            "payout_rub": payout_rub,
        }
        if method == "wallet":
            order_data["wallet_address"] = (body.get("wallet_address") or "").strip()
            order_data["wallet_memo"] = (body.get("wallet_memo") or "").strip()
        elif method == "sbp":
            order_data["sbp_phone"] = (body.get("sbp_phone") or "").strip()
            order_data["sbp_bank"] = (body.get("sbp_bank") or "").strip()
        elif method == "card":
            order_data["card_number"] = (body.get("card_number") or "").strip()
            order_data["card_bank"] = (body.get("card_bank") or "").strip()

        PENDING_SELL_STARS_ORDERS[order_id] = order_data

        try:
            await bot.send_message(
                telegram_id,
                "Оплатите счёт для успешной продажи звёзд:",
                parse_mode=None,
            )
            await bot.send_invoice(
                chat_id=telegram_id,
                title="Продажа звёзд",
                description=f"Продажа {stars_amount} ⭐. Вы получите примерно {payout_rub:.2f} ₽.",
                payload=f"sell_stars:{order_id}",
                provider_token="1744374395:TEST:36675594277e9de887a6",
                currency="XTR",
                prices=[LabeledPrice(label="Звёзды", amount=stars_amount)],
                max_tip_amount=0,
                need_name=False,
                need_phone_number=False,
                need_email=False,
                need_shipping_address=False,
                is_flexible=False,
            )
        except Exception as e:
            PENDING_SELL_STARS_ORDERS.pop(order_id, None)
            logger.exception(f"sellstars create-invoice send_invoice: {e}")
            return _json_response({"error": "send_failed", "message": str(e)}, status=502)

        return _json_response({"success": True, "order_id": order_id})

    app.router.add_post("/api/sellstars/create-invoice", sellstars_create_invoice_handler)
    app.router.add_route("OPTIONS", "/api/sellstars/create-invoice", lambda r: Response(status=204, headers=_cors_headers()))

    async def ton_pack_address_handler(request):
        """Конвертация raw-адреса TON (0:hex) в user-friendly через TonCenter."""
        raw = request.rel_url.query.get("address", "").strip()
        if not raw or not re.match(r"^(-1|0):[0-9a-fA-F]{32,64}$", raw):
            return _json_response({"error": "bad_request", "message": "Invalid raw address"}, status=400)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://toncenter.com/api/v2/packAddress",
                    params={"address": raw}
                ) as resp:
                    data = await resp.json(content_type=None) if resp.content_type else {}
            result = data.get("result") if data.get("ok") else None
            if result:
                return _json_response({"address": result})
            return _json_response({"error": "toncenter_error"}, status=502)
        except Exception as e:
            logger.warning(f"ton packAddress error: {e}")
            return _json_response({"error": str(e)}, status=502)

    app.router.add_get('/api/ton/pack-address', ton_pack_address_handler)
    app.router.add_route('OPTIONS', '/api/ton/pack-address', lambda r: Response(status=204, headers=_cors_headers()))

    async def telethon_status_handler(request):
        try:
            payload = {
                "telethon_configured": bool(TELEGRAM_API_ID > 0 and TELEGRAM_API_HASH and TELEGRAM_STRING_SESSION),
                "telethon_connected": bool(telethon_client is not None),
                "cache_size": len(_tg_lookup_cache),
                "sources": {
                    "env_api_id": bool(os.getenv("TELEGRAM_API_ID")),
                    "env_api_hash": bool(os.getenv("TELEGRAM_API_HASH")),
                    "env_string_session": bool(os.getenv("TELEGRAM_STRING_SESSION") or os.getenv("TELETHON_STRING_SESSION")),
                    "file_config_exists": os.path.exists(_cfg_file),
                    "file_session_exists": os.path.exists(_session_file),
                },
                "lengths": {
                    "api_hash_len": len(TELEGRAM_API_HASH or ""),
                    "session_len": len(TELEGRAM_STRING_SESSION or ""),
                }
            }
            return Response(
                text=json.dumps(payload, ensure_ascii=False),
                content_type='application/json',
                charset='utf-8',
                headers={
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, OPTIONS',
                    'Access-Control-Allow-Headers': '*'
                }
            )
        except Exception as e:
            logger.error(f"/api/telethon/status error: {e}")
            return Response(
                text=json.dumps({"error": "internal_error", "details": str(e)}, ensure_ascii=False),
                status=500,
                content_type='application/json',
                charset='utf-8',
                headers={
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, OPTIONS',
                    'Access-Control-Allow-Headers': '*'
                }
            )

    app.router.add_get('/api/telethon/status', telethon_status_handler)

    async def donatehub_status_handler(request):
        try:
            ok = bool(DONATEHUB_USERNAME and DONATEHUB_PASSWORD)
            return _json_response({
                "configured": ok,
                "has_2fa_code": bool(DONATEHUB_2FA_CODE),
                "config_file_exists": os.path.exists(_donatehub_cfg_file)
            })
        except Exception as e:
            return _json_response({"error": "internal_error", "details": str(e)}, status=500)

    async def donatehub_steam_topup_handler(request):
        try:
            body = await request.json()
        except Exception:
            body = {}

        account = str(body.get("account", "")).strip()
        amount_local = body.get("amount", 0)
        currency = str(body.get("currency", "RUB")).strip().upper()

        if not account:
            return _json_response({"error": "bad_request", "message": "account is required"}, status=400)
        try:
            amount_local = float(amount_local)
        except Exception:
            return _json_response({"error": "bad_request", "message": "amount must be a number"}, status=400)
        if amount_local <= 0:
            return _json_response({"error": "bad_request", "message": "amount must be > 0"}, status=400)

        # Конвертируем в USD (DonateHub использует долларовые курсы Steam)
        amount_usd, meta = await _convert_to_usd(amount_local, currency)
        if amount_usd < 1 or amount_usd > 1000:
            return _json_response({
                "error": "bad_request",
                "message": "amount in USD must be between 1 and 1000",
                "amount_usd": amount_usd
            }, status=400)

        # 1) Проверка и получение custom_id + total
        check = await _donatehub_request("GET", "/create_steam_order", params={"account": account, "amount": amount_usd})
        custom_id = check.get("custom_id")
        total = check.get("total")
        if not custom_id:
            return _json_response({"error": "donatehub_error", "message": "custom_id missing", "raw": check}, status=502)

        # 2) Создание заказа
        order = await _donatehub_request("POST", "/create_steam_order", json_body={"custom_id": custom_id})
        # order: {id, amount, status, description, created_at}

        return _json_response({
            "provider": "donatehub",
            "account": account,
            "currency": meta["currency"],
            "rate_usd_to_local": meta["rate"],
            "amount_local": amount_local,
            "amount_usd": amount_usd,
            "check_total": total,
            "custom_id": custom_id,
            "order": order
        })

    async def donatehub_order_status_handler(request):
        order_id = request.match_info.get("id", "").strip()
        if not order_id:
            return _json_response({"error": "bad_request", "message": "id is required"}, status=400)
        data = await _donatehub_request("GET", f"/order/{order_id}")
        return _json_response(data)

    app.router.add_get("/api/donatehub/status", donatehub_status_handler)
    app.router.add_post("/api/donatehub/steam/topup", donatehub_steam_topup_handler)
    app.router.add_get("/api/donatehub/order/{id}", donatehub_order_status_handler)
    
    # Crypto Pay (CryptoBot)
    _cryptobot_cfg_early = _read_json_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), "cryptobot_config.json"))
    CRYPTO_PAY_TOKEN = _get_env_clean("CRYPTO_PAY_TOKEN") or _cryptobot_cfg_early.get("api_token", "")
    CRYPTO_PAY_BASE = "https://pay.crypt.bot/api"

    # Fragment.com (сайт) — вызов fragment.com/api через cookies + hash (как в ezstar).
    _script_dir = os.path.dirname(os.path.abspath(__file__))
    _fragment_site_cfg = _read_json_file(os.path.join(_script_dir, "fragment_site_config.json"))
    if not _fragment_site_cfg:
        _fragment_site_cfg = _read_json_file(os.path.join(os.getcwd(), "fragment_site_config.json"))
    if not _fragment_site_cfg:
        _parent_cfg = _read_json_file(os.path.join(os.path.dirname(_script_dir), "fragment_site_config.json"))
        if _parent_cfg:
            _fragment_site_cfg = _parent_cfg
    if not _fragment_site_cfg:
        logger.warning("fragment_site_config.json не найден (искали в %s и cwd); задайте TONAPI_KEY и MNEMONIC в переменных окружения", _script_dir)
    if not TON_PAYMENT_ADDRESS.get("value") and _fragment_site_cfg:
        TON_PAYMENT_ADDRESS["value"] = str(_fragment_site_cfg.get("ton_payment_address") or "").strip()
    FRAGMENT_SITE_COOKIES = (
        _get_env_clean("FRAGMENT_SITE_COOKIES")
        or _get_env_clean("FRAGMENT_COOKIES")
        or str(_fragment_site_cfg.get("cookies", "") or "").strip()
    )
    FRAGMENT_SITE_HASH = (
        _get_env_clean("FRAGMENT_SITE_HASH")
        or _get_env_clean("FRAGMENT_HASH")
        or str(_fragment_site_cfg.get("hash", "") or "").strip()
    )
    FRAGMENT_SITE_ENABLED = bool(FRAGMENT_SITE_COOKIES and FRAGMENT_SITE_HASH)
    # TON-кошелёк бота для отправки TON в Fragment (как в ezstar: бот сам платит Fragment, звёзды приходят получателю).
    TONAPI_KEY = _get_env_clean("TONAPI_KEY") or str(_fragment_site_cfg.get("tonapi_key", "") or "").strip()
    _mnemonic_raw = _get_env_clean("MNEMONIC") or _fragment_site_cfg.get("mnemonic")
    if isinstance(_mnemonic_raw, str):
        MNEMONIC = [s.strip() for s in _mnemonic_raw.replace(",", " ").split() if s.strip()] if _mnemonic_raw else []
    elif isinstance(_mnemonic_raw, list):
        MNEMONIC = [str(x).strip() for x in _mnemonic_raw if str(x).strip()]
    else:
        MNEMONIC = []
    TON_WALLET_ENABLED = bool(TONAPI_KEY and len(MNEMONIC) >= 24)

    def _fragment_site_headers(*, referer: str) -> dict:
        return {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "origin": "https://fragment.com",
            "referer": referer,
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "cookie": FRAGMENT_SITE_COOKIES,
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest",
        }

    async def _fragment_site_post(method_payload: dict, *, referer: str) -> dict:
        if not FRAGMENT_SITE_ENABLED:
            raise RuntimeError("FRAGMENT_SITE_COOKIES/FRAGMENT_SITE_HASH not configured")
        params = {"hash": FRAGMENT_SITE_HASH}
        url = "https://fragment.com/api"
        headers = _fragment_site_headers(referer=referer)
        async with aiohttp.ClientSession() as session:
            async with session.post(url, params=params, headers=headers, data=method_payload) as resp:
                data = await resp.json(content_type=None)
                if resp.status >= 400:
                    raise RuntimeError(f"fragment.com/api error {resp.status}: {data}")
                return data if isinstance(data, dict) else {"data": data}

    def _fragment_encoded(encoded_string: str) -> str:
        """Декодирование payload из Fragment (как ezstar api/fragment.encoded)."""
        s = (encoded_string or "").strip()
        missing = len(s) % 4
        if missing:
            s += "=" * (4 - missing)
        try:
            decoded = base64.b64decode(s).decode("utf-8", errors="ignore")
            for i, c in enumerate(decoded):
                if c.isdigit():
                    return decoded[i:]
            return decoded
        except Exception:
            return encoded_string

    def _extract_any_url(obj) -> Optional[str]:
        # Пытаемся найти URL в ответе Fragment (встречается как payment_url/link/url или внутри HTML)
        if isinstance(obj, dict):
            for k in ("payment_url", "paymentUrl", "pay_url", "payUrl", "url", "link"):
                v = obj.get(k)
                if isinstance(v, str) and v.startswith(("http://", "https://")):
                    return v
            for v in obj.values():
                u = _extract_any_url(v)
                if u:
                    return u
        elif isinstance(obj, list):
            for v in obj:
                u = _extract_any_url(v)
                if u:
                    return u
        elif isinstance(obj, str):
            m = re.search(r"https?://[^\s\"'<>]+", obj)
            if m:
                return m.group(0)
        return None

    # --- ezstar: получение адреса получателя (found.recipient), init, getBuyStarsLink → transaction.messages[0] ---
    async def _fragment_get_recipient_address(username: str) -> tuple:
        """Поиск получателя (searchStarsRecipient). Возвращает (name, address) как в ezstar."""
        referer = f"https://fragment.com/stars/buy?recipient={username}&quantity=50"
        payload = {"query": username, "quantity": "", "method": "searchStarsRecipient"}
        data = await _fragment_site_post(payload, referer=referer)
        found = (data or {}).get("found")
        if not found or not isinstance(found, dict):
            raise RuntimeError("Fragment: получатель не найден (found)")
        name = found.get("name")
        address = found.get("recipient")
        if not address:
            raise RuntimeError("Fragment: у получателя нет recipient (address)")
        return (name or username, str(address).strip())

    async def _fragment_init_buy(recipient_address: str, quantity: int) -> str:
        """Инициализация покупки (initBuyStarsRequest). recipient = address из found.recipient. Возвращает req_id."""
        referer = "https://fragment.com/stars/buy?recipient=test&quantity=50"
        payload = {"recipient": recipient_address, "quantity": int(quantity), "method": "initBuyStarsRequest"}
        data = await _fragment_site_post(payload, referer=referer)
        req_id = (data or {}).get("req_id") or (data or {}).get("id")
        if not req_id:
            req_id = ((data or {}).get("data") or {}).get("id") if isinstance((data or {}).get("data"), dict) else None
        if not req_id:
            raise RuntimeError(f"Fragment initBuyStarsRequest: нет req_id в ответе: {data}")
        return str(req_id)

    async def _fragment_get_buy_link(req_id: str) -> tuple:
        """Получение данных для оплаты (getBuyStarsLink). Возвращает (address, amount_nanoton, payload_b64) как в ezstar."""
        referer = "https://fragment.com/stars/buy?recipient=test&quantity=50"
        payload = {"transaction": "1", "id": str(req_id), "show_sender": "0", "method": "getBuyStarsLink"}
        data = await _fragment_site_post(payload, referer=referer)
        tx = (data or {}).get("transaction")
        if not tx or not isinstance(tx, dict):
            raise RuntimeError("Fragment getBuyStarsLink: нет transaction в ответе")
        messages = tx.get("messages") or []
        if not messages or not isinstance(messages[0], dict):
            raise RuntimeError("Fragment getBuyStarsLink: нет transaction.messages[0]")
        msg = messages[0]
        address = msg.get("address")
        amount = msg.get("amount")
        payload_b64 = msg.get("payload") or ""
        if not address:
            raise RuntimeError("Fragment getBuyStarsLink: нет address в messages[0]")
        if amount is None:
            raise RuntimeError("Fragment getBuyStarsLink: нет amount в messages[0]")
        return (str(address).strip(), int(amount), str(payload_b64))

    async def _ton_wallet_send_safe(address: str, amount_nanoton: int, body_payload: str) -> tuple[Optional[str], Optional[str]]:
        """Возвращает (tx_hash, None) при успехе или (None, error_message) при ошибке."""
        if not TONAPI_KEY or not MNEMONIC:
            return (None, "TONAPI_KEY или MNEMONIC не заданы")
        try:
            from tonutils.client import TonapiClient
            from tonutils.utils import to_amount
            from tonutils.wallet import WalletV5R1
            client = TonapiClient(api_key=TONAPI_KEY, is_testnet=False)
            wallet, _, _, _ = WalletV5R1.from_mnemonic(client, MNEMONIC)
            wallet_addr = getattr(wallet, "address", None)
            if wallet_addr is not None:
                addr_str = getattr(wallet_addr, "to_str", lambda: str(wallet_addr))()
                if addr_str:
                    fee_buffer = 50_000_000  # 0.05 TON на комиссию
                    try:
                        async with aiohttp.ClientSession() as sess:
                            async with sess.get(
                                f"https://tonapi.io/v2/accounts/{addr_str}",
                                headers={"Authorization": f"Bearer {TONAPI_KEY}"}
                            ) as resp:
                                if resp.status == 200:
                                    data = await resp.json(content_type=None) if resp.content_type else {}
                                    bal_raw = data.get("balance")
                                    bal = 0
                                    if isinstance(bal_raw, (int, float)):
                                        bal = int(bal_raw)
                                    elif isinstance(bal_raw, str):
                                        bal = int(float(bal_raw)) if bal_raw else 0
                                    elif isinstance(bal_raw, dict):
                                        ton_val = bal_raw.get("ton") or bal_raw.get("ton_string") or 0
                                        nano_val = bal_raw.get("nanoton") or bal_raw.get("nano") or 0
                                        if isinstance(ton_val, str):
                                            ton_val = float(ton_val.replace(",", ".") or 0)
                                        bal = int(float(ton_val or 0) * 1e9) + int(nano_val or 0)
                                    need = amount_nanoton + fee_buffer
                                    if bal > 0 and bal < need:
                                        return (None, f"Недостаточно TON на кошельке бота: нужно {amount_nanoton/1e9:.4f} TON + ~0.05 комиссия, доступно {bal/1e9:.4f} TON")
                                    elif bal == 0:
                                        logger.info("TON balance check: 0 or unknown format (bal_raw=%s), пробуем отправить", type(bal_raw).__name__)
                    except Exception as be:
                        logger.warning("Balance check failed: %s", be)
            amount_val = to_amount(amount_nanoton, 9, 9)
            if asyncio.iscoroutinefunction(wallet.transfer):
                tx_hash = await wallet.transfer(destination=address, amount=amount_val, body=body_payload)
            else:
                tx_hash = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: wallet.transfer(destination=address, amount=amount_val, body=body_payload)
                )
            return (str(tx_hash) if tx_hash else None, None)
        except Exception as e:
            err = str(e).strip() or repr(e)
            logger.exception("TON wallet send error: %s", e)
            if "insufficient" in err.lower() or "balance" in err.lower() or "not enough" in err.lower():
                return (None, f"Недостаточно TON: {err}")
            return (None, err)

    async def _fragment_site_create_star_order(app_: web.Application, *, recipient: str, stars_amount: int) -> dict:
        """Создание заказа: только валидация + при наличии кошелька не отдаём ссылку (оплата через CryptoBot, затем deliver-stars)."""
        referer = f"https://fragment.com/stars/buy?recipient={recipient}&quantity={stars_amount}"
        search_payload = {"query": recipient, "quantity": "", "method": "searchStarsRecipient"}
        search = await _fragment_site_post(search_payload, referer=referer)
        found = (search or {}).get("found")
        if not found or not isinstance(found, dict) or not found.get("recipient"):
            raise RuntimeError("Fragment: получатель не найден")
        if TON_WALLET_ENABLED:
            return {"success": True, "order_id": None, "payment_url": None, "mode": "wallet"}
        address = found.get("recipient")
        init_payload = {"recipient": address, "quantity": int(stars_amount), "method": "initBuyStarsRequest"}
        init = await _fragment_site_post(init_payload, referer=referer)
        req_id = (init or {}).get("req_id") or (init or {}).get("id") or str((init or {}).get("data") or {}).get("id", "")
        if not req_id:
            raise RuntimeError(f"Fragment initBuyStarsRequest: нет req_id в ответе: {init}")
        link_payload = {"transaction": "1", "id": str(req_id), "show_sender": "0", "method": "getBuyStarsLink"}
        link = await _fragment_site_post(link_payload, referer=referer)
        pay_url = _extract_any_url(link)
        if isinstance(app_.get("fragment_site_orders"), dict):
            app_["fragment_site_orders"][req_id] = {"type": "stars", "recipient": recipient, "quantity": int(stars_amount), "created_at": time.time()}
        return {"success": True, "order_id": req_id, "payment_url": pay_url or None, "order": {"search": search, "init": init, "link": link}}

    # Проверка оплаты (Fragment.com / TonKeeper / CryptoBot).
    async def payment_check_handler(request):
        try:
            body = await request.json()
        except Exception:
            body = {}
        purchase = body.get("purchase") or {}
        purchase_type = (purchase.get("type") or purchase.get("Type") or "").strip()
        is_stars = purchase_type == "stars" or (purchase.get("stars_amount") is not None and purchase.get("stars_amount") != 0)
        is_premium = purchase_type == "premium" or (purchase.get("months") is not None and purchase.get("months") != 0)
        order_id = (body.get("order_id") or body.get("orderId") or "").strip()
        transaction_id = (body.get("transaction_id") or body.get("transactionId") or "").strip()
        invoice_id = body.get("invoice_id")
        method = (body.get("method") or "").strip().lower()
        # Fragment.com (site): пробуем проверить статус по order_id (req_id)
        # Важно: пытаемся проверять даже если сервер перезапускался и meta не сохранилось.
        if is_stars and order_id and FRAGMENT_SITE_ENABLED:
            try:
                site_orders = request.app.get("fragment_site_orders") or {}
                meta = site_orders.get(order_id) if isinstance(site_orders, dict) else None
                meta = meta or {}
                rec = (meta.get("recipient") or "").strip()
                qty = meta.get("quantity")
                referer = "https://fragment.com/stars/buy"
                if rec and qty:
                    referer = f"https://fragment.com/stars/buy?recipient={rec}&quantity={qty}"
                link_payload = {"transaction": "1", "id": str(order_id), "show_sender": "0", "method": "getBuyStarsLink"}
                link = await _fragment_site_post(link_payload, referer=referer)
                if _fragment_site_is_paid(link):
                    return _json_response({"paid": True, "order_id": order_id, "delivered_by_fragment": True})
                return _json_response({"paid": False, "order_id": order_id})
            except Exception as e:
                logger.warning(f"Fragment(site) payment check failed for order_id={order_id}: {e}")
                return _json_response({"paid": False, "order_id": order_id})
        # TON (Tonkeeper): строгая проверка через TonAPI по сумме и уникальному order_id в действии
        _ton_addr = (TON_PAYMENT_ADDRESS.get("value") or "").strip()
        if method == "ton" and order_id and _ton_addr and TONAPI_KEY:
            ton_orders = request.app.get("ton_orders") or {}
            order = ton_orders.get(order_id) if isinstance(ton_orders, dict) else None
            if order:
                try:
                    addr = _ton_addr.strip()
                    if not re.match(r"^[A-Za-z0-9_-]{48}$", addr):
                        addr = addr.replace(" ", "").replace("://", "")
                    url = f"https://tonapi.io/v2/accounts/{addr}/events?limit=50"
                    async with aiohttp.ClientSession() as session:
                        async with session.get(
                            url,
                            headers={"Authorization": f"Bearer {TONAPI_KEY}", "Content-Type": "application/json"}
                        ) as resp:
                            data = await resp.json(content_type=None) if resp.content_type else {}
                    events = data.get("events") or []
                    want_nanoton = int(order.get("amount_nanoton") or 0)
                    # Проходим по всем TonTransfer и ищем тот, в JSON которого встречается order_id и хватает суммы
                    for ev in events:
                        for act in ev.get("actions") or []:
                            if act.get("type") == "TonTransfer":
                                try:
                                    blob = json.dumps(act, ensure_ascii=False)
                                except Exception:
                                    blob = str(act)
                                if order_id not in blob:
                                    continue
                                amount = int(act.get("amount") or 0)
                                if amount >= max(0, want_nanoton - int(1e6)):
                                    logger.info("TON payment confirmed via TonAPI for order_id=%s, amount=%s", order_id, amount)
                                    return _json_response({"paid": True, "order_id": order_id, "method": "ton"})
                except Exception as e:
                    logger.warning(f"TON payment check failed for order_id={order_id}: {e}")
            return _json_response({"paid": False, "order_id": order_id})
        if invoice_id and method == "cryptobot" and CRYPTO_PAY_TOKEN:
            try:
                # Метаданные инвойса, сохранённые при создании
                order_meta = None
                try:
                    orders = request.app.get("cryptobot_orders")
                    if isinstance(orders, dict):
                        order_meta = orders.get(str(invoice_id))
                except Exception as meta_err:
                    logger.warning("cryptobot order meta read failed for %s: %s", invoice_id, meta_err)

                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"{CRYPTO_PAY_BASE}/getInvoices",
                        headers={"Content-Type": "application/json", "Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN},
                        params={"invoice_ids": str(invoice_id), "status": "paid"}
                    ) as resp:
                        cdata = await resp.json(content_type=None) if resp.content_type else {}
                        if cdata.get("ok"):
                            result = cdata.get("result")
                            # Crypto Pay API обычно возвращает {"result": {"items": [...]}}
                            items = []
                            if isinstance(result, dict):
                                items = result.get("items") or []
                            elif isinstance(result, list):
                                items = result
                            if isinstance(items, list):
                                paid_invoice = None
                                for inv in items:
                                    if isinstance(inv, dict) and str(inv.get("invoice_id")) == str(invoice_id) and inv.get("status") == "paid":
                                        paid_invoice = inv
                                        break
                                if paid_invoice:
                                    # Сумма в рублях для реферальной системы и логики выдачи
                                    amount_rub = None
                                    if order_meta and isinstance(order_meta.get("amount_rub"), (int, float)):
                                        amount_rub = round(float(order_meta["amount_rub"]), 2)

                                    response_data = {"paid": True, "invoice_id": invoice_id}
                                    if amount_rub and amount_rub > 0:
                                        response_data["amount_rub"] = amount_rub

                                    # Если это покупка звёзд через CryptoBot, можем пометить, что выдача выполнена
                                    if order_meta and order_meta.get("context") == "purchase":
                                        purchase_meta = order_meta.get("purchase") or {}
                                        if purchase_meta.get("type") == "stars":
                                            # Здесь пока не запускаем Fragment-выдачу напрямую, так как
                                            # сейчас используется сайт fragment.com и/или TonKeeper/webhook.
                                            # Главное — не доверять данным от клиента.
                                            try:
                                                orders = request.app.get("cryptobot_orders")
                                                if isinstance(orders, dict):
                                                    orders[str(invoice_id)]["delivered"] = True
                                            except Exception as upd_err:
                                                logger.warning("Failed to mark cryptobot order delivered: %s", upd_err)

                                    return _json_response(response_data)
            except Exception as e:
                logger.warning(f"Crypto Pay check invoice {invoice_id}: {e}")
        if invoice_id and method == "cryptobot":
            return _json_response({"paid": False})
        # Fragment.com (site flow): если не нашли заказ в памяти — подтвердить оплату не можем.
        if is_stars or is_premium:
            return _json_response({"paid": False, "order_id": order_id or None})
        if transaction_id:
            pass
        return _json_response({"paid": False})
    
    app.router.add_post("/api/payment/check", payment_check_handler)
    app.router.add_route("OPTIONS", "/api/payment/check", lambda r: Response(status=204, headers=_cors_headers()))
    
    async def fragment_status_handler(request):
        """Healthcheck Fragment.com (cookies+hash) и TON-кошелька (ezstar)."""
        if not FRAGMENT_SITE_ENABLED:
            return _json_response({"configured": False, "api_ok": False, "mode": "site", "wallet_enabled": False}, status=503)
        return _json_response({
            "configured": True,
            "api_ok": True,
            "mode": "wallet" if TON_WALLET_ENABLED else "site",
            "wallet_enabled": TON_WALLET_ENABLED,
        })

    app.router.add_get("/api/fragment/status", fragment_status_handler)
    app.router.add_route("OPTIONS", "/api/fragment/status", lambda r: Response(status=204, headers=_cors_headers()))

    async def fragment_deliver_stars_handler(request):
        """
        Выдача звёзд как в ezstar: бот отправляет TON с своего кошелька в Fragment
        (get address → init → get link → send TON).

        ВНИМАНИЕ: этот хендлер больше не должен вызываться напрямую с клиентской стороны.
        Публичный HTTP‑эндпоинт /api/fragment/deliver-stars отключён, чтобы злоумышленник
        не мог бесплатно выдавать себе звёзды, просто сделав POST‑запрос.

        Логику выдачи звёзд следует вызывать только из доверенного бекенда
        (например, из webhook‑обработчика платёжной системы или админ‑скрипта),
        передавая сюда уже проверенные данные.
        """
        if not TON_WALLET_ENABLED:
            return _json_response({
                "error": "not_configured",
                "message": "TONAPI_KEY и MNEMONIC не заданы. Укажите в fragment_site_config.json (рядом с bot.py или в текущей папке) или в переменных окружения TONAPI_KEY и MNEMONIC (24 слова через пробел)."
            }, status=503)
        try:
            body = await request.json()
        except Exception:
            return _json_response({"error": "bad_request", "message": "Invalid JSON"}, status=400)
        recipient = (body.get("recipient") or body.get("username") or "").strip().lstrip("@")
        stars_amount = body.get("stars_amount") or body.get("quantity")
        if not recipient or not stars_amount:
            return _json_response({"error": "bad_request", "message": "recipient и stars_amount обязательны"}, status=400)
        stars_amount = int(stars_amount)
        if stars_amount < 50 or stars_amount > 1_000_000:
            return _json_response({"error": "bad_request", "message": "stars_amount 50..1000000"}, status=400)
        if not FRAGMENT_SITE_ENABLED:
            return _json_response({"error": "not_configured", "message": "Fragment cookies+hash не заданы"}, status=503)
        try:
            _, recipient_address = await _fragment_get_recipient_address(recipient)
            req_id = await _fragment_init_buy(recipient_address, stars_amount)
            tx_address, amount_nanoton, payload_b64 = await _fragment_get_buy_link(req_id)
            payload_decoded = _fragment_encoded(payload_b64)
            tx_hash, send_err = await _ton_wallet_send_safe(tx_address, amount_nanoton, payload_decoded)
            if not tx_hash:
                msg = send_err or "Не удалось отправить TON"
                return _json_response({"error": "wallet_error", "message": msg}, status=502)
            logger.info("Fragment stars delivered: recipient=%s, amount=%s, tx=%s", recipient, stars_amount, tx_hash)
            return _json_response({"success": True, "recipient": recipient, "stars_amount": stars_amount, "tx_hash": tx_hash})
        except RuntimeError as e:
            logger.warning("Fragment deliver-stars: %s", e)
            return _json_response({"error": "fragment_error", "message": str(e)}, status=400)
        except Exception as e:
            logger.exception("Fragment deliver-stars error: %s", e)
            return _json_response({"error": "internal_error", "message": str(e)}, status=500)

    # Создание заказа Fragment: при наличии TON-кошелька — только валидация (оплата CryptoBot → deliver-stars). Иначе — ссылка на оплату TON.
    async def fragment_create_star_order_handler(request):
        """Создать заказ на звёзды: возвращает order_id и payment_url (если API отдаёт), фронт открывает ссылку оплаты TonKeeper"""
        try:
            body = await request.json()
        except Exception:
            return _json_response({"error": "bad_request", "message": "Invalid JSON"}, status=400)
        recipient = (body.get("recipient") or body.get("username") or "").strip().lstrip("@")
        stars_amount = body.get("stars_amount") or body.get("quantity")
        if not stars_amount:
            return _json_response({"error": "bad_request", "message": "stars_amount is required"}, status=400)
        stars_amount = int(stars_amount)
        if stars_amount < 50 or stars_amount > 1_000_000:
            return _json_response({"error": "bad_request", "message": "stars_amount 50..1000000"}, status=400)
        if not recipient:
            return _json_response({"error": "bad_request", "message": "recipient is required"}, status=400)

        if not FRAGMENT_SITE_ENABLED:
            return _json_response({
                "error": "not_configured",
                "message": "Set FRAGMENT_SITE_COOKIES + FRAGMENT_SITE_HASH (or FRAGMENT_COOKIES + FRAGMENT_HASH)"
            }, status=503)
        try:
            if TON_WALLET_ENABLED:
                await _fragment_get_recipient_address(recipient)
                return _json_response({
                    "success": True,
                    "order_id": None,
                    "payment_url": None,
                    "stars_amount": stars_amount,
                    "recipient": recipient,
                    "mode": "wallet",
                    # Более нейтральное сообщение для фронта: оплата уже открыта,
                    # дальше пользователь просто подтверждает.
                    "message": "Мы открыли способ оплаты. После оплаты вернитесь в мини‑приложение и нажмите «Подтвердить оплату» — звёзды будут отправлены автоматически.",
                })
            res = await _fragment_site_create_star_order(request.app, recipient=recipient, stars_amount=stars_amount)
            return _json_response({
                "success": True,
                "order_id": res.get("order_id"),
                "payment_url": res.get("payment_url"),
                "order": res.get("order"),
                "stars_amount": stars_amount,
                "recipient": recipient,
                "mode": "site",
            })
        except Exception as e:
            logger.error(f"Fragment create star order error: {e}")
            return _json_response({"error": "fragment_site_error", "message": str(e)}, status=502)

    async def fragment_create_premium_order_handler(request):
        """Создать заказ на Premium: возвращает order_id и payment_url (если есть), фронт открывает оплату TonKeeper"""
        return _json_response(
            {
                "error": "not_supported",
                "message": "Premium отключён: оставлен только режим Stars через fragment.com cookies+hash.",
            },
            status=501,
        )

    app.router.add_post("/api/fragment/create-star-order", fragment_create_star_order_handler)
    app.router.add_route("OPTIONS", "/api/fragment/create-star-order", lambda r: Response(status=204, headers=_cors_headers()))
    app.router.add_post("/api/fragment/create-premium-order", fragment_create_premium_order_handler)
    app.router.add_route("OPTIONS", "/api/fragment/create-premium-order", lambda r: Response(status=204, headers=_cors_headers()))

    # Health check
    async def api_health_handler(request):
        return _json_response({"ok": True, "service": "jet-store-bot", "message": "Бот работает"})
    app.router.add_get('/api/health', api_health_handler)

    # CryptoBot status + проверка токена через getMe
    async def cryptobot_status_handler(request):
        has_token = bool(CRYPTO_PAY_TOKEN)
        token_source = "env" if _get_env_clean("CRYPTO_PAY_TOKEN") else ("file" if _cryptobot_cfg_early.get("api_token") else "none")
        result = {
            "configured": has_token,
            "token_source": token_source,
            "token_preview": (CRYPTO_PAY_TOKEN[:10] + "...") if has_token else None
        }
        if has_token:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"{CRYPTO_PAY_BASE}/getMe",
                        headers={"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}) as resp:
                        me_data = await resp.json(content_type=None) if resp.content_type else {}
                        result["api_ok"] = me_data.get("ok", False)
                        if not me_data.get("ok"):
                            result["api_error"] = me_data.get("error", "unknown")
            except Exception as e:
                result["api_ok"] = False
                result["api_error"] = str(e)
        return _json_response(result)
    app.router.add_get("/api/cryptobot/status", cryptobot_status_handler)

    # CryptoBot create invoice
    async def cryptobot_create_invoice_handler(request):
        """
        Создание инвойса CryptoBot.

        ВАЖНО: пользователь НЕ задаёт цену и payload напрямую.
        Фронт может передавать:
        - context='purchase' + purchase (type, stars_amount, months, login) + user_id
        - context='deposit'  + amount (RUB) + user_id
        """
        if not CRYPTO_PAY_TOKEN:
            return _json_response(
                {
                    "error": "not_configured",
                    "message": "CRYPTO_PAY_TOKEN не задан. Добавьте в переменные окружения Railway/Render.",
                },
                status=503,
            )

        try:
            body = await request.json()
        except Exception:
            return _json_response({"error": "bad_request", "message": "Invalid JSON"}, status=400)

        context = (body.get("context") or "").strip() or "deposit"
        user_id = str(body.get("user_id") or body.get("userId") or "").strip() or "unknown"

        amount: float
        description: str
        payload_data: str
        use_usdt = False  # пока создаём инвойсы только в RUB, USDT-логику можно добавить отдельно

        # ----------- Покупка (звёзды / премиум) -----------
        if context == "purchase":
            purchase = body.get("purchase") or {}
            ptype = (purchase.get("type") or "").strip()

            if ptype == "stars":
                try:
                    stars_amount = int(purchase.get("stars_amount") or purchase.get("starsAmount") or 0)
                except (TypeError, ValueError):
                    stars_amount = 0
                login = (purchase.get("login") or "").strip().lstrip("@")
                if stars_amount <= 0 or not login:
                    return _json_response(
                        {"error": "bad_request", "message": "Неверные данные покупки звёзд"}, status=400
                    )
                amount = round(stars_amount * STAR_PRICE_RUB, 2)
                if amount < 1:
                    amount = 1.0
                description = f"Звёзды Telegram — {stars_amount} шт. для @{login}"
                payload_data = json.dumps(
                    {
                        "context": "purchase",
                        "type": "stars",
                        "user_id": user_id,
                        "login": login,
                        "stars_amount": stars_amount,
                        "amount_rub": amount,
                        "timestamp": time.time(),
                    },
                    ensure_ascii=False,
                )[:4096]
            elif ptype == "premium":
                try:
                    months = int(purchase.get("months") or 0)
                except (TypeError, ValueError):
                    months = 0
                if months not in PREMIUM_PRICES_RUB:
                    return _json_response(
                        {"error": "bad_request", "message": "Неверная длительность Premium"}, status=400
                    )
                amount = float(PREMIUM_PRICES_RUB[months])
                description = f"Telegram Premium — {months} мес."
                payload_data = json.dumps(
                    {
                        "context": "purchase",
                        "type": "premium",
                        "user_id": user_id,
                        "months": months,
                        "amount_rub": amount,
                        "timestamp": time.time(),
                    },
                    ensure_ascii=False,
                )[:4096]
            else:
                return _json_response(
                    {"error": "bad_request", "message": "Поддерживаются только покупки звёзд и Premium"}, status=400
                )

        # ----------- Пополнение баланса (депозит) -----------
        else:
            amount_rub = body.get("amount") or body.get("total_amount")
            try:
                amount = float(amount_rub) if amount_rub is not None else 0.0
            except (TypeError, ValueError):
                return _json_response(
                    {"error": "bad_request", "message": "amount должен быть числом (RUB)"}, status=400
                )
            if amount < 1:
                return _json_response({"error": "bad_request", "message": "Минимальная сумма 1 ₽"}, status=400)
            if amount > 1_000_000:
                return _json_response(
                    {"error": "bad_request", "message": "Максимальная сумма 1,000,000 ₽"}, status=400
                )
            description = f"Пополнение баланса JET Store на {amount:.0f} ₽"
            payload_data = json.dumps(
                {
                    "context": "deposit",
                    "user_id": user_id,
                    "amount_rub": amount,
                    "timestamp": time.time(),
                },
                ensure_ascii=False,
            )[:4096]

        # ----------- Общие поля инвойса -----------
        paid_btn_url = WEB_APP_URL or "https://jetstoreapp.ru"
        try:
            me = await bot.get_me()
            if me and getattr(me, "username", None):
                paid_btn_url = f"https://t.me/{me.username}/app"
        except Exception:
            pass

        payload_obj = {
            "currency_type": "fiat",
            "fiat": "RUB",
            "amount": f"{amount:.2f}",
            "description": description[:1024],
            "accepted_assets": "USDT,TON,BTC,ETH,TRX,USDC",
            "payload": payload_data,
            "paid_btn_name": "callback",
            "paid_btn_url": paid_btn_url,
        }
        headers = {
            "Content-Type": "application/json",
            "Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN,
        }
        logger.info(f"CryptoBot createInvoice: context={context}, amount={amount}")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{CRYPTO_PAY_BASE}/createInvoice", headers=headers, json=payload_obj) as resp:
                    resp_text = await resp.text()
                    logger.info(f"CryptoBot response status={resp.status}, body={resp_text[:300]}")
                    try:
                        data = json.loads(resp_text) if resp_text else {}
                    except json.JSONDecodeError:
                        return _json_response({
                            "error": "cryptobot_error",
                            "message": f"Неверный ответ API: {resp_text[:150]}"
                        }, status=502)
                    if not data.get("ok"):
                        err = data.get("error")
                        if isinstance(err, dict):
                            err_msg = err.get("name") or err.get("message") or str(err)
                        else:
                            err_msg = str(err) if err else "Unknown error"
                        logger.error(f"CryptoBot API error: {err_msg}, full={data}")
                        return _json_response({
                            "error": "cryptobot_error",
                            "message": err_msg,
                            "details": data.get("error")
                        }, status=502)
                    inv = data.get("result", {})
                    pay_url = (inv.get("mini_app_invoice_url") or inv.get("web_app_invoice_url")
                               or inv.get("bot_invoice_url") or inv.get("pay_url") or "")
                    if not pay_url and isinstance(inv, dict):
                        for k in ("mini_app_invoice_url", "web_app_invoice_url", "bot_invoice_url", "pay_url"):
                            if inv.get(k):
                                pay_url = inv[k]
                                break
                    invoice_id = inv.get("invoice_id")
                    logger.info(
                        "CryptoBot invoice created: invoice_id=%s, context=%s, amount=%s, pay_url_len=%s",
                        invoice_id,
                        context,
                        amount,
                        len(pay_url) if pay_url else 0,
                    )
                    # Сохраняем метаданные инвойса на стороне сервера,
                    # чтобы не доверять данным из клиента при последующей проверке оплаты.
                    try:
                        orders = request.app.get("cryptobot_orders")
                        if isinstance(orders, dict) and invoice_id:
                            orders[str(invoice_id)] = {
                                "context": context,
                                "user_id": user_id,
                                "amount_rub": float(amount),
                                "purchase": purchase if context == "purchase" else None,
                                "created_at": time.time(),
                                "delivered": False,
                            }
                    except Exception as meta_err:
                        logger.warning("Failed to store cryptobot order meta: %s", meta_err)
                    return _json_response({
                        "success": True, "invoice_id": invoice_id,
                        "payment_url": pay_url or None, "pay_url": pay_url or None, "hash": inv.get("hash"),
                    })
        except aiohttp.ClientError as e:
            logger.error(f"CryptoBot network error: {e}")
            return _json_response({"error": "network_error", "message": f"Ошибка связи с Crypto Pay: {e}"}, status=502)
        except Exception as e:
            logger.error(f"CryptoBot createInvoice error: {e}")
            return _json_response({"error": "internal_error", "message": str(e)}, status=500)

    async def cryptobot_check_invoice_handler(request):
        if not CRYPTO_PAY_TOKEN:
            return _json_response({"error": "not_configured"}, status=503)
        try:
            body = await request.json()
        except Exception:
            return _json_response({"error": "bad_request"}, status=400)
        invoice_id = body.get("invoice_id")
        if not invoice_id:
            return _json_response({"error": "bad_request", "message": "invoice_id required"}, status=400)
        headers = {"Content-Type": "application/json", "Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{CRYPTO_PAY_BASE}/getInvoices", headers=headers,
                    params={"invoice_ids": str(invoice_id), "status": "paid"}) as resp:
                    data = await resp.json(content_type=None) if resp.content_type else {}
                    if not data.get("ok"):
                        return _json_response({"paid": False})
                    items = data.get("result") or []
                    paid = any(str(inv.get("invoice_id")) == str(invoice_id) and inv.get("status") == "paid" for inv in items) if isinstance(items, list) else False
                    return _json_response({"paid": paid, "invoice_id": invoice_id})
        except Exception as e:
            logger.error(f"Crypto Pay getInvoices error: {e}")
            return _json_response({"paid": False, "error": str(e)}, status=500)

    app.router.add_post("/api/cryptobot/create-invoice", cryptobot_create_invoice_handler)
    app.router.add_route("OPTIONS", "/api/cryptobot/create-invoice", lambda r: Response(status=204, headers=_cors_headers()))
    app.router.add_post("/api/cryptobot/check-invoice", cryptobot_check_invoice_handler)
    app.router.add_route("OPTIONS", "/api/cryptobot/check-invoice", lambda r: Response(status=204, headers=_cors_headers()))
    
    # Рейтинг покупателей
    RATING_DATA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rating_data.json")
    
    def _read_rating_data():
        return _read_json_file(RATING_DATA_FILE)
    
    def _write_rating_data(data: dict):
        try:
            with open(RATING_DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"rating_data write error: {e}")
    
    async def rating_leaderboard_handler(request):
        """GET /api/rating/leaderboard?period=all|month|week|today — покупатели из PostgreSQL или users_data.json"""
        try:
            period = (request.query.get("period") or "all").lower()
            if period not in ("all", "month", "week", "today"):
                period = "all"
            
            entries = []
            import db as _db
            if _db.is_enabled():
                users_data = await _db.get_users_with_purchases()
                rating_prefs = await _db.rating_get_all()
            else:
                rating_prefs = _read_rating_data() or {}
                _script_dir = os.path.dirname(os.path.abspath(__file__))
                users_data = None
                for p in [
                    os.path.join(_script_dir, "users_data.json"),
                    os.path.join(os.path.dirname(_script_dir), "users_data.json"),
                    os.path.join(_script_dir, "..", "users_data.json"),
                ]:
                    if os.path.exists(p):
                        users_data = _read_json_file(p)
                        break
                if not users_data:
                    users_data = {}
            
            if not users_data or not isinstance(users_data, dict):
                return _json_response({"entries": []})
            
            now = datetime.now()
            cutoff_all = 0
            cutoff_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timestamp()
            cutoff_week = (now.timestamp() - 7 * 24 * 3600)
            cutoff_today = now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
            
            for uid, u in users_data.items():
                if not isinstance(u, dict):
                    continue
                purchases = u.get("purchases") or []
                if not purchases:
                    continue
                
                total_stars = 0
                orders_count = 0
                for p in purchases:
                    if not isinstance(p, dict):
                        continue
                    ts = None
                    try:
                        dt = p.get("date") or p.get("created_at") or p.get("timestamp")
                        if dt:
                            s = str(dt).replace("T", " ")[:19]
                            ts = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").timestamp()
                        elif isinstance(dt, (int, float)):
                            ts = float(dt) if dt > 1e9 else dt
                    except Exception:
                        pass
                    if period == "month" and (not ts or ts < cutoff_month):
                        continue
                    if period == "week" and (not ts or ts < cutoff_week):
                        continue
                    if period == "today" and (not ts or ts < cutoff_today):
                        continue
                    stars = p.get("stars_amount") or p.get("starsAmount") or p.get("amount") or 0
                    if isinstance(stars, (int, float)):
                        total_stars += int(stars)
                    orders_count += 1
                
                if orders_count <= 0:
                    continue
                if total_stars <= 0:
                    total_stars = orders_count * 100
                
                show = rating_prefs.get(str(uid), {}).get("show_in_rating", True)
                entries.append({
                    "userId": str(uid),
                    "username": u.get("username") or "",
                    "firstName": u.get("first_name") or u.get("firstName") or "",
                    "ordersCount": orders_count,
                    "score": total_stars,
                    "hidden": not show,
                })
            
            entries.sort(key=lambda x: x["score"], reverse=True)
            entries = entries[:15]
            
            for e in entries:
                if e["hidden"]:
                    e["username"] = ""
                    e["firstName"] = ""
            
            return _json_response({"entries": entries})
        except Exception as e:
            logger.error(f"rating leaderboard error: {e}")
            return _json_response({"entries": []})
    
    async def rating_anonymity_handler(request):
        """POST /api/rating/anonymity { show: bool, userId: str }"""
        try:
            body = await request.json() if request.can_read_body else {}
            show = body.get("show", True)
            uid = str(body.get("userId") or "").strip()
            if not uid:
                return _json_response({"error": "userId required"}, status=400)
            import db as _db
            if _db.is_enabled():
                await _db.rating_set(uid, bool(show))
            else:
                data = _read_rating_data() or {}
                if uid not in data:
                    data[uid] = {}
                data[uid]["show_in_rating"] = bool(show)
                _write_rating_data(data)
            return _json_response({"success": True, "show": show})
        except Exception as e:
            logger.error(f"rating anonymity error: {e}")
            return _json_response({"error": str(e)}, status=500)
    
    def _rating_cors(r):
        return Response(status=204, headers=_cors_headers())
    app.router.add_get("/api/rating/leaderboard", rating_leaderboard_handler)
    app.router.add_route("OPTIONS", "/api/rating/leaderboard", _rating_cors)
    app.router.add_post("/api/rating/anonymity", rating_anonymity_handler)
    app.router.add_route("OPTIONS", "/api/rating/anonymity", _rating_cors)
    
    # API записи покупки: рейтинг + рефералы + users_data.json
    USERS_DATA_PATHS = [
        os.path.join(_script_dir, "users_data.json"),
        os.path.join(os.path.dirname(_script_dir), "users_data.json"),
    ]
    
    def _get_users_data_path():
        for p in USERS_DATA_PATHS:
            if os.path.exists(p):
                return p
        return USERS_DATA_PATHS[0]
    
    async def purchases_record_handler(request):
        """
        POST /api/purchases/record
        Сохраняет покупку в users_data.json (для рейтинга), начисляет рефералам.
        JSON: { user_id, amount_rub, stars_amount?, type?, productName?, rating_only?, referral_only? }
        rating_only=True — только рейтинг (при отправке денег), referral_only=True — только рефералы (при успешной оплате)
        """
        try:
            body = await request.json() if request.can_read_body else {}
            user_id = str(body.get("user_id") or "").strip()
            amount_rub = float(body.get("amount_rub") or body.get("amount") or 0)
            stars_amount = int(body.get("stars_amount") or 0)
            purchase_type = (body.get("type") or "stars").strip()
            product_name = body.get("productName") or body.get("product_name") or ""
            username = body.get("username") or ""
            first_name = body.get("first_name") or ""
            rating_only = bool(body.get("rating_only"))
            referral_only = bool(body.get("referral_only"))
            if not user_id:
                return _json_response({"error": "user_id required"}, status=400)
            if amount_rub <= 0:
                return _json_response({"error": "amount_rub must be > 0"}, status=400)
            
            if not referral_only:
                import db as _db
                if _db.is_enabled():
                    await _db.user_upsert(user_id, username, first_name)
                    await _db.purchase_add(user_id, amount_rub, stars_amount, purchase_type, product_name)
                else:
                    path = _get_users_data_path()
                    users_data = _read_json_file(path) or {}
                    if user_id not in users_data:
                        users_data[user_id] = {
                            "id": int(user_id) if user_id.isdigit() else user_id,
                            "username": username,
                            "first_name": first_name,
                            "purchases": [],
                        }
                    u = users_data[user_id]
                    if "purchases" not in u:
                        u["purchases"] = []
                    u["purchases"].append({
                        "stars_amount": stars_amount or int(amount_rub / 0.65),
                        "amount": amount_rub,
                        "type": purchase_type,
                        "productName": product_name,
                        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    })
                    try:
                        with open(path, "w", encoding="utf-8") as f:
                            json.dump(users_data, f, ensure_ascii=False, indent=2)
                    except Exception as e:
                        logger.warning("purchases_record write users_data: %s", e)
            
            if not rating_only:
                await _load_referrals()
                user_ref = await _get_or_create_ref_user(user_id)
                user_ref["username"] = user_ref.get("username") or username
                user_ref["first_name"] = user_ref.get("first_name") or first_name
                for pid, percent in (
                    (user_ref.get("parent1"), 0.15),
                    (user_ref.get("parent2"), 0.20),
                    (user_ref.get("parent3"), 0.25),
                ):
                    if not pid:
                        continue
                    pref = await _get_or_create_ref_user(pid)
                    pref["volume_rub"] = float(pref.get("volume_rub") or 0) + amount_rub
                    pref["earned_rub"] = float(pref.get("earned_rub") or 0) + amount_rub * percent
                await _save_referrals()
            
            return _json_response({"success": True})
        except Exception as e:
            logger.error("purchases_record error: %s", e)
            return _json_response({"error": str(e)}, status=500)
    
    app.router.add_post("/api/purchases/record", purchases_record_handler)
    app.router.add_route("OPTIONS", "/api/purchases/record", lambda r: Response(status=204, headers=_cors_headers()))
    
    # Раздача статических файлов мини-аппа (index.html, script.js, style.css, assets/* и т.д.)
    # В продакшене статику лучше обслуживать отдельным хостингом (например, GitHub Pages / Nginx),
    # чтобы API-сервер НЕ раздавал исходники (bot.py, конфиги и т.п.).
    #
    # Для локальной разработки можно включить раздачу статики, установив SERVE_STATIC=1.
    if os.getenv("SERVE_STATIC", "").strip() == "1":
        # Отдаём только каталог html/, а не весь репозиторий
        static_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "html")
        if os.path.isdir(static_root):
            app.router.add_static("/", static_root, show_index=False)
    return app

# ============ ЗАПУСК БОТА ============

async def main():
    """Основная функция запуска бота"""
    print("=" * 50)
    print("🤖 Jet Store Bot запускается...")
    print(f"🔧 Токен: {BOT_TOKEN[:10]}...")
    print(f"👑 Админы (из кода): {ADMIN_IDS}")
    print(f"🌐 Web App: {WEB_APP_URL}")
    print("=" * 50)
    print("📝 Основные команды:")
    print("   • /start - Главное меню (выбор языка)")
    print("   • /admin - Админ панель")
    print("   • /id - Узнать свой ID и статус")
    print("   • /users - Статистика пользователей (админы)")
    print("=" * 50)
    print("⚠️  Чтобы стать админом, добавьте свой ID в код:")
    print(f"    ADMIN_IDS = [6928639672]  ← замени 6928639672 на свой ID")
    print("=" * 50)
    
    # Подключаем PostgreSQL (если задан DATABASE_URL)
    try:
        import db
        await db.init_pool()
    except Exception as e:
        logger.warning("PostgreSQL: %s", e)
    
    # Подключаем userbot (Telethon)
    try:
        logger.info(
            f"Telethon ENV: api_id={'set' if TELEGRAM_API_ID > 0 else 'missing'}; "
            f"api_hash={'set' if bool(TELEGRAM_API_HASH) else 'missing'}; "
            f"string_session={'set' if bool(TELEGRAM_STRING_SESSION) else 'missing'}"
        )
        logger.info(f"Telethon lengths: api_hash_len={len(TELEGRAM_API_HASH)}; session_len={len(TELEGRAM_STRING_SESSION)}")
        await init_telethon()
    except Exception as e:
        logger.error(f"Ошибка инициализации Telethon: {e}")

    # Настраиваем HTTP сервер для API
    http_app = setup_http_server()
    runner = web.AppRunner(http_app)
    await runner.setup()
    port = int(os.getenv("PORT") or "3000")
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"🌐 HTTP API сервер запущен на порту {port}")
    print("   Эндпоинт: /api/telegram/user, /api/cryptobot/create-invoice")
    print("   Для Railway: git push → получите публичный URL бота")
    print("=" * 50)
    
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Ошибка запуска бота: {e}")
        print(f"❌ Ошибка запуска бота: {e}")
    finally:
        try:
            import db
            await db.close_pool()
        except Exception:
            pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.exception("Критическая ошибка при запуске: %s", e)
        raise