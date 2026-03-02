import asyncio
import logging
import json
import os
import re
import base64
import time
import uuid
import hmac
import hashlib
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
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from telethon import TelegramClient
from telethon.errors import UsernameInvalidError, UsernameNotOccupiedError
from telethon.sessions import StringSession

try:
    # Библиотека для работы с Fragment (Stars, Premium, TON)
    from FragmentAPI import AsyncFragmentAPI  # type: ignore
except Exception:  # pragma: no cover - опциональная зависимость
    AsyncFragmentAPI = None  # type: ignore

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
# Токен ТОЛЬКО из переменной окружения, без fallback (чтобы не утек при деплое)
BOT_TOKEN = (os.environ.get("BOT_TOKEN") or "").strip()
if not BOT_TOKEN:
    raise RuntimeError(
        "BOT_TOKEN не задан. Установите переменную окружения BOT_TOKEN "
        "(например, в Railway/Render) перед запуском бота."
    )
# Админы ТОЛЬКО из env (без дефолта, чтобы не дать доступ по умолчанию)
_admin_ids_str = (os.environ.get("ADMIN_IDS") or "").strip()
ADMIN_IDS = [int(x) for x in _admin_ids_str.split(",") if x.strip()] if _admin_ids_str else []
WEB_APP_URL = os.getenv("WEB_APP_URL", "https://jetstoreapp.ru")
ADM_WEB_APP_URL = os.getenv("ADM_WEB_APP_URL", "https://jetstoreapp.ru/html/admin.html")

# Группы/чаты для уведомлений
# SELL_STARS_NOTIFY_CHAT_ID: уведомления о продаже звёзд
# TON_NOTIFY_CHAT_ID: заявки на покупку TON
# IDEAS_CHAT_ID: идеи/предложения из мини‑приложения
SELL_STARS_NOTIFY_CHAT_ID = int(os.getenv("SELL_STARS_NOTIFY_CHAT_ID", "0") or "0")
TON_NOTIFY_CHAT_ID = int(os.getenv("TON_NOTIFY_CHAT_ID", "0") or "0")
IDEAS_CHAT_ID = int(os.getenv("IDEAS_CHAT_ID", "0") or "0")

# Курсы звёзд и Steam: из env по умолчанию, из админки (файл + API) — переопределяют
STAR_BUY_RATE_RUB_DEFAULT = float(os.getenv("STAR_BUY_RATE_RUB", "0.65") or "0.65")
STAR_PRICE_RUB_DEFAULT = float(os.getenv("STAR_PRICE_RUB", "1.37") or "1.37")
STEAM_RATE_RUB_DEFAULT = float(os.getenv("STEAM_RATE_RUB", "1.06") or "1.06")

_star_price_rub_override: Optional[float] = None
_star_buy_rate_rub_override: Optional[float] = None
_steam_rate_rub_override: Optional[float] = None

def _get_star_price_rub() -> float:
    if _star_price_rub_override is not None and _star_price_rub_override > 0:
        return _star_price_rub_override
    return STAR_PRICE_RUB_DEFAULT

def _get_star_buy_rate_rub() -> float:
    if _star_buy_rate_rub_override is not None and _star_buy_rate_rub_override > 0:
        return _star_buy_rate_rub_override
    return STAR_BUY_RATE_RUB_DEFAULT

def _get_steam_rate_rub() -> float:
    if _steam_rate_rub_override is not None and _steam_rate_rub_override > 0:
        return _steam_rate_rub_override
    return STEAM_RATE_RUB_DEFAULT

# Для обратной совместимости в коде
# Алиасы для чтения через геттеры (используйте _get_star_price_rub / _get_star_buy_rate_rub в коде)
STAR_PRICE_RUB = STAR_PRICE_RUB_DEFAULT  # fallback; в расчётах используется _get_star_price_rub()
STAR_BUY_RATE_RUB = STAR_BUY_RATE_RUB_DEFAULT
STEAM_RATE_RUB = STEAM_RATE_RUB_DEFAULT

# Цены на Premium в рублях (по умолчанию совпадают с мини‑аппом)
PREMIUM_PRICES_RUB = {
    3: float(os.getenv("PREMIUM_PRICE_3M", "983") or "983"),
    6: float(os.getenv("PREMIUM_PRICE_6M", "1311") or "1311"),
    12: float(os.getenv("PREMIUM_PRICE_12M", "2377") or "2377"),
}

# Комиссия Platega (из админки / env): СБП % и Карты %
_platega_sbp_commission_override: Optional[float] = None
_platega_cards_commission_override: Optional[float] = None
def _get_platega_sbp_commission() -> float:
    if _platega_sbp_commission_override is not None and _platega_sbp_commission_override >= 0:
        return _platega_sbp_commission_override
    return float(os.getenv("PLATEGA_SBP_COMMISSION_PERCENT", "10") or "10")
def _get_platega_cards_commission() -> float:
    if _platega_cards_commission_override is not None and _platega_cards_commission_override >= 0:
        return _platega_cards_commission_override
    return float(os.getenv("PLATEGA_CARDS_COMMISSION_PERCENT", "14") or "14")

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
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REFERRALS_FILE = os.path.join(_SCRIPT_DIR, "referrals_data.json")

# Ключ для шифрования ID в реферальной ссылке (XOR + base62 = короткая ссылка)
REFERRAL_ENC_KEY = (os.getenv("REFERRAL_ENC_KEY", "jet_ref_2024_secret") or "").encode()[:32].ljust(32, b"0")
_B62_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

# Ограничение по отправке идей: один запрос раз в 12 часов на пользователя
IDEAS_LIMITS_FILE = os.path.join(_SCRIPT_DIR, "ideas_limits.json")

# ============ ЗАЩИТА ОТ ХАКЕРОВ: лимиты и валидация ============
VALIDATION_LIMITS = {
    "stars_min": 50,
    "stars_max": 50000,
    "steam_min": 50,
    "steam_max": 500000,
    "amount_rub_max": 2_000_000,
    "login_max_len": 32,
    "order_id_max_len": 64,
    "username_max_len": 32,
    "premium_months": (3, 6, 12),
}


def _validate_user_id(user_id: str) -> bool:
    """Проверка user_id: только цифры, разумная длина."""
    if not user_id or not isinstance(user_id, str):
        return False
    s = str(user_id).strip()
    if len(s) > 20:
        return False
    return s.isdigit()


def _validate_telegram_init_data(init_data: str, max_age_sec: int = 86400) -> Optional[str]:
    """
    Проверка подписи Telegram WebApp initData. Возвращает user_id при успехе, иначе None.
    Защита от подделки: только запросы с валидной подписью от Telegram принимаются.
    """
    if not init_data or not BOT_TOKEN:
        return None
    init_data = str(init_data).strip()
    if not init_data:
        return None
    try:
        from urllib.parse import parse_qsl
        params = dict(parse_qsl(init_data, keep_blank_values=True))
    except Exception:
        return None
    hash_received = (params.get("hash") or "").strip()
    if not hash_received:
        return None
    data_check_parts = sorted((k, v) for k, v in params.items() if k != "hash")
    data_check_string = "\n".join(f"{k}={v}" for k, v in data_check_parts)
    secret_key = hmac.new(
        b"WebAppData",
        BOT_TOKEN.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    hash_computed = hmac.new(
        secret_key,
        data_check_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(hash_computed, hash_received):
        return None
    auth_date_str = params.get("auth_date") or ""
    try:
        auth_date = int(auth_date_str)
        if time.time() - auth_date > max_age_sec:
            return None
    except (TypeError, ValueError):
        return None
    user_json = params.get("user") or "{}"
    try:
        user_obj = json.loads(user_json)
        user_id = str(user_obj.get("id") or "").strip()
        return user_id if _validate_user_id(user_id) else None
    except (json.JSONDecodeError, TypeError):
        return None


def _validate_login(login: str, field_name: str = "login") -> tuple[str, Optional[str]]:
    """Валидация логина/username: только буквы, цифры, подчёркивание. Возвращает (очищенный, ошибка)."""
    if not login or not isinstance(login, str):
        return ("", f"{field_name} обязателен")
    s = login.strip().lstrip("@")
    if len(s) > VALIDATION_LIMITS["login_max_len"]:
        return (s[:VALIDATION_LIMITS["login_max_len"]], None)
    if not re.match(r"^[a-zA-Z0-9_]+$", s):
        return ("", f"{field_name}: только латиница, цифры и _")
    return (s, None)


def _validate_order_id(order_id: str) -> tuple[str, Optional[str]]:
    """Валидация order_id: буквы, цифры, дефис, подчёркивание."""
    if not order_id or not isinstance(order_id, str):
        return ("", "order_id обязателен")
    s = str(order_id).strip()
    if len(s) > VALIDATION_LIMITS["order_id_max_len"]:
        return ("", "order_id слишком длинный")
    if not re.match(r"^[a-zA-Z0-9_#-]+$", s):
        return ("", "order_id: недопустимые символы")
    return (s, None)


def _validate_stars_amount(amount: int) -> Optional[str]:
    """Проверка количества звёзд."""
    if not isinstance(amount, int):
        return "Некорректное количество звёзд"
    if amount < VALIDATION_LIMITS["stars_min"]:
        return f"Минимум {VALIDATION_LIMITS['stars_min']} звёзд"
    if amount > VALIDATION_LIMITS["stars_max"]:
        return f"Максимум {VALIDATION_LIMITS['stars_max']} звёзд за одну покупку"
    return None


def _validate_steam_amount(amount: float) -> Optional[str]:
    """Проверка суммы Steam."""
    try:
        a = float(amount)
    except (TypeError, ValueError):
        return "Некорректная сумма"
    if a < VALIDATION_LIMITS["steam_min"]:
        return f"Минимум {VALIDATION_LIMITS['steam_min']} ₽ для Steam"
    if a > VALIDATION_LIMITS["steam_max"]:
        return f"Максимум {VALIDATION_LIMITS['steam_max']} ₽ для Steam"
    return None


def _validate_amount_rub(amount: float) -> Optional[str]:
    """Проверка суммы в рублях."""
    try:
        a = float(amount)
    except (TypeError, ValueError):
        return "Некорректная сумма"
    if a <= 0:
        return "Сумма должна быть > 0"
    if a > VALIDATION_LIMITS["amount_rub_max"]:
        return f"Сумма превышает лимит"
    if a != a:  # NaN
        return "Некорректная сумма"
    return None


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


# Глобальный помощник: поиск заказа по нашему кастомному order_id (#ABC123)
_CRYPTOBOT_ORDERS_FILE_GLOBAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cryptobot_orders.json")
_PLATEGA_ORDERS_FILE_GLOBAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "platega_orders.json")
_FREEKASSA_ORDERS_FILE_GLOBAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "freekassa_orders.json")


async def _find_order_by_custom_id(order_id: str) -> Optional[tuple]:
    """
    Ищет заказ по кастомному order_id (#ABC123) в:
      - cryptobot_orders.json
      - platega_orders.json
      - freekassa_orders.json
    Возвращает (source, order_key, meta):
      source = 'cryptobot' | 'platega' | 'freekassa'
      order_key = invoice_id / transaction_id / внутренний ID FreeKassa.
    """
    oid = (order_id or "").strip().upper()
    if not oid:
        return None
    if not oid.startswith("#"):
        oid = "#" + oid
    try:
        # CryptoBot: ищем в purchase.order_id
        data = _read_json_file(_CRYPTOBOT_ORDERS_FILE_GLOBAL) or {}
        if isinstance(data, dict):
            for inv_id, meta in data.items():
                if not isinstance(meta, dict):
                    continue
                purchase_meta = meta.get("purchase") or {}
                if str(purchase_meta.get("order_id") or "").upper() == oid:
                    return ("cryptobot", str(inv_id), meta)

        # Platega: тоже ищем по purchase.order_id
        data = _read_json_file(_PLATEGA_ORDERS_FILE_GLOBAL) or {}
        if isinstance(data, dict):
            for tx_id, meta in data.items():
                if not isinstance(meta, dict):
                    continue
                purchase_meta = meta.get("purchase") or {}
                if str(purchase_meta.get("order_id") or "").upper() == oid:
                    return ("platega", str(tx_id), meta)

        # FreeKassa: order_id у нас хранится в meta.original_order_id (с #),
        # ключом в файле является очищенный payment_id без #.
        data = _read_json_file(_FREEKASSA_ORDERS_FILE_GLOBAL) or {}
        if isinstance(data, dict):
            for fk_key, meta in data.items():
                if not isinstance(meta, dict):
                    continue
                # Пытаемся сопоставить как с original_order_id, так и с purchase.order_id (на будущее)
                orig = str(meta.get("original_order_id") or "").upper()
                purchase_meta = meta.get("purchase") or {}
                poid = str(purchase_meta.get("order_id") or "").upper()
                if orig == oid or poid == oid:
                    return ("freekassa", str(fk_key), meta)
    except Exception as e:
        logger.warning(f"_find_order_by_custom_id error: {e}")
    return None


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


async def _apply_referral_earnings_for_purchase(
    user_id: str | int,
    amount_rub: float,
    username: str = "",
    first_name: str = "",
) -> None:
    """
    Начисляет реферальные проценты за покупку пользователя по цепочке parent1/parent2/parent3.
    
    Новая логика: прогрессивный процент в зависимости от суммарного объёма покупок рефералов (в звёздах):
    - до 50 000 звёзд → 3% от суммы в рублях
    - от 50 000 до 100 000 звёзд → 5%
    - от 100 000 до 200 000 звёзд → 7%
    
    Всегда работает через общую систему REFERRALS + _save_referrals(),
    которая сама решает, писать в PostgreSQL или JSON (fallback).
    Так мы исключаем расхождения между разными путями начисления.
    """
    try:
        amount = float(amount_rub or 0)
    except Exception:
        logger.warning("apply_referral_earnings: неправильная сумма amount_rub=%r", amount_rub)
        return
    if amount <= 0:
        logger.info("apply_referral_earnings: amount <= 0, начисление пропущено (amount=%s)", amount)
        return

    uid = str(user_id).strip()
    if not uid:
        logger.warning("apply_referral_earnings: пустой user_id, начисление пропущено")
        return

    # Загружаем существующие данные (из БД или JSON) и обновляем по цепочке
    await _load_referrals()
    user_ref = await _get_or_create_ref_user(uid)
    if username and not user_ref.get("username"):
        user_ref["username"] = username
    if first_name and not user_ref.get("first_name"):
        user_ref["first_name"] = first_name

    # Определяем процент по объёму покупок parent1 (в звёздах, но храним объём в рублях)
    # Для простоты: 1 звезда ≈ 2.8 ₽ (средний курс)
    # Пороги: 50k звёзд = 140k ₽, 100k звёзд = 280k ₽, 200k звёзд = 560k ₽
    def _calc_percent_by_volume_rub(volume_rub: float) -> float:
        """Расчёт процента по объёму в рублях (примерная конвертация из звёзд)."""
        # Примерный курс: 1 звезда ≈ 2.8 ₽
        volume_stars = volume_rub / 2.8
        if volume_stars < 50000:
            return 0.03  # 3%
        elif volume_stars < 100000:
            return 0.05  # 5%
        elif volume_stars < 200000:
            return 0.07  # 7%
        else:
            return 0.07  # максимум 7%

    # Начисляем только на parent1, parent2, parent3 — одинаковый процент для всех.
    # Награда сразу зачисляется на RUB-баланс родителя.
    parents = [user_ref.get("parent1"), user_ref.get("parent2"), user_ref.get("parent3")]
    any_parent = False
    for pid in parents:
        if not pid:
            continue
        any_parent = True
        pref = await _get_or_create_ref_user(pid)
        # Обновляем объём parent'а
        pref["volume_rub"] = float(pref.get("volume_rub") or 0.0) + amount
        # Определяем процент по **новому** объёму parent'а
        percent = _calc_percent_by_volume_rub(pref["volume_rub"])
        # Награда за эту покупку
        bonus = amount * percent
        if bonus <= 0:
            continue
        pref["earned_rub"] = float(pref.get("earned_rub") or 0.0) + bonus
        # Мгновенно зачисляем на баланс родителя (если доступна БД)
        try:
            import db as _db_ref
            if _db_ref.is_enabled():
                await _db_ref.balance_add_rub(str(pid), bonus)
        except Exception as e:
            logger.warning("apply_referral_earnings: failed to credit balance for parent %s: %s", pid, e)

    if not any_parent:
        logger.info("apply_referral_earnings: у пользователя %s нет parent1/2/3, начислять некому", uid)
        return

    await _save_referrals()


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
            # Используем premium‑эмодзи 💡 (tg://emoji?id=5422439311196834318) для блока "О нас"
            'about_text_ru': '''<b><tg-emoji emoji-id="5422439311196834318">💡</tg-emoji> О сервисе Jet Store</b>

Мы предоставляем:
• ⭐️ <b>Покупку звёзд</b>
• 🎡 <b>Участие в рулетке</b>
• 🗂️ <b>Каталог цифровых товаров</b>''',
            'about_text_en': '''<b><tg-emoji emoji-id="5422439311196834318">💡</tg-emoji> About Jet Store Service</b>

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
    # выбор языка больше не используется
    choosing_language = State()

class AdminStates(StatesGroup):
    waiting_welcome_text = State()
    waiting_welcome_photo = State()
    waiting_about_text = State()
    waiting_notification_text = State()
    waiting_notification_photo = State()
    waiting_user_balance = State()
    waiting_order_id = State()


class SellStarsStates(StatesGroup):
    waiting_amount = State()

# ============ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ============

def is_admin(user_id: int) -> bool:
    """Проверка прав администратора - ТОЛЬКО из кода"""
    return db.is_admin(user_id)

def get_main_menu(language: str = 'ru'):
    """Главное меню — синие кнопки (primary) для основных действий, красная (danger) для помощи"""
    keyboard = [
        [
            InlineKeyboardButton(text="🚀 Открыть приложение", web_app=WebAppInfo(url=WEB_APP_URL), style="primary"),
        ],
        [
            InlineKeyboardButton(text="📰 Подписаться на канал", url="https://t.me/JetStoreApp", style="primary"),
        ],
        [
            InlineKeyboardButton(text="? Помощь", callback_data="help_info", style="danger"),
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_about_menu(language: str = 'ru'):
    """Меню 'О нас' (только русский текст)"""
    keyboard = [
        [
            InlineKeyboardButton(text="📞 Помощь", url="https://t.me/L3ZTADM", style="primary"),
            InlineKeyboardButton(text="📢 Наш канал", url="https://t.me/JetStoreApp", style="primary")
        ],
        [
            InlineKeyboardButton(text="📄 Договор оферты",
                                url="https://telegra.ph/Dogovor-Oferty-02-11-4", style="primary"),
        ],
        [
            InlineKeyboardButton(text="📜 Пользовательское соглашение",
                                url="https://telegra.ph/Polzovatelskoe-soglashenie-02-11-33", style="primary"),
        ],
        [
            InlineKeyboardButton(text="🔒 Политика конфиденциальности",
                                url="https://telegra.ph/Politika-konfidecialnosti-02-11", style="primary"),
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
            InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_notification"),
            InlineKeyboardButton(text="🖼️ Изменить фото", callback_data="admin_photo")
        ],
        [
            InlineKeyboardButton(text="🔍 Поиск заказа", callback_data="admin_search_order"),
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

    # Синхронизируем пользователя с PostgreSQL (для админки и баланса)
    try:
        import db as _db_pg
        if _db_pg.is_enabled():
            await _db_pg.user_upsert(str(user.id), user.username or "", user.first_name or "")
    except Exception as e:
        logger.warning("cmd_start: failed to upsert user in PostgreSQL: %s", e)

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

    username_display = user.username and f"@{user.username}" or user.first_name or "друг"
    language = db.get_user_language(user.id)

    text = (
        "Добро пожаловать в <b>Jet Store</b>! 🚀\n"
        f"Привет, <b>{username_display}</b>!\n\n"
        "⚡ Покупай и управляй цифровыми товарами прямо в Telegram.\n\n"
        "Выбери действие:"
    )

    # Используем стандартное меню с кнопкой отзывов
    keyboard = get_main_menu(language)

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
    payout_rub = stars * _get_star_buy_rate_rub()

    await state.clear()

    prices = [LabeledPrice(label="Продажа звёзд", amount=stars)]

    await message.answer_invoice(
        title="Продажа Telegram Stars",
        description=(
            f"Вы продаёте {stars} ⭐ Telegram Stars.\n\n"
            f"Примерная выплата: <b>{payout_rub:.2f} ₽</b> по курсу {_get_star_buy_rate_rub()} ₽ за 1 ⭐."
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
        payout_rub = stars * _get_star_buy_rate_rub()
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

        payout_rub = stars * _get_star_buy_rate_rub()
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


@dp.callback_query(F.data == "admin_search_order")
async def admin_search_order(callback_query: types.CallbackQuery, state: FSMContext):
    """Запросить у админа ID заказа для поиска"""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("⛔ Нет прав администратора", show_alert=True)
        return
    
    await callback_query.message.answer(
        "🔍 <b>Поиск заказа</b>\n\n"
        "Отправьте ID заказа в формате <code>#ABC123</code>.\n"
        "ID можно скопировать из истории покупок в мини‑приложении.",
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.waiting_order_id)
    await callback_query.answer()


@dp.message(AdminStates.waiting_order_id)
async def process_admin_order_search(message: types.Message, state: FSMContext):
    """Обработать введённый ID заказа и показать информацию по нему"""
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Нет прав администратора")
        await state.clear()
        return
    
    raw_id = (message.text or "").strip().upper()
    if not raw_id:
        await message.answer("⚠️ Отправьте ID заказа в формате <code>#ABC123</code>.", parse_mode="HTML")
        return
    
    if not raw_id.startswith("#"):
        raw_id = "#" + raw_id
    
    found = await _find_order_by_custom_id(raw_id)
    if not found:
        await message.answer(f"❌ Заказ с ID <code>{raw_id}</code> не найден.", parse_mode="HTML")
        await state.clear()
        return
    
    source, order_key, meta = found
    meta = meta or {}
    purchase = meta.get("purchase") or {}
    amount_rub = meta.get("amount_rub") or 0
    user_id = meta.get("user_id") or "unknown"
    context = meta.get("context") or "purchase"
    delivered = bool(meta.get("delivered"))
    
    ptype = (purchase.get("type") or "").strip().lower()
    username = purchase.get("username") or ""
    first_name = purchase.get("first_name") or ""
    stars_amount = int(purchase.get("stars_amount") or 0)
    months = int(purchase.get("months") or 0)
    login = purchase.get("login") or ""
    created_ts = meta.get("created_at")
    created_str = ""
    if isinstance(created_ts, (int, float)):
        try:
            created_str = datetime.fromtimestamp(created_ts).strftime("%d.%m.%Y %H:%M:%S")
        except Exception:
            created_str = ""
    
    if ptype == "stars":
        product_desc = f"Звёзды Telegram — {stars_amount} шт."
    elif ptype == "premium":
        product_desc = f"Telegram Premium — {months} мес."
    elif ptype == "steam":
        product_desc = f"Пополнение Steam для аккаунта: <code>{login or '—'}</code>"
    else:
        product_desc = purchase.get("productName") or purchase.get("product_name") or "Неизвестный товар"
    
    status_lines = []
    if delivered:
        status_lines.append("✅ <b>Товар выдан</b>")
    else:
        status_lines.append("⏳ <b>Оплата подтверждена, выдача ещё не завершена</b>")
    status_lines.append(f"💵 Сумма: <b>{float(amount_rub or 0):.2f} ₽</b>")
    
    if source == "cryptobot":
        id_label = "Invoice ID (CryptoBot):"
    elif source == "platega":
        id_label = "Transaction ID (Platega):"
    elif source == "freekassa":
        id_label = "Внутренний ID заказа FreeKassa:"
    else:
        id_label = "Внутренний ID заказа:"
    text_lines = [
        f"🔎 <b>Информация по заказу {raw_id}</b>",
        "",
        f"🧾 <b>{id_label}</b> <code>{order_key}</code>",
        f"📦 <b>Тип:</b> {ptype or '—'}",
        f"📚 <b>Товар:</b> {product_desc}",
        "",
        f"👤 <b>Пользователь ID:</b> <code>{user_id}</code>",
        f"👤 <b>Имя:</b> {first_name or '—'}",
        f"🔗 <b>Username:</b> @{username}" if username else "🔗 <b>Username:</b> —",
    ]
    if created_str:
        text_lines.append(f"🕒 <b>Создан:</b> {created_str}")
    text_lines.append("")
    text_lines.extend(status_lines)
    
    await message.answer("\n".join(text_lines), parse_mode="HTML")
    await state.clear()

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
    # Всегда используем русскоязычный текст "О сервисе"
    about_text = db.get_content('about_text_ru', 'Информация о сервисе...')
    await callback_query.message.answer(
        text=about_text,
        reply_markup=get_about_menu('ru'),
        parse_mode="HTML"
    )
    await callback_query.answer()

# ============ КНОПКА "ПОМОЩЬ" ============

@dp.callback_query(F.data == "help_info")
async def show_help(callback_query: types.CallbackQuery):
    """Раздел 'Помощь и контакты'"""
    help_text = (
        "💡 <b>Помощь и контакты</b>\n\n"
        "Поддержка: <a href=\"https://t.me/JetStoreHelper\">@JetStoreHelper</a>\n\n"
        "📄 Договор оферты: "
        "<a href=\"https://telegra.ph/Dogovor-Oferty-02-11-4\">читать</a>\n"
        "🔒 Политика конфиденциальности: "
        "<a href=\"https://telegra.ph/Politika-konfidecialnosti-02-11\">читать</a>\n"
        "📜 Пользовательское соглашение: "
        "<a href=\"https://telegra.ph/Polzovatelskoe-soglashenie-02-11-33\">читать</a>"
    )
    await callback_query.message.answer(help_text, parse_mode="HTML", disable_web_page_preview=True)
    await callback_query.answer()

# ============ ПРОФИЛЬ ============


# ============ БОНУСЫ ============

# ============ НАЗАД В ГЛАВНОЕ МЕНЮ ============

@dp.callback_query(F.data == "back_to_main")
async def back_to_main(callback_query: types.CallbackQuery):
    """Возврат в главное меню"""
    user_id = callback_query.from_user.id
    language = db.get_user_language(user_id)
    
    # Получаем текст приветствия
    if language == 'en':
        welcome_text = db.get_content('welcome_text_en', '👋 <b>Welcome to Jet Store!</b>\n\nChoose action:')
    else:
        welcome_text = db.get_content('welcome_text_ru', '👋 <b>Добро пожаловать в Jet Store!</b>\n\nВыберите действие:')
    
    keyboard = get_main_menu(language)
    
    # Редактируем сообщение вместо отправки нового
    try:
        await callback_query.message.edit_text(
            text=welcome_text,
            reply_markup=keyboard,
            parse_mode="HTML"
        )
    except Exception as e:
        # Если редактирование не удалось (например, сообщение с фото), отправляем новое
        logger.warning(f"Не удалось отредактировать сообщение: {e}")
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

        # 2) Fallback через Fragment (searchStarsRecipient) — тоже ищет «из всего Telegram»
        # Это даёт поведение "как во Fragment" даже без Telethon.
        frag_enabled = bool(request.app.get("fragment_site_enabled"))
        frag_cookie = str(request.app.get("fragment_site_cookies") or "").strip()
        frag_hash = str(request.app.get("fragment_site_hash") or "").strip()
        if frag_enabled and frag_cookie and frag_hash:
            try:
                referer = f"https://fragment.com/stars/buy?recipient={clean_username}&quantity=50"
                payload = {"query": clean_username, "quantity": "", "method": "searchStarsRecipient"}
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        "https://fragment.com/api",
                        params={"hash": frag_hash},
                        json=payload,
                        headers={
                            "content-type": "application/json",
                            "cookie": frag_cookie,
                            "referer": referer,
                            "origin": "https://fragment.com",
                            "accept": "application/json, text/plain, */*",
                            "user-agent": "Mozilla/5.0",
                        },
                        timeout=aiohttp.ClientTimeout(total=20),
                    ) as resp:
                        frag_data = await resp.json(content_type=None) if resp.content_type else {}
                found = (frag_data or {}).get("found")
                if isinstance(found, dict) and found.get("recipient"):
                    name = (found.get("name") or found.get("title") or found.get("display_name") or "").strip() or clean_username
                    avatar = (
                        found.get("photo")
                        or found.get("photo_url")
                        or found.get("avatar")
                        or found.get("avatar_url")
                        or found.get("image")
                        or found.get("image_url")
                    )
                    # иногда URL может лежать глубже — попробуем вытащить любую ссылку
                    if not avatar:
                        avatar = _extract_any_url(found) if "_extract_any_url" in globals() else None
                    if isinstance(avatar, str) and avatar.startswith("/"):
                        avatar = "https://fragment.com" + avatar
                    result = {
                        "username": clean_username,
                        "firstName": name,
                        "lastName": "",
                        "avatar": avatar or None,
                        "source": "fragment",
                    }
                    return Response(
                        text=json.dumps(result, ensure_ascii=False),
                        content_type="application/json",
                        charset="utf-8",
                        headers={
                            "Access-Control-Allow-Origin": "*",
                            "Access-Control-Allow-Methods": "GET, OPTIONS",
                            "Access-Control-Allow-Headers": "*",
                        },
                    )
            except Exception as fe:
                logger.warning("Fragment searchStarsRecipient lookup failed for %s: %s", clean_username, fe)

        # 2) Fallback: Bot API (работает только если пользователь доступен для бота)
        try:
            chat = await bot.get_chat(f'@{clean_username}')
        except Exception as e:
            logger.error(f"BotAPI get_chat failed for {clean_username}: {e}")
            return Response(
                text=json.dumps({
                    'error': 'not_found',
                    'message': 'Пользователь не найден. Убедитесь, что указан верный @username.',
                    'details': str(e),
                    'telethon_connected': bool(telethon_client is not None),
                    'fragment_connected': bool(request.app.get("fragment_site_enabled")),
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
    
    # Вспомогательная функция для получения пути к users_data.json
    # (определяем здесь, чтобы использовать в вебхуках и других хендлерах)
    _script_dir = os.path.dirname(os.path.abspath(__file__))
    USERS_DATA_PATHS = [
        os.path.join(_script_dir, "users_data.json"),
        os.path.join(os.path.dirname(_script_dir), "users_data.json"),
    ]
    
    def _get_users_data_path():
        for p in USERS_DATA_PATHS:
            if os.path.exists(p):
                return p
        return USERS_DATA_PATHS[0]
    
    # Файл заказов CryptoBot (чтобы вебхук находил заказ после перезапуска)
    CRYPTOBOT_ORDERS_FILE = os.path.join(_script_dir, "cryptobot_orders.json")
    
    def _load_cryptobot_order_from_file(invoice_id: str) -> Optional[dict]:
        try:
            data = _read_json_file(CRYPTOBOT_ORDERS_FILE) or {}
            return data.get(str(invoice_id)) if isinstance(data, dict) else None
        except Exception as e:
            logger.warning("Failed to load cryptobot order from file: %s", e)
            return None
    
    def _save_cryptobot_order_to_file(invoice_id: str, order_meta: dict):
        try:
            data = _read_json_file(CRYPTOBOT_ORDERS_FILE) or {}
            if not isinstance(data, dict):
                data = {}
            data[str(invoice_id)] = order_meta
            with open(CRYPTOBOT_ORDERS_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning("Failed to save cryptobot order to file: %s", e)
    
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
    # Platega.io: transaction_id (UUID) -> meta (context, user_id, purchase, amount_rub, order_id, created_at, delivered)
    PLATEGA_ORDERS_FILE = os.path.join(_script_dir, "platega_orders.json")

    def _load_platega_order_from_file(transaction_id: str) -> Optional[dict]:
        try:
            data = _read_json_file(PLATEGA_ORDERS_FILE) or {}
            return data.get(str(transaction_id)) if isinstance(data, dict) else None
        except Exception as e:
            logger.warning("Failed to load platega order from file: %s", e)
            return None

    def _save_platega_order_to_file(transaction_id: str, order_meta: dict):
        try:
            data = _read_json_file(PLATEGA_ORDERS_FILE) or {}
            if not isinstance(data, dict):
                data = {}
            data[str(transaction_id)] = order_meta
            with open(PLATEGA_ORDERS_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning("Failed to save platega order to file: %s", e)

    try:
        _platega_data = _read_json_file(PLATEGA_ORDERS_FILE) or {}
        app["platega_orders"] = _platega_data if isinstance(_platega_data, dict) else {}
    except Exception:
        app["platega_orders"] = {}

    # FreeKassa: MERCHANT_ORDER_ID (наш order_id, обычно вида #ABC123) -> meta
    FREEKASSA_ORDERS_FILE = os.path.join(_script_dir, "freekassa_orders.json")

    def _load_freekassa_order_from_file(order_id: str) -> Optional[dict]:
        try:
            data = _read_json_file(FREEKASSA_ORDERS_FILE) or {}
            return data.get(str(order_id)) if isinstance(data, dict) else None
        except Exception:
            return None

    def _save_freekassa_order_to_file(order_id: str, meta: dict) -> None:
        try:
            data = _read_json_file(FREEKASSA_ORDERS_FILE) or {}
            if not isinstance(data, dict):
                data = {}
            data[str(order_id)] = meta
            _save_json_file(FREEKASSA_ORDERS_FILE, data)
        except Exception as e:
            logger.warning("Failed to save FreeKassa order to file: %s", e)

    try:
        app["freekassa_orders"] = _read_json_file(FREEKASSA_ORDERS_FILE) or {}
        if not isinstance(app["freekassa_orders"], dict):
            app["freekassa_orders"] = {}
    except Exception:
        app["freekassa_orders"] = {}

    # Комиссия Platega: сохраняем в файл, чтобы переживала перезапуск бота
    PLATEGA_COMMISSION_FILE = os.path.join(_script_dir, "platega_commission.json")

    def _load_platega_commission_from_file():
        global _platega_sbp_commission_override, _platega_cards_commission_override
        try:
            data = _read_json_file(PLATEGA_COMMISSION_FILE) or {}
            if isinstance(data, dict):
                sbp = data.get("sbp_percent")
                cards = data.get("cards_percent")
                if sbp is not None and sbp != "":
                    _platega_sbp_commission_override = float(sbp)
                if cards is not None and cards != "":
                    _platega_cards_commission_override = float(cards)
        except Exception as e:
            logger.warning("Failed to load Platega commission from file: %s", e)

    _load_platega_commission_from_file()

    # Курсы звёзд и Steam: загрузка из файла (заданы в админке)
    APP_RATES_FILE = os.path.join(_script_dir, "app_rates.json")

    def _load_app_rates_from_file():
        global _star_price_rub_override, _star_buy_rate_rub_override, _steam_rate_rub_override
        try:
            data = _read_json_file(APP_RATES_FILE) or {}
            if isinstance(data, dict):
                v = data.get("star_price_rub")
                if v is not None and v != "":
                    _star_price_rub_override = float(v)
                v = data.get("star_buy_rate_rub")
                if v is not None and v != "":
                    _star_buy_rate_rub_override = float(v)
                v = data.get("steam_rate_rub")
                if v is not None and v != "":
                    _steam_rate_rub_override = float(v)
        except Exception as e:
            logger.warning("Failed to load app rates from file: %s", e)

    def _save_app_rates_to_file():
        try:
            data = {}
            if _star_price_rub_override is not None:
                data["star_price_rub"] = _star_price_rub_override
            if _star_buy_rate_rub_override is not None:
                data["star_buy_rate_rub"] = _star_buy_rate_rub_override
            if _steam_rate_rub_override is not None:
                data["steam_rate_rub"] = _steam_rate_rub_override
            if data:
                _save_json_file(APP_RATES_FILE, data)
        except Exception as e:
            logger.warning("Failed to save app rates to file: %s", e)

    _load_app_rates_from_file()

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

    # Debug: проверить Fragment searchStarsRecipient (без Telethon)
    async def fragment_search_recipient_handler(request):
        username = _get_username_from_request(request)
        clean = (username or "").lstrip("@").strip()
        if not clean:
            return _json_response({"error": "bad_request", "message": "username is required"}, status=400)
        if not app.get("fragment_site_enabled"):
            return _json_response(
                {"error": "not_configured", "message": "Fragment не настроен (FRAGMENT_COOKIES/FRAGMENT_HASH)"},
                status=503,
            )
        try:
            referer = f"https://fragment.com/stars/buy?recipient={clean}&quantity=50"
            payload = {"query": clean, "quantity": "", "method": "searchStarsRecipient"}
            data = await _fragment_site_post(payload, referer=referer)
            return _json_response({"ok": True, "data": data})
        except Exception as e:
            logger.warning("fragment_search_recipient failed for %s: %s", clean, e)
            return _json_response({"ok": False, "error": str(e)}, status=502)

    app.router.add_get("/api/fragment/search-recipient", fragment_search_recipient_handler)
    app.router.add_route("OPTIONS", "/api/fragment/search-recipient", lambda r: Response(status=204, headers=_cors_headers()))

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

    # Admin: проверка пароля на бэкенде (пароль в ADMIN_PASSWORD env)
    ADMIN_PASSWORD = (os.environ.get("ADMIN_PASSWORD") or "").strip()

    async def admin_verify_handler(request):
        """POST /api/admin/verify — проверка пароля админки. JSON: { "password": "..." }"""
        try:
            body = await request.json()
        except Exception:
            return _json_response({"ok": False, "message": "Invalid JSON"}, status=400)
        pwd = (body.get("password") or "").strip()
        if not ADMIN_PASSWORD:
            logger.warning("ADMIN_PASSWORD не задан в env — админка не защищена")
            return _json_response({"ok": False, "message": "Админка не настроена"}, status=503)
        ok = pwd and len(pwd) > 0 and pwd == ADMIN_PASSWORD
        return _json_response({"ok": ok})

    app.router.add_post("/api/admin/verify", admin_verify_handler)
    app.router.add_route("OPTIONS", "/api/admin/verify", lambda r: Response(status=204, headers=_cors_headers()))

    async def admin_stats_handler(request):
        """GET /api/admin/stats — статистика для админки. Заголовок: Authorization: Bearer <ADMIN_PASSWORD>"""
        try:
            auth = request.headers.get("Authorization") or ""
            token = (auth.replace("Bearer ", "").replace("bearer ", "").strip() if auth else "") or request.headers.get("X-Admin-Password") or ""
            if not ADMIN_PASSWORD or token != ADMIN_PASSWORD:
                return _json_response({"error": "unauthorized"}, status=401)
            # 1) БАЗОВЫЕ ДАННЫЕ: JSON (users_data.json) — чтобы не потерять старую статистику
            path = _get_users_data_path()
            users_data = _read_json_file(path) if path else {}
            if path:
                logger.info(f"admin_stats: Loaded base stats from JSON file: {path}, {len(users_data) if isinstance(users_data, dict) else 0} users")
            else:
                logger.warning("admin_stats: No users_data.json path found")

            if not isinstance(users_data, dict):
                users_data = {}

            # 2) ДОПОЛНИТЕЛЬНО ПОДМЕШИВАЕМ PostgreSQL, если включена
            try:
                import db as _db_stats
                if _db_stats.is_enabled():
                    try:
                        # stars_only=False — новые версии db.py; для старых падать не будем
                        try:
                            db_users = await _db_stats.get_users_with_purchases(stars_only=False)
                        except TypeError as te:
                            if "stars_only" in str(te):
                                db_users = await _db_stats.get_users_with_purchases()
                            else:
                                raise
                        logger.info(f"admin_stats: Loaded {len(db_users)} users from PostgreSQL for merge")
                    except Exception as db_err:
                        logger.warning(f"admin_stats: Failed to load from PostgreSQL: {db_err}")
                        db_users = {}

                    if isinstance(db_users, dict) and db_users:
                        for uid, u in db_users.items():
                            if not isinstance(u, dict):
                                continue
                            if uid not in users_data or not isinstance(users_data.get(uid), dict):
                                users_data[uid] = u
                                continue
                            # Мержим данные по пользователю
                            base_u = users_data[uid]
                            # username / first_name — берём из БД, если есть
                            if u.get("username"):
                                base_u["username"] = u.get("username")
                            if u.get("first_name"):
                                base_u["first_name"] = u.get("first_name")
                            # Даты регистрации/активности
                            if u.get("registration_date") and not base_u.get("registration_date"):
                                base_u["registration_date"] = u.get("registration_date")
                            if u.get("created_at") and not base_u.get("created_at"):
                                base_u["created_at"] = u.get("created_at")
                            if u.get("last_activity"):
                                # Берём более позднюю активность
                                la_base = str(base_u.get("last_activity") or "")
                                la_db = str(u.get("last_activity") or "")
                                if not la_base or la_db > la_base:
                                    base_u["last_activity"] = la_db
                            # Покупки: просто дописываем список
                            base_p = base_u.get("purchases") or []
                            db_p = u.get("purchases") or []
                            if not isinstance(base_p, list):
                                base_p = []
                            if not isinstance(db_p, list):
                                db_p = []
                            base_u["purchases"] = base_p + db_p
                            users_data[uid] = base_u
            except Exception as merge_err:
                logger.warning(f"admin_stats: error merging PostgreSQL stats: {merge_err}")
            
            now = datetime.now()
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            week_start = now.timestamp() - 7 * 24 * 3600
            month_start = now.timestamp() - 30 * 24 * 3600
            total_users = len(users_data)
            total_sales = 0
            total_turnover_rub = 0.0
            sales_today = sales_week = sales_month = 0
            turnover_today = turnover_week = turnover_month = 0.0
            # Отдельная статистика по пополнениям баланса (type='balance')
            balance_topups_count = 0
            balance_topups_rub = 0.0
            
            logger.info(f"admin_stats: Processing {total_users} users")
            
            for uid, u in users_data.items():
                if not isinstance(u, dict):
                    continue
                purchases_list = u.get("purchases") or []
                if not purchases_list:
                    continue
                for p in purchases_list:
                    if not isinstance(p, dict):
                        continue
                    amount = float(p.get("amount") or p.get("amount_rub") or 0)
                    ptype = (p.get("type") or "").strip().lower()
                    total_sales += 1
                    total_turnover_rub += amount
                    if ptype == "balance":
                        balance_topups_count += 1
                        balance_topups_rub += amount
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
                    if ts is not None:
                        if ts >= today_start.timestamp():
                            sales_today += 1
                            turnover_today += amount
                        if ts >= week_start:
                            sales_week += 1
                            turnover_week += amount
                        if ts >= month_start:
                            sales_month += 1
                            turnover_month += amount
            
            # Подсчёт регистраций по периодам
            regs_day = regs_week = regs_month = 0
            activity_day = activity_week = activity_month = 0
            
            for uid, u in users_data.items():
                if not isinstance(u, dict):
                    continue
                # Проверяем дату регистрации
                reg_date = u.get("registration_date") or u.get("created_at")
                if reg_date:
                    try:
                        if isinstance(reg_date, str):
                            reg_ts = datetime.strptime(reg_date[:19], "%Y-%m-%d %H:%M:%S").timestamp()
                        elif isinstance(reg_date, (int, float)):
                            reg_ts = float(reg_date) if reg_date > 1e9 else reg_date
                        else:
                            reg_ts = None
                        if reg_ts:
                            if reg_ts >= today_start.timestamp():
                                regs_day += 1
                            if reg_ts >= week_start:
                                regs_week += 1
                            if reg_ts >= month_start:
                                regs_month += 1
                    except Exception:
                        pass
                
                # Проверяем активность (last_activity)
                last_act = u.get("last_activity") or u.get("updated_at")
                if last_act:
                    try:
                        if isinstance(last_act, str):
                            act_ts = datetime.strptime(last_act[:19], "%Y-%m-%d %H:%M:%S").timestamp()
                        elif isinstance(last_act, (int, float)):
                            act_ts = float(last_act) if last_act > 1e9 else last_act
                        else:
                            act_ts = None
                        if act_ts:
                            if act_ts >= today_start.timestamp():
                                activity_day += 1
                            if act_ts >= week_start:
                                activity_week += 1
                            if act_ts >= month_start:
                                activity_month += 1
                    except Exception:
                        pass
            
            logger.info(f"admin_stats: total_users={total_users}, total_sales={total_sales}, total_turnover={total_turnover_rub:.2f}, regs_day={regs_day}, activity_day={activity_day}")
            
            return _json_response({
                "totalUsers": total_users,
                "totalSales": total_sales,
                "totalTurnoverRub": round(total_turnover_rub, 2),
                "salesToday": sales_today,
                "salesWeek": sales_week,
                "salesMonth": sales_month,
                "turnoverToday": round(turnover_today, 2),
                "turnoverWeek": round(turnover_week, 2),
                "turnoverMonth": round(turnover_month, 2),
                "regsDay": regs_day,
                "regsWeek": regs_week,
                "regsMonth": regs_month,
                "activityDay": activity_day,
                "activityWeek": activity_week,
                "activityMonth": activity_month,
                "balanceTopupsCount": balance_topups_count,
                "balanceTopupsRub": round(balance_topups_rub, 2),
            })
        except Exception as e:
            logger.error("admin_stats error: %s", e)
            return _json_response({"error": str(e)}, status=500)

    app.router.add_get("/api/admin/stats", admin_stats_handler)
    app.router.add_route("OPTIONS", "/api/admin/stats", lambda r: Response(status=204, headers=_cors_headers()))

    async def admin_balance_adjust_handler(request):
        """
        POST /api/admin/balance-adjust
        Заголовок: Authorization: Bearer <ADMIN_PASSWORD>
        JSON: { "username": "name", "user_id": "123", "amount": 100 }  # amount может быть отрицательной
        """
        auth = request.headers.get("Authorization") or ""
        token = (auth.replace("Bearer ", "").replace("bearer ", "").strip() if auth else "") or request.headers.get("X-Admin-Password") or ""
        if not ADMIN_PASSWORD or token != ADMIN_PASSWORD:
            return _json_response({"success": False, "error": "unauthorized"}, status=401)

        try:
            body = await request.json() if request.can_read_body else {}
        except Exception:
            body = {}

        username = (body.get("username") or "").strip().lstrip("@")
        user_id = str(body.get("user_id") or "").strip()
        try:
            amount = float(body.get("amount") or 0)
        except (TypeError, ValueError):
            amount = 0.0

        if not amount:
            return _json_response({"success": False, "error": "bad_amount", "message": "Сумма должна быть ненулевой"}, status=400)

        import db as _db_admin
        if not _db_admin.is_enabled():
            return _json_response({"success": False, "error": "service_unavailable", "message": "База данных недоступна"}, status=503)

        # Определяем user_id по username, если он не передан явно
        if not user_id:
            if not username:
                return _json_response({"success": False, "error": "bad_request", "message": "Нужен username или user_id"}, status=400)
            user_id = await _db_admin.user_find_by_username(username)
            if not user_id:
                return _json_response({"success": False, "error": "not_found", "message": "Пользователь не найден"}, status=404)

        # Корректируем баланс
        new_balance = None
        if amount > 0:
            ok = await _db_admin.balance_add_rub(user_id, amount)
            if not ok:
                return _json_response({"success": False, "error": "db_error", "message": "Не удалось пополнить баланс"}, status=500)
            bal = await _db_admin.balance_get(user_id)
            new_balance = bal.get("balance_rub", 0.0)
        else:
            res = await _db_admin.balance_deduct_rub(user_id, -amount)
            if res is None:
                return _json_response({"success": False, "error": "insufficient_funds", "message": "Недостаточно средств для списания"}, status=400)
            new_balance = res.get("balance_rub", 0.0)

        return _json_response({
            "success": True,
            "user_id": user_id,
            "username": username,
            "delta": amount,
            "balance_rub": new_balance,
        })

    app.router.add_post("/api/admin/balance-adjust", admin_balance_adjust_handler)
    app.router.add_route("OPTIONS", "/api/admin/balance-adjust", lambda r: Response(status=204, headers=_cors_headers()))

    # Отдаём robots.txt, чтобы боты (например, Яндекс) не вызывали 404 и не засоряли логи
    async def robots_handler(request):
        """
        Простой robots.txt: разрешаем всё, но главное — не даём 404.
        """
        text = "User-agent: *\nAllow: /\n"
        return web.Response(text=text, content_type="text/plain", charset="utf-8")

    app.router.add_get('/robots.txt', robots_handler)

    # Favicon — чтобы браузеры и проверки не давали 404/500 в логах
    async def favicon_handler(request):
        return web.Response(status=204)

    app.router.add_get('/favicon.ico', favicon_handler)

    # Поиск заказа по пользовательскому ID вида #ABC123
    async def _find_order_by_custom_id(order_id: str) -> tuple | None:
        """
        Ищет заказ CryptoBot по нашему кастомному order_id (#ABC123)
        в файле cryptobot_orders.json (CRYPTOBOT_ORDERS_FILE).
        """
        oid = (order_id or "").strip().upper()
        if not oid:
            return None
        if not oid.startswith("#"):
            oid = "#" + oid
        try:
            data = _read_json_file(CRYPTOBOT_ORDERS_FILE) or {}
            if not isinstance(data, dict):
                return None
            for inv_id, meta in data.items():
                if not isinstance(meta, dict):
                    continue
                purchase_meta = meta.get("purchase") or {}
                if str(purchase_meta.get("order_id") or "").upper() == oid:
                    return (inv_id, meta)
        except Exception as e:
            logger.warning(f"_find_order_by_custom_id error: {e}")
        return None

    async def idea_submit_handler(request):
        """
        POST /api/idea/submit
        Принимает идею/предложение и пересылает её в служебный чат IDEAS_CHAT_ID.
        JSON: { "user_id": "...", "username": "...", "first_name": "...", "text": "..." }
        """
        try:
            try:
                body = await request.json()
            except Exception:
                return _json_response({"success": False, "error": "bad_request", "message": "Invalid JSON"}, status=400)

            text = (body.get("text") or "").strip()
            if len(text) < 5:
                return _json_response({"success": False, "error": "too_short", "message": "Идея слишком короткая"}, status=400)
            if len(text) > 500:
                text = text[:500]

            user_id_raw = body.get("user_id")
            user_id = str(user_id_raw or "").strip()
            username = (body.get("username") or "").strip()
            first_name = (body.get("first_name") or "").strip()

            if not user_id:
                return _json_response(
                    {
                        "success": False,
                        "error": "user_required",
                        "message": "Не удалось определить пользователя. Откройте мини‑приложение из Telegram и попробуйте ещё раз.",
                    },
                    status=400,
                )

            # Лимит: не чаще 1 раза в 12 часов на одного пользователя
            try:
                limits = _read_json_file(IDEAS_LIMITS_FILE) or {}
                if not isinstance(limits, dict):
                    limits = {}
            except Exception as e:
                logger.warning("Failed to read ideas limits file: %s", e)
                limits = {}

            now_ts = time.time()
            last_ts = float(limits.get(user_id, 0) or 0)
            cooldown = 12 * 60 * 60  # 12 часов
            if last_ts and now_ts - last_ts < cooldown:
                remaining = int(cooldown - (now_ts - last_ts))
                hours = remaining // 3600
                minutes = (remaining % 3600) // 60
                if hours > 0:
                    remain_text = f"{hours} ч"
                    if minutes > 0:
                        remain_text += f" {minutes} мин"
                else:
                    remain_text = f"{minutes} мин"
                return _json_response(
                    {
                        "success": False,
                        "error": "cooldown",
                        "message": f"Вы уже отправляли идею. Новую можно будет отправить через {remain_text}.",
                        "retry_after_seconds": remaining,
                    },
                    status=429,
                )

            # Сразу фиксируем отправку в лимитах, чтобы таймер работал даже
            # если нет IDEAS_CHAT_ID или отправка в чат по какой-то причине упала.
            try:
                limits[user_id] = now_ts
                _save_json_file(IDEAS_LIMITS_FILE, limits)
            except Exception as se:
                logger.warning("Failed to update ideas limits file (pre-send): %s", se)

            header = "💡 <b>Новая идея</b>\n\n"
            user_line = ""
            if username:
                user_line = f"От: @{username}"
            elif first_name or user_id:
                user_line = f"От: {first_name or 'пользователь'}"
                if user_id:
                    user_line += f" (ID: <code>{user_id}</code>)"
            if not user_line and user_id:
                user_line = f"От: ID <code>{user_id}</code>"
            if not user_line:
                user_line = "От: неизвестный пользователь"

            idea_block = f"\n\nТекст идеи:\n<code>{text}</code>"

            full_text = header + user_line + idea_block

            if IDEAS_CHAT_ID:
                try:
                    await bot.send_message(IDEAS_CHAT_ID, full_text, parse_mode="HTML", disable_web_page_preview=True)
                except Exception as e:
                    logger.warning(f"Failed to send idea to IDEAS_CHAT_ID={IDEAS_CHAT_ID}: {e}")
                    return _json_response({"success": False, "error": "send_failed", "message": "Не удалось отправить идею. Попробуйте позже."}, status=502)
            else:
                logger.warning("IDEAS_CHAT_ID not set; idea text:\n%s", full_text)

            return _json_response({"success": True})
        except Exception as e:
            logger.exception(f"/api/idea/submit error: {e}")
            return _json_response({"success": False, "error": "internal_error", "message": "Внутренняя ошибка сервера"}, status=500)

    app.router.add_post('/api/idea/submit', idea_submit_handler)
    app.router.add_route('OPTIONS', '/api/idea/submit', lambda r: Response(status=204, headers=_cors_headers()))

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

    async def steam_rate_handler(request):
        """Курс Steam: 1 рубль на Steam = X ₽ (из env / админки). GET — вернуть текущий, POST — установить (body: { steam_rate_rub: number })."""
        global _steam_rate_rub_override
        if request.method == "POST":
            try:
                body = await request.json()
            except Exception:
                return _json_response({"error": "bad_request", "message": "Invalid JSON"}, status=400)
            try:
                rate = float(body.get("steam_rate_rub") or body.get("steam_rate") or 0)
            except (TypeError, ValueError):
                return _json_response({"error": "bad_request", "message": "steam_rate_rub должен быть числом"}, status=400)
            if rate < 0.01 or rate > 100:
                return _json_response({"error": "bad_request", "message": "Курс должен быть от 0.01 до 100"}, status=400)
            _steam_rate_rub_override = rate
            _save_app_rates_to_file()
            try:
                import db as _db_rates
                if _db_rates.is_enabled():
                    await _db_rates.rates_set("steam_rate_rub", rate)
                    saved_rates = await _db_rates.rates_get()
                    saved_value = saved_rates.get("steam_rate_rub")
                    if saved_value == rate:
                        logger.info(f"✓ Saved steam_rate_rub={rate} to DB and verified")
                    else:
                        logger.error(f"✗ Failed to save steam_rate_rub: expected {rate}, got {saved_value}")
                else:
                    logger.warning("PostgreSQL not enabled, steam_rate_rub not saved to DB")
            except Exception as e:
                logger.error(f"rates_set steam_rate_rub error: {e}", exc_info=True)
            return _json_response({"steam_rate_rub": _steam_rate_rub_override})
        try:
            rate = _get_steam_rate_rub()
            return _json_response({"steam_rate_rub": rate})
        except Exception as e:
            logger.warning("steam_rate GET error: %s", e)
            return _json_response({"steam_rate_rub": 1.06})

    app.router.add_get('/api/steam-rate', steam_rate_handler)
    app.router.add_post('/api/steam-rate', steam_rate_handler)
    app.router.add_route('OPTIONS', '/api/steam-rate', lambda r: Response(status=204, headers=_cors_headers()))

    async def star_rate_handler(request):
        """Курсы звёзд: GET — текущие, POST — установить из админки (body: { star_price_rub?, star_buy_rate_rub? })."""
        global _star_price_rub_override, _star_buy_rate_rub_override
        if request.method == "POST":
            try:
                body = await request.json()
            except Exception:
                return _json_response({"error": "bad_request", "message": "Invalid JSON"}, status=400)
            updated = False
            v = body.get("star_price_rub")
            if v is not None and v != "":
                try:
                    r = float(v)
                    if 0.01 <= r <= 1000:
                        _star_price_rub_override = r
                        updated = True
                        try:
                            import db as _db_rates
                            if _db_rates.is_enabled():
                                await _db_rates.rates_set("star_price_rub", r)
                                # Проверяем, что курс действительно сохранился
                                saved_rates = await _db_rates.rates_get()
                                saved_value = saved_rates.get("star_price_rub")
                                if saved_value == r:
                                    logger.info(f"✓ Saved star_price_rub={r} to DB and verified")
                                else:
                                    logger.error(f"✗ Failed to save star_price_rub: expected {r}, got {saved_value}")
                            else:
                                logger.warning("PostgreSQL not enabled, star_price_rub not saved to DB")
                        except Exception as e:
                            logger.error(f"rates_set star_price_rub error: {e}", exc_info=True)
                except (TypeError, ValueError):
                    pass
            v = body.get("star_buy_rate_rub")
            if v is not None and v != "":
                try:
                    r = float(v)
                    if 0.01 <= r <= 100:
                        _star_buy_rate_rub_override = r
                        updated = True
                        try:
                            import db as _db_rates
                            if _db_rates.is_enabled():
                                await _db_rates.rates_set("star_buy_rate_rub", r)
                                saved_rates = await _db_rates.rates_get()
                                saved_value = saved_rates.get("star_buy_rate_rub")
                                if saved_value == r:
                                    logger.info(f"✓ Saved star_buy_rate_rub={r} to DB and verified")
                                else:
                                    logger.error(f"✗ Failed to save star_buy_rate_rub: expected {r}, got {saved_value}")
                            else:
                                logger.warning("PostgreSQL not enabled, star_buy_rate_rub not saved to DB")
                        except Exception as e:
                            logger.error(f"rates_set star_buy_rate_rub error: {e}", exc_info=True)
                except (TypeError, ValueError):
                    pass
            if updated:
                _save_app_rates_to_file()
            return _json_response({
                "star_price_rub": _get_star_price_rub(),
                "star_buy_rate_rub": _get_star_buy_rate_rub(),
            })
        try:
            return _json_response({
                "star_price_rub": _get_star_price_rub(),
                "star_buy_rate_rub": _get_star_buy_rate_rub(),
            })
        except Exception as e:
            logger.warning("star_rate GET error: %s", e)
            return _json_response({"star_price_rub": 1.37, "star_buy_rate_rub": 0.65})

    app.router.add_get('/api/star-rate', star_rate_handler)
    app.router.add_post('/api/star-rate', star_rate_handler)
    app.router.add_route('OPTIONS', '/api/star-rate', lambda r: Response(status=204, headers=_cors_headers()))

    async def premium_prices_handler(request):
        """GET/POST /api/premium-prices — цены Premium (3, 6, 12 мес.) для FreeKassa и др. POST: { premium_3?, premium_6?, premium_12? } или { 3?, 6?, 12? }."""
        if request.method == "POST":
            try:
                body = await request.json()
            except Exception:
                return _json_response({"error": "bad_request", "message": "Invalid JSON"}, status=400)
            for key_name, num in [("premium_3", 3), ("premium_6", 6), ("premium_12", 12)]:
                v = body.get(key_name) or body.get(str(num))
                if v is not None and v != "":
                    try:
                        r = float(v)
                        if 1 <= r <= 100000:
                            try:
                                import db as _db_prem
                                if _db_prem.is_enabled():
                                    await _db_prem.rates_set(key_name, r)
                                    logger.info(f"Saved {key_name}={r} to DB")
                            except Exception as e:
                                logger.warning(f"rates_set {key_name} error: {e}")
                    except (TypeError, ValueError):
                        pass
            return _json_response({"success": True})
        try:
            import db as _db_prem
            if _db_prem.is_enabled():
                rates = await _db_prem.rates_get()
                return _json_response({
                    "premium_3": float(rates.get("premium_3") or 0) or 983,
                    "premium_6": float(rates.get("premium_6") or 0) or 1311,
                    "premium_12": float(rates.get("premium_12") or 0) or 2377,
                })
        except Exception as e:
            logger.debug("premium_prices rates_get: %s", e)
        return _json_response({
            "premium_3": PREMIUM_PRICES_RUB.get(3, 983),
            "premium_6": PREMIUM_PRICES_RUB.get(6, 1311),
            "premium_12": PREMIUM_PRICES_RUB.get(12, 2377),
        })

    app.router.add_get('/api/premium-prices', premium_prices_handler)
    app.router.add_post('/api/premium-prices', premium_prices_handler)
    app.router.add_route('OPTIONS', '/api/premium-prices', lambda r: Response(status=204, headers=_cors_headers()))

    # Комиссия Platega: СБП % и Карты % (GET — текущие, POST — установить из админки)
    async def platega_commission_handler(request):
        global _platega_sbp_commission_override, _platega_cards_commission_override
        if request.method == "POST":
            try:
                body = await request.json()
            except Exception:
                return _json_response({"error": "bad_request", "message": "Invalid JSON"}, status=400)
            try:
                sbp = float(body.get("sbp_percent") or body.get("sbp") or 10)
                cards = float(body.get("cards_percent") or body.get("cards") or 14)
            except (TypeError, ValueError):
                return _json_response({"error": "bad_request", "message": "sbp_percent и cards_percent должны быть числами"}, status=400)
            if sbp < 0 or sbp > 100 or cards < 0 or cards > 100:
                return _json_response({"error": "bad_request", "message": "Комиссия от 0 до 100%"}, status=400)
            _platega_sbp_commission_override = sbp
            _platega_cards_commission_override = cards
            try:
                _save_json_file(PLATEGA_COMMISSION_FILE, {"sbp_percent": sbp, "cards_percent": cards})
            except Exception as e:
                logger.warning("Failed to save Platega commission to file: %s", e)
            return _json_response({"sbp_percent": sbp, "cards_percent": cards})
        return _json_response({
            "sbp_percent": _get_platega_sbp_commission(),
            "cards_percent": _get_platega_cards_commission(),
        })
    app.router.add_get('/api/platega-commission', platega_commission_handler)
    app.router.add_post('/api/platega-commission', platega_commission_handler)
    app.router.add_route('OPTIONS', '/api/platega-commission', lambda r: Response(status=204, headers=_cors_headers()))

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
        # Проценты JetRefs: 1‑й уровень 4%, 2‑й — 8%, 3‑й — 12%
        for pid, percent in (
            (parent1, 0.04),
            (parent2, 0.08),
            (parent3, 0.12),
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
        
        В новой логике вся реферальная награда сразу зачисляется на баланс в рублях.
        Здесь возвращаем только статистику в RUB (без TON).
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
        payload = {
            "user_id": uid,
            "earned_rub": round(earned_rub, 2),
            "volume_rub": round(volume_rub, 2),
            "referrals_level1": lvl1,
            "referrals_level2": lvl2,
            "referrals_level3": lvl3,
            "total_referrals": total_refs,
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
        payout_rub = round(stars_amount * _get_star_buy_rate_rub(), 2)

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
    # Для тестов через @CryptoTestnetBot можно:
    #   - выдать тестовый токен;
    #   - в Railway установить CRYPTO_PAY_TOKEN=ТЕСТОВЫЙ_ТОКЕН
    #   - при необходимости переопределить базовый URL:
    #       CRYPTO_PAY_BASE=https://testnet-pay.crypt.bot/api
    _cryptobot_cfg_early = _read_json_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), "cryptobot_config.json"))
    CRYPTO_PAY_TOKEN = _get_env_clean("CRYPTO_PAY_TOKEN") or _cryptobot_cfg_early.get("api_token", "")
    CRYPTO_PAY_BASE = os.getenv("CRYPTO_PAY_BASE", "https://pay.crypt.bot/api").rstrip("/")

    # Platega.io — оплата картами и СБП (https://docs.platega.io/)
    PLATEGA_MERCHANT_ID = os.getenv("PLATEGA_MERCHANT_ID", "").strip()
    PLATEGA_SECRET = os.getenv("PLATEGA_SECRET", "").strip()
    PLATEGA_BASE_URL = (os.getenv("PLATEGA_BASE_URL", "https://app.platega.io") or "https://app.platega.io").rstrip("/")

    # FreeKassa — приём СБП и карт через API (https://docs.freekassa.net/)
    FREEKASSA_SHOP_ID = (os.getenv("FREEKASSA_SHOP_ID") or "").strip()
    FREEKASSA_API_KEY = _get_env_clean("FREEKASSA_API_KEY") or ""
    FREEKASSA_SECRET1 = _get_env_clean("FREEKASSA_SECRET1") or ""
    FREEKASSA_SECRET2 = _get_env_clean("FREEKASSA_SECRET2") or ""

    def _get_client_ip(request: web.Request) -> str:
        # Пытаемся взять реальный IP, если прокси передаёт заголовки
        ip = request.headers.get("X-Real-IP") or request.headers.get("X-Forwarded-For", "")
        if ip and "," in ip:
            ip = ip.split(",")[0].strip()
        if not ip:
            ip = request.remote or ""
        return ip

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
    # Пробрасываем в app, чтобы ими могли пользоваться хендлеры вне этой функции
    app["fragment_site_enabled"] = FRAGMENT_SITE_ENABLED
    app["fragment_site_cookies"] = FRAGMENT_SITE_COOKIES
    app["fragment_site_hash"] = FRAGMENT_SITE_HASH
    # TON-кошелёк бота для отправки TON в Fragment (как в ezstar: бот сам платит Fragment, звёзды приходят получателю).
    TONAPI_KEY = _get_env_clean("TONAPI_KEY") or str(_fragment_site_cfg.get("tonapi_key", "") or "").strip()
    _mnemonic_raw = _get_env_clean("MNEMONIC") or _fragment_site_cfg.get("mnemonic")
    if isinstance(_mnemonic_raw, str):
        MNEMONIC = [s.strip() for s in _mnemonic_raw.replace(",", " ").split() if s.strip()] if _mnemonic_raw else []
    elif isinstance(_mnemonic_raw, list):
        MNEMONIC = [str(x).strip() for x in _mnemonic_raw if str(x).strip()]
    else:
        MNEMONIC = []

    # Проверяем, доступна ли библиотека tonutils (на некоторых деплоях она может не установиться).
    try:
        import tonutils.client as _tu_client  # type: ignore  # noqa: F401
        _TONUTILS_AVAILABLE = True
    except Exception:
        _TONUTILS_AVAILABLE = False

    TON_WALLET_ENABLED = bool(TONAPI_KEY and len(MNEMONIC) >= 24 and _TONUTILS_AVAILABLE)

    # Клиент fragment-api-py (для Telegram Premium через Fragment)
    _FRAGMENT_API_CLIENT: Optional["AsyncFragmentAPI"] = None  # type: ignore[valid-type]

    async def _get_fragment_api_client() -> "AsyncFragmentAPI":  # type: ignore[valid-type]
        """
        Ленивая инициализация AsyncFragmentAPI.
        Используем те же cookies/hash/mnemonic/TONAPI_KEY, что и для выдачи звёзд.
        """
        if AsyncFragmentAPI is None:
            raise RuntimeError("fragment-api-py не установлен. Добавьте fragment-api-py в зависимости.")
        if not FRAGMENT_SITE_ENABLED:
            raise RuntimeError("FRAGMENT_SITE_COOKIES/FRAGMENT_SITE_HASH не настроены для Fragment.")
        if not TON_WALLET_ENABLED:
            raise RuntimeError("TONAPI_KEY/MNEMONIC не настроены для кошелька бота.")

        global _FRAGMENT_API_CLIENT
        if _FRAGMENT_API_CLIENT is None:
            wallet_version = os.getenv("WALLET_VERSION", "V4R2")
            _FRAGMENT_API_CLIENT = AsyncFragmentAPI(
                cookies=FRAGMENT_SITE_COOKIES,
                hash_value=FRAGMENT_SITE_HASH,
                wallet_mnemonic=_mnemonic_raw,
                wallet_api_key=TONAPI_KEY,
                wallet_version=wallet_version,
            )
        return _FRAGMENT_API_CLIENT

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
        # Если нет ключей или tonutils недоступен — кошелёк считаем отключённым.
        if not TON_WALLET_ENABLED:
            return (None, "TON-кошелёк отключён (нет TONAPI_KEY/MNEMONIC или не установлена библиотека tonutils)")
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
        """Создание заказа на звёзды через fragment.com/stars.
        Раньше при наличии TON_WALLET_ENABLED мы делали только валидацию и не отдавали ссылку (режим CryptoBot+кошелёк бота).
        Сейчас ВСЕГДА создаём полноценный заказ и отдаём payment_url, чтобы пользователь оплачивал напрямую через TonKeeper,
        а Fragment сам доставлял звёзды получателю.
        """
        referer = f"https://fragment.com/stars/buy?recipient={recipient}&quantity={stars_amount}"
        search_payload = {"query": recipient, "quantity": "", "method": "searchStarsRecipient"}
        search = await _fragment_site_post(search_payload, referer=referer)
        found = (search or {}).get("found")
        if not found or not isinstance(found, dict) or not found.get("recipient"):
            raise RuntimeError("Fragment: получатель не найден")
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
        """
        Проверка статуса оплаты.
        
        ВАЖНО: Для CryptoBot принимается ТОЛЬКО invoice_id.
        Вся критичная информация (сумма, тип покупки, получатель) хранится на бэкенде
        в метаданных инвойса, созданных при вызове create-invoice.
        """
        try:
            body = await request.json()
        except Exception:
            body = {}
        
        method = (body.get("method") or "").strip().lower()
        invoice_id = body.get("invoice_id")
        
        # Для CryptoBot принимаем ТОЛЬКО invoice_id - остальное игнорируем
        # Если invoice_id нет — возвращаем paid: false (без 400), чтобы клиент не показывал ошибку
        if method == "cryptobot":
            if not invoice_id:
                return _json_response({"paid": False})
            # Продолжаем проверку только с invoice_id
        # Для других методов (Fragment, TON, Platega) используем purchase, order_id, transaction_id из body
        purchase = body.get("purchase") or {}
        purchase_type = (purchase.get("type") or purchase.get("Type") or "").strip()
        is_stars = purchase_type == "stars" or (purchase.get("stars_amount") is not None and purchase.get("stars_amount") != 0)
        is_premium = purchase_type == "premium" or (purchase.get("months") is not None and purchase.get("months") != 0)
        order_id = (body.get("order_id") or body.get("orderId") or "").strip()
        transaction_id = (body.get("transaction_id") or body.get("transactionId") or "").strip()

        # FreeKassa (СБП / карты): проверка по нашему order_id (MERCHANT_ORDER_ID)
        if method in ("sbp", "card"):
            order_id_raw = (body.get("order_id") or body.get("orderId") or "").strip()
            if not order_id_raw:
                return _json_response({"paid": False})
            # Очищаем order_id от # и недопустимых символов (как при создании заказа)
            import re as _re
            payment_id = order_id_raw.lstrip("#").strip()
            payment_id = _re.sub(r'[^a-zA-Z0-9_-]', '', payment_id)
            if not payment_id:
                payment_id = order_id_raw.lstrip("#").replace("#", "").replace(" ", "").replace("-", "_")
            orders_fk = request.app.get("freekassa_orders") or {}
            order_meta = None
            if isinstance(orders_fk, dict):
                order_meta = orders_fk.get(str(payment_id))
            if not order_meta:
                order_meta = _load_freekassa_order_from_file(str(payment_id))
                if order_meta and isinstance(orders_fk, dict):
                    orders_fk[str(payment_id)] = order_meta
            if order_meta and order_meta.get("delivered"):
                # Возвращаем оригинальный order_id с # для фронтенда
                return _json_response({"paid": True, "order_id": order_id_raw, "delivered_by_freekassa": True})
            return _json_response({"paid": False, "order_id": order_id_raw})

        # Platega (карты / СБП): проверка по transaction_id
        if method == "platega":
            platega_tid = (body.get("transaction_id") or body.get("transactionId") or "").strip()
            if not platega_tid:
                return _json_response({"paid": False})
            order_meta = None
            try:
                orders = request.app.get("platega_orders")
                if isinstance(orders, dict):
                    order_meta = orders.get(platega_tid)
                if not order_meta:
                    order_meta = _load_platega_order_from_file(platega_tid)
                    if order_meta and isinstance(orders, dict):
                        orders[platega_tid] = order_meta
            except Exception as meta_err:
                logger.warning("platega order meta read failed for %s: %s", platega_tid, meta_err)
            if not order_meta:
                return _json_response({"paid": False})
            if order_meta.get("delivered"):
                return _json_response({"paid": True, "transaction_id": platega_tid})
            if not PLATEGA_MERCHANT_ID or not PLATEGA_SECRET:
                return _json_response({"paid": False})
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"{PLATEGA_BASE_URL}/transaction/{platega_tid}",
                        headers={"X-MerchantId": PLATEGA_MERCHANT_ID, "X-Secret": PLATEGA_SECRET},
                    ) as resp:
                        if resp.status != 200:
                            return _json_response({"paid": False})
                        data = await resp.json(content_type=None) if resp.content_type else {}
                status = (data.get("status") or "").strip().upper()
                if status != "CONFIRMED":
                    return _json_response({"paid": False, "transaction_id": platega_tid})
                # Выдача товара (аналогично CryptoBot fallback в payment_check)
                purchase_meta = order_meta.get("purchase") or {}
                ptype = (purchase_meta.get("type") or "").strip().lower()
                user_id = str(order_meta.get("user_id") or "unknown")
                try:
                    amount_rub = float(order_meta.get("amount_rub") or 0.0)
                except (TypeError, ValueError):
                    amount_rub = 0.0
                if ptype == "steam":
                    account = (purchase_meta.get("login") or "").strip()
                    amount_steam = purchase_meta.get("amount_steam") or purchase_meta.get("amount") or amount_rub
                    try:
                        amount_steam = float(amount_steam)
                    except (TypeError, ValueError):
                        amount_steam = amount_rub
                    steam_notify_chat_id = int(os.getenv("STEAM_NOTIFY_CHAT_ID", "0") or "0")
                    notify_lines = [
                        "💻 Новый заказ пополнения Steam (Platega)",
                        "",
                        f"👤 Аккаунт Steam: <code>{account or '—'}</code>",
                        f"💰 Сумма на кошелёк Steam: <b>{amount_steam:.0f} ₽</b>",
                        f"💵 Оплачено: <b>{amount_rub:.2f} ₽</b>",
                        f"🧾 Platega transaction_id: <code>{platega_tid}</code>",
                    ]
                    funpay_url = os.getenv("FUNPAY_STEAM_URL", "").strip()
                    if funpay_url:
                        notify_lines.append("")
                        notify_lines.append(f"🛒 Лот / профиль FunPay: {funpay_url}")
                    notify_text = "\n".join(notify_lines)
                    if steam_notify_chat_id:
                        try:
                            await bot.send_message(
                                chat_id=steam_notify_chat_id,
                                text=notify_text,
                                parse_mode="HTML",
                                disable_web_page_preview=True,
                            )
                            try:
                                await _apply_referral_earnings_for_purchase(
                                    user_id=user_id,
                                    amount_rub=amount_rub,
                                    username=purchase_meta.get("username") or "",
                                    first_name=purchase_meta.get("first_name") or "",
                                )
                            except Exception as ref_err:
                                logger.warning("Platega payment_check: referral error: %s", ref_err)
                        except Exception as send_err:
                            logger.warning("Platega Steam notify failed: %s", send_err)
                    order_meta["delivered"] = True
                    try:
                        po = request.app.get("platega_orders")
                        if isinstance(po, dict):
                            po[platega_tid] = order_meta
                    except Exception:
                        pass
                    _save_platega_order_to_file(platega_tid, order_meta)
                elif ptype == "stars":
                    recipient = (purchase_meta.get("login") or "").strip().lstrip("@")
                    stars_amount = int(purchase_meta.get("stars_amount") or 0)
                    if recipient and stars_amount >= 50 and TON_WALLET_ENABLED:
                        try:
                            _, recipient_address = await _fragment_get_recipient_address(recipient)
                            req_id = await _fragment_init_buy(recipient_address, stars_amount)
                            tx_address, amount_nanoton, payload_b64 = await _fragment_get_buy_link(req_id)
                            payload_decoded = _fragment_encoded(payload_b64)
                            tx_hash, send_err = await _ton_wallet_send_safe(tx_address, amount_nanoton, payload_decoded)
                            if tx_hash:
                                order_meta["delivered"] = True
                                try:
                                    po = request.app.get("platega_orders")
                                    if isinstance(po, dict):
                                        po[platega_tid] = order_meta
                                except Exception:
                                    pass
                                _save_platega_order_to_file(platega_tid, order_meta)
                                try:
                                    import db as _db
                                    order_id_custom = str(purchase_meta.get("order_id") or "").strip() or None
                                    if _db.is_enabled():
                                        await _db.user_upsert(user_id, purchase_meta.get("username") or "", purchase_meta.get("first_name") or "")
                                        await _db.purchase_add(user_id, amount_rub, stars_amount, "stars", f"{stars_amount} звёзд", order_id_custom)
                                    await _apply_referral_earnings_for_purchase(
                                        user_id=user_id,
                                        amount_rub=amount_rub,
                                        username=purchase_meta.get("username") or "",
                                        first_name=purchase_meta.get("first_name") or "",
                                    )
                                except Exception as record_err:
                                    logger.warning("Platega payment_check stars record: %s", record_err)
                                return _json_response({"paid": True, "transaction_id": platega_tid})
                        except Exception as e:
                            logger.exception("Platega payment_check stars delivery: %s", e)
                elif ptype == "premium":
                    try:
                        await _apply_referral_earnings_for_purchase(
                            user_id=user_id,
                            amount_rub=amount_rub,
                            username=purchase_meta.get("username") or "",
                            first_name=purchase_meta.get("first_name") or "",
                        )
                    except Exception as ref_err:
                        logger.warning("Platega payment_check referral: %s", ref_err)
                    order_meta["delivered"] = True
                    try:
                        po = request.app.get("platega_orders")
                        if isinstance(po, dict):
                            po[platega_tid] = order_meta
                    except Exception:
                        pass
                    _save_platega_order_to_file(platega_tid, order_meta)
                return _json_response({"paid": True, "transaction_id": platega_tid})
            except Exception as e:
                logger.warning("Platega GET transaction status failed for %s: %s", platega_tid, e)
                return _json_response({"paid": False})
        
        # Fragment.com (site): пробуем проверить статус по order_id (req_id)
        # Важно: пытаемся проверять даже если сервер перезапускался и meta не сохранилось.
        if method != "cryptobot" and is_stars and order_id and FRAGMENT_SITE_ENABLED:
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
        if method != "cryptobot" and method == "ton" and order_id and _ton_addr and TONAPI_KEY:
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
        # Platega: проверка по transaction_id (поллинг с фронта или после редиректа)
        if method == "platega" and transaction_id and PLATEGA_MERCHANT_ID and PLATEGA_SECRET:
            orders_pl = request.app.get("platega_orders") or {}
            if not isinstance(orders_pl, dict):
                orders_pl = {}
            order_meta = orders_pl.get(str(transaction_id))
            if not order_meta:
                order_meta = _load_platega_order_from_file(str(transaction_id))
                if order_meta and isinstance(orders_pl, dict):
                    orders_pl[str(transaction_id)] = order_meta
            if order_meta and order_meta.get("delivered"):
                return _json_response({"paid": True, "transaction_id": transaction_id})
            # Опционально: уточняем статус у Platega
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"{PLATEGA_BASE_URL}/transaction/{transaction_id}",
                        headers={"X-MerchantId": PLATEGA_MERCHANT_ID, "X-Secret": PLATEGA_SECRET},
                    ) as resp:
                        if resp.status == 200:
                            pdata = await resp.json(content_type=None) if resp.content_type else {}
                            if str(pdata.get("status")).upper() == "CONFIRMED":
                                if not order_meta:
                                    order_meta = _load_platega_order_from_file(str(transaction_id))
                                    if order_meta:
                                        orders_pl[str(transaction_id)] = order_meta
                                if order_meta and not order_meta.get("delivered"):
                                    # Выдача товара (то же, что в callback)
                                    await _deliver_platega_order(
                                        request.app, order_meta, str(transaction_id),
                                        _save_platega_order_to_file, bot,
                                    )
                                return _json_response({"paid": True, "transaction_id": transaction_id})
            except Exception as e:
                logger.warning("Platega getTransaction status check failed for %s: %s", transaction_id, e)
            return _json_response({"paid": False, "transaction_id": transaction_id})
        # CryptoBot: проверка по invoice_id (единственный обязательный параметр)
        if method == "cryptobot" and invoice_id and CRYPTO_PAY_TOKEN:
            try:
                # Метаданные инвойса, сохранённые при создании
                order_meta = None
                try:
                    orders = request.app.get("cryptobot_orders")
                    if isinstance(orders, dict):
                        order_meta = orders.get(str(invoice_id))
                except Exception as meta_err:
                    logger.warning("cryptobot order meta read failed for %s: %s", invoice_id, meta_err)
                
                # После перезапуска метаданные могут быть только в файле
                if not order_meta:
                    try:
                        order_meta = _load_cryptobot_order_from_file(str(invoice_id))
                        if order_meta and isinstance(orders, dict):
                            orders[str(invoice_id)] = order_meta
                    except Exception as meta_file_err:
                        logger.warning("cryptobot order meta file read failed for %s: %s", invoice_id, meta_file_err)

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

                                    # Покупка через CryptoBot: для Steam отправляем задачу в группу даже без webhook
                                    if order_meta and order_meta.get("context") == "purchase" and not order_meta.get("delivered"):
                                        purchase_meta = order_meta.get("purchase") or {}
                                        ptype = (purchase_meta.get("type") or "").strip().lower()

                                        if ptype == "steam":
                                            account = (purchase_meta.get("login") or "").strip()
                                            amount_steam = purchase_meta.get("amount_steam")
                                            if amount_steam is None:
                                                amount_steam = purchase_meta.get("amount") or amount_rub
                                            try:
                                                amount_steam = float(amount_steam)
                                            except (TypeError, ValueError):
                                                amount_steam = float(amount_rub or 0)

                                            funpay_url = os.getenv("FUNPAY_STEAM_URL", "").strip()
                                            steam_notify_chat_id = int(os.getenv("STEAM_NOTIFY_CHAT_ID", "0") or "0")
                                            notify_lines = [
                                                "💻 Новый заказ пополнения Steam (FunPay)",
                                                "",
                                                f"👤 Аккаунт Steam: <code>{account or '—'}</code>",
                                                f"💰 Сумма на кошелёк Steam: <b>{amount_steam:.0f} ₽</b>",
                                                f"💵 Оплачено: <b>{float(amount_rub or 0):.2f} ₽</b>",
                                                f"🧾 CryptoBot invoice_id: <code>{invoice_id}</code>",
                                            ]
                                            if funpay_url:
                                                notify_lines.append("")
                                                notify_lines.append(f"🛒 Лот / профиль FunPay: {funpay_url}")
                                                notify_lines.append("➡️ Оформите пополнение через FunPay‑бота для этого аккаунта Steam.")
                                            notify_text = "\n".join(notify_lines)

                                            if steam_notify_chat_id:
                                                try:
                                                    await bot.send_message(
                                                        chat_id=steam_notify_chat_id,
                                                        text=notify_text,
                                                        parse_mode="HTML",
                                                        disable_web_page_preview=True,
                                                    )
                                                    # Начисляем реферальный процент за покупку Steam,
                                                    # если известно user_id и сумма в рублях.
                                                    try:
                                                        uid = str(order_meta.get("user_id") or "").strip()
                                                        if uid and amount_rub:
                                                            await _apply_referral_earnings_for_purchase(
                                                                user_id=uid,
                                                                amount_rub=amount_rub,
                                                                username=purchase_meta.get("username") or "",
                                                                first_name=purchase_meta.get("first_name") or "",
                                                            )
                                                    except Exception as ref_err:
                                                        logger.warning(f"Failed to apply referral earnings for Steam (payment_check): {ref_err}")

                                                    order_meta["delivered"] = True
                                                    try:
                                                        orders = request.app.get("cryptobot_orders")
                                                        if isinstance(orders, dict):
                                                            orders[str(invoice_id)]["delivered"] = True
                                                    except Exception:
                                                        pass
                                                    _save_cryptobot_order_to_file(str(invoice_id), order_meta)
                                                except Exception as send_err:
                                                    logger.warning(
                                                        "Failed to send Steam FunPay notify (fallback via payment/check) to chat %s: %s",
                                                        steam_notify_chat_id,
                                                        send_err,
                                                    )
                                            else:
                                                logger.warning(
                                                    "STEAM_NOTIFY_CHAT_ID not set; Steam FunPay task will not be sent (fallback via payment/check). Text:\n%s",
                                                    notify_text,
                                                )

                                        elif ptype == "stars":
                                            # Fallback для звёзд: если webhook не сработал,
                                            # запускаем выдачу звёзд через Fragment прямо из /api/payment/check.
                                            recipient = (purchase_meta.get("login") or "").strip().lstrip("@")
                                            stars_amount = int(purchase_meta.get("stars_amount") or 0)
                                            if recipient and stars_amount >= 50 and TON_WALLET_ENABLED:
                                                try:
                                                    _, recipient_address = await _fragment_get_recipient_address(recipient)
                                                    req_id = await _fragment_init_buy(recipient_address, stars_amount)
                                                    tx_address, amount_nanoton, payload_b64 = await _fragment_get_buy_link(req_id)
                                                    payload_decoded = _fragment_encoded(payload_b64)
                                                    tx_hash, send_err = await _ton_wallet_send_safe(tx_address, amount_nanoton, payload_decoded)
                                                    if tx_hash:
                                                        logger.info(
                                                            "CryptoBot payment_check: stars delivered via Fragment, invoice_id=%s, recipient=%s, stars=%s, tx=%s",
                                                            invoice_id,
                                                            recipient,
                                                            stars_amount,
                                                            tx_hash,
                                                        )
                                                        order_meta["delivered"] = True
                                                        try:
                                                            orders = request.app.get("cryptobot_orders")
                                                            if isinstance(orders, dict):
                                                                orders[str(invoice_id)]["delivered"] = True
                                                        except Exception:
                                                            pass
                                                        _save_cryptobot_order_to_file(str(invoice_id), order_meta)
                                                        # Записываем покупку в рейтинг (как в вебхуке)
                                                        try:
                                                            user_id = str(order_meta.get("user_id") or "unknown")
                                                            purchase_type_str = "stars"
                                                            product_name = f"{stars_amount} звёзд"
                                                            order_id_custom = str(purchase_meta.get("order_id") or "").strip() or None
                                                            import db as _db
                                                            if _db.is_enabled():
                                                                await _db.user_upsert(
                                                                    user_id,
                                                                    purchase_meta.get("username") or "",
                                                                    purchase_meta.get("first_name") or "",
                                                                )
                                                                await _db.purchase_add(
                                                                    user_id,
                                                                    amount_rub,
                                                                    stars_amount,
                                                                    purchase_type_str,
                                                                    product_name,
                                                                    order_id_custom,
                                                                )
                                                            else:
                                                                # Fallback на JSON файл
                                                                path = _get_users_data_path()
                                                                users_data = _read_json_file(path) or {}
                                                                if user_id not in users_data:
                                                                    users_data[user_id] = {
                                                                        "id": int(user_id) if user_id.isdigit() else user_id,
                                                                        "username": purchase_meta.get("username") or "",
                                                                        "first_name": purchase_meta.get("first_name") or "",
                                                                        "purchases": [],
                                                                    }
                                                                u = users_data[user_id]
                                                                if "purchases" not in u:
                                                                    u["purchases"] = []
                                                                u["purchases"].append({
                                                                    "stars_amount": stars_amount,
                                                                    "amount": amount_rub,
                                                                    "type": purchase_type_str,
                                                                    "productName": product_name,
                                                                    "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                                                })
                                                                try:
                                                                    with open(path, "w", encoding="utf-8") as f:
                                                                        json.dump(users_data, f, ensure_ascii=False, indent=2)
                                                                except Exception as file_err:
                                                                    logger.warning(f"Failed to write users_data.json (payment_check): {file_err}")
                                                            
                                                            # Начисление рефералов
                                                            try:
                                                                await _apply_referral_earnings_for_purchase(
                                                                    user_id=user_id,
                                                                    amount_rub=amount_rub,
                                                                    username=purchase_meta.get("username") or "",
                                                                    first_name=purchase_meta.get("first_name") or "",
                                                                )
                                                            except Exception as ref_err:
                                                                logger.warning(f"Failed to update referral earnings (payment_check): {ref_err}")
                                                            
                                                            logger.info(
                                                                "CryptoBot payment_check: purchase recorded, invoice_id=%s, user_id=%s, amount_rub=%s",
                                                                invoice_id,
                                                                user_id,
                                                                amount_rub,
                                                            )
                                                        except Exception as record_err:
                                                            logger.exception(f"CryptoBot payment_check: error recording purchase for invoice_id={invoice_id}: {record_err}")
                                                    else:
                                                        logger.error(
                                                            "CryptoBot payment_check: failed to deliver stars, invoice_id=%s, error=%s",
                                                            invoice_id,
                                                            send_err,
                                                        )
                                                except Exception as star_err:
                                                    logger.exception(
                                                        "CryptoBot payment_check: error delivering stars for invoice_id=%s: %s",
                                                        invoice_id,
                                                        star_err,
                                                    )

                                        elif ptype == "spin":
                                            order_meta["delivered"] = True
                                            try:
                                                orders = request.app.get("cryptobot_orders")
                                                if isinstance(orders, dict):
                                                    orders[str(invoice_id)]["delivered"] = True
                                            except Exception:
                                                pass
                                            _save_cryptobot_order_to_file(str(invoice_id), order_meta)
                                            try:
                                                await _apply_referral_earnings_for_purchase(
                                                    user_id=str(order_meta.get("user_id") or "unknown"),
                                                    amount_rub=float(order_meta.get("amount_rub") or 100),
                                                    username=purchase_meta.get("username") or "",
                                                    first_name=purchase_meta.get("first_name") or "",
                                                )
                                            except Exception as ref_err:
                                                logger.warning("Failed to apply referral earnings (payment_check spin): %s", ref_err)
                                            logger.info("CryptoBot payment_check: spin delivered, invoice_id=%s", invoice_id)

                                    return _json_response(response_data)
            except Exception as e:
                logger.warning(f"Crypto Pay check invoice {invoice_id}: {e}")
        
        # CryptoBot: если дошли сюда, значит invoice_id не найден или не оплачен
        if method == "cryptobot":
            return _json_response({"paid": False})
        
        # Fragment.com (site flow): если не нашли заказ в памяти — подтвердить оплату не можем.
        if method != "cryptobot":
            purchase = body.get("purchase") or {}
            purchase_type = (purchase.get("type") or purchase.get("Type") or "").strip()
            is_stars = purchase_type == "stars" or (purchase.get("stars_amount") is not None and purchase.get("stars_amount") != 0)
            is_premium = purchase_type == "premium" or (purchase.get("months") is not None and purchase.get("months") != 0)
            order_id = (body.get("order_id") or body.get("orderId") or "").strip()
        if is_stars or is_premium:
            return _json_response({"paid": False, "order_id": order_id or None})
        
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

    # /api/fragment/deliver-stars УДАЛЁН. Выдача звёзд — ТОЛЬКО через webhooks платёжек
    # (CryptoBot webhook, Platega callback, FreeKassa notify). Публичный эндпоинт позволил бы
    # любому выдать себе звёзды без оплаты.

    # Создание заказа Fragment: при наличии TON-кошелька — только валидация (оплата CryptoBot → deliver-stars). Иначе — ссылка на оплату TON.
    async def fragment_create_star_order_handler(request):
        """
        Создать заказ на звёзды: возвращает order_id и payment_url (если API отдаёт), фронт открывает ссылку оплаты TonKeeper.
        ВАЖНО: для работы мини‑приложения используется метод POST. Для GET просто возвращаем информацию,
        чтобы при открытии URL в браузере не было ошибки Method Not Allowed.
        """
        if request.method != "POST":
            # Чтобы не пугать 405, просто отвечаем, какой метод поддерживается.
            return _json_response(
                {
                    "ok": True,
                    "method": request.method,
                    "message": "Используйте POST с JSON из мини‑приложения для создания заказа звёзд."
                }
            )
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
            # ВСЕГДА используем режим fragment.com site: создаём заказ на стороне Fragment
            # и отдаём TonKeeper‑ссылку. Fragment сам доставляет звёзды после оплаты.
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

    # Принимаем ЛЮБЫЕ методы, чтобы не было "Method Not Allowed" при открытии URL в браузере.
    # ВАЖНО: не регистрируем отдельный OPTIONS, потому что "*" уже включает все методы.
    app.router.add_route("*", "/api/fragment/create-star-order", fragment_create_star_order_handler)
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
        ptype = ""

        amount: float
        description: str
        payload_data: str
        use_usdt = False  # пока создаём инвойсы только в RUB, USDT-логику можно добавить отдельно
        deposit_amount_rub = None

        # ----------- Покупка (звёзды / премиум / Steam) -----------
        # ВАЖНО: клиент НЕ задаёт цену и payload. Только type + минимальные данные.
        # Цена считается на бэке из stars_amount * курс (из БД или файла)
        if context == "purchase":
            purchase = body.get("purchase") or {}
            ptype = (purchase.get("type") or "").strip().lower()
            if not _validate_user_id(user_id):
                return _json_response({"error": "bad_request", "message": "Некорректный user_id"}, status=400)
            rates_db = {}
            db_enabled = False
            try:
                import db as _db_pg
                db_enabled = _db_pg.is_enabled()
                if db_enabled:
                    rates_db = await _db_pg.rates_get()
                    logger.info(f"CryptoBot: rates from DB: {rates_db}")
            except Exception as e:
                logger.warning(f"CryptoBot rates_get error: {e}")
            star_from_db = rates_db.get("star_price_rub")
            steam_from_db = rates_db.get("steam_rate_rub")
            _star = float(star_from_db) if star_from_db is not None and float(star_from_db) > 0 else _get_star_price_rub()
            _steam = float(steam_from_db) if steam_from_db is not None and float(steam_from_db) > 0 else _get_steam_rate_rub()
            _premium = {
                3: float(rates_db.get("premium_3")) if rates_db.get("premium_3") is not None and float(rates_db.get("premium_3", 0)) > 0 else PREMIUM_PRICES_RUB.get(3, 983),
                6: float(rates_db.get("premium_6")) if rates_db.get("premium_6") is not None and float(rates_db.get("premium_6", 0)) > 0 else PREMIUM_PRICES_RUB.get(6, 1311),
                12: float(rates_db.get("premium_12")) if rates_db.get("premium_12") is not None and float(rates_db.get("premium_12", 0)) > 0 else PREMIUM_PRICES_RUB.get(12, 2377)
            }
            logger.info(f"CryptoBot create-invoice: ptype={ptype}, DB enabled={db_enabled}, star_rate={_star}, steam_rate={_steam}")
            # Игнорируем amount, price, payload от клиента — всё считаем на бэке
            if ptype == "stars" and (purchase.get("amount") is not None or purchase.get("price") is not None):
                return _json_response(
                    {"error": "bad_request", "message": "Для звёзд передавайте только stars_amount и login. Цена рассчитывается на сервере."},
                    status=400,
                )

            if ptype == "stars":
                try:
                    stars_amount = int(purchase.get("stars_amount") or purchase.get("starsAmount") or 0)
                except (TypeError, ValueError):
                    stars_amount = 0
                login_val, login_err = _validate_login(purchase.get("login") or "", "Получатель")
                if login_err:
                    return _json_response({"error": "bad_request", "message": login_err}, status=400)
                stars_err = _validate_stars_amount(stars_amount)
                if stars_err:
                    return _json_response({"error": "bad_request", "message": stars_err}, status=400)
                amount = round(stars_amount * _star, 2)
                if amount < 1:
                    amount = 1.0
                description = f"Звёзды Telegram — {stars_amount} шт. для @{login_val}"
                login = login_val
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
                if months not in VALIDATION_LIMITS["premium_months"]:
                    return _json_response(
                        {"error": "bad_request", "message": "Premium: допустимые периоды 3, 6 или 12 мес."}, status=400
                    )
                if months not in _premium:
                    return _json_response(
                        {"error": "bad_request", "message": "Неверная длительность Premium"}, status=400
                    )
                amount = float(_premium[months])
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
            elif ptype == "steam":
                # Покупка пополнения Steam: клиент передаёт amount_steam (рубли на Steam)
                try:
                    amount_steam = float(purchase.get("amount_steam") or purchase.get("amount") or 0)
                except (TypeError, ValueError):
                    amount_steam = 0.0
                login_val, login_err = _validate_login(purchase.get("login") or "", "Логин Steam")
                if login_err:
                    return _json_response({"error": "bad_request", "message": login_err}, status=400)
                steam_err = _validate_steam_amount(amount_steam)
                if steam_err:
                    return _json_response({"error": "bad_request", "message": steam_err}, status=400)
                amount_rub = round(amount_steam * _steam, 2)
                rub_err = _validate_amount_rub(amount_rub)
                if rub_err:
                    return _json_response({"error": "bad_request", "message": rub_err}, status=400)
                login = login_val
                amount = float(amount_rub)
                description = f"Пополнение Steam для {login} на {amount_steam:.0f} ₽ (к оплате {amount_rub:.2f} ₽)"
                payload_data = json.dumps(
                    {
                        "context": "purchase",
                        "type": "steam",
                        "user_id": user_id,
                        "login": login,
                        "amount_steam": amount_steam,
                        "amount_rub": amount_rub,
                        "timestamp": time.time(),
                    },
                    ensure_ascii=False,
                )[:4096]
            elif ptype == "spin":
                amount = 1.5  # 1.5 USDT
                description = "1 спин рулетки — 1.5 USDT"
                payload_data = json.dumps(
                    {
                        "context": "purchase",
                        "type": "spin",
                        "user_id": user_id,
                        "amount_usdt": 1.5,
                        "timestamp": time.time(),
                    },
                    ensure_ascii=False,
                )[:4096]
            else:
                return _json_response(
                    {"error": "bad_request", "message": "Поддерживаются только покупки звёзд, Premium, Steam и спин рулетки"}, status=400
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
            deposit_amount_rub = round(amount, 2)
            payload_data = json.dumps(
                {
                    "context": "deposit",
                    "user_id": user_id,
                    "amount_rub": deposit_amount_rub,
                    "timestamp": time.time(),
                },
                ensure_ascii=False,
            )[:4096]

        # Комиссия CryptoBot 4% — сумма к оплате увеличивается (для spin в USDT тоже)
        CRYPTOBOT_COMMISSION_PERCENT = 4.0
        amount = round(amount * (1 + CRYPTOBOT_COMMISSION_PERCENT / 100), 2)

        # ----------- Общие поля инвойса -----------
        paid_btn_url = WEB_APP_URL or "https://jetstoreapp.ru"
        try:
            me = await bot.get_me()
            if me and getattr(me, "username", None):
                paid_btn_url = f"https://t.me/{me.username}/app"
        except Exception:
            pass

        # Spin в USDT: currency_type crypto, иначе fiat RUB
        if ptype == "spin":
            payload_obj = {
                "currency_type": "crypto",
                "asset": "USDT",
                "amount": f"{amount:.2f}",
                "description": description[:1024],
                "payload": payload_data,
                "paid_btn_name": "callback",
                "paid_btn_url": paid_btn_url,
            }
        else:
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
                        if context == "deposit" and deposit_amount_rub is not None:
                            amt_rub = deposit_amount_rub
                        elif context == "purchase" and (purchase or {}).get("type") == "spin":
                            amt_rub = 100.0
                        else:
                            amt_rub = float(amount)
                        order_meta = {
                                "context": context,
                                "user_id": user_id,
                                "amount_rub": amt_rub,
                                "purchase": purchase if context == "purchase" else None,
                                "created_at": time.time(),
                                "delivered": False,
                            }
                        orders = request.app.get("cryptobot_orders")
                        if isinstance(orders, dict) and invoice_id:
                            orders[str(invoice_id)] = order_meta
                        if invoice_id:
                            _save_cryptobot_order_to_file(str(invoice_id), order_meta)
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

    async def cryptobot_webhook_handler(request):
        """
        Webhook для CryptoBot: автоматическая выдача товаров и запись покупок при оплате инвойса.
        
        CryptoBot отправляет POST запросы с данными об изменении статуса инвойса.
        Формат: { "update_id": int, "update_type": "invoice_paid", "request": { "invoice_id": int, ... } }
        """
        # CryptoBot может делать тестовые запросы разными методами.
        # Для всего, что не POST, просто отвечаем 200 OK, чтобы не было "Method Not Allowed".
        if request.method != "POST":
            return _json_response({"ok": True, "method": request.method})

        if not CRYPTO_PAY_TOKEN:
            return _json_response({"error": "not_configured"}, status=503)

        # Дополнительная защита: проверяем подпись CryptoBot (если заголовок присутствует).
        # Согласно документации Crypto Pay, подпись считается как HMAC-SHA256 от "сырых" данных запроса,
        # используя CRYPTO_PAY_TOKEN в качестве секрета, и передаётся в заголовке Crypto-Pay-Signature.
        try:
            raw_body = await request.read()
        except Exception:
            raw_body = b""

        signature = request.headers.get("Crypto-Pay-Signature") or request.headers.get("crypto-pay-signature")
        if signature and raw_body:
            try:
                mac = hmac.new(CRYPTO_PAY_TOKEN.encode("utf-8"), msg=raw_body, digestmod=hashlib.sha256)
                expected = mac.hexdigest()
                if not hmac.compare_digest(expected, str(signature).strip()):
                    logger.warning("CryptoBot webhook: invalid signature, invoice ignored")
                    return _json_response({"error": "forbidden", "message": "Invalid signature"}, status=403)
            except Exception as sig_err:
                logger.warning(f"CryptoBot webhook: signature check error: {sig_err}")

        # После проверки подписи парсим JSON
        try:
            body = json.loads(raw_body.decode("utf-8")) if raw_body else await request.json()
        except Exception:
            return _json_response({"error": "bad_request", "message": "Invalid JSON"}, status=400)
        
        update_type = body.get("update_type") or ""
        request_data = body.get("request") or body
        invoice_id = request_data.get("invoice_id") or body.get("invoice_id")
        if invoice_id is not None:
            invoice_id = int(invoice_id) if isinstance(invoice_id, (int, float)) else invoice_id
        
        # Обрабатываем только событие оплаты инвойса
        if update_type != "invoice_paid" or not invoice_id:
            return _json_response({"ok": True, "message": "ignored"})
        
        try:
            # Получаем метаданные инвойса (память или файл — после перезапуска заказы только в файле)
            orders = request.app.get("cryptobot_orders")
            order_meta = None
            if isinstance(orders, dict):
                order_meta = orders.get(str(invoice_id))
            if not order_meta:
                order_meta = _load_cryptobot_order_from_file(str(invoice_id))
                if order_meta and isinstance(orders, dict):
                    orders[str(invoice_id)] = order_meta
            if not order_meta:
                logger.warning(f"CryptoBot webhook: order_meta not found for invoice_id={invoice_id}")
                return _json_response({"ok": True, "message": "order_meta_not_found"})
            
            # Проверяем, что товар ещё не был выдан
            if order_meta.get("delivered"):
                logger.info(f"CryptoBot webhook: invoice_id={invoice_id} already delivered, skipping")
                return _json_response({"ok": True, "message": "already_delivered"})
            
            context = order_meta.get("context")
            purchase = order_meta.get("purchase") or {}
            user_id = str(order_meta.get("user_id") or "unknown")
            try:
                amount_rub = float(order_meta.get("amount_rub") or 0.0)
            except (TypeError, ValueError):
                amount_rub = 0.0
            
            # Выдача товара в зависимости от типа покупки
            if context == "purchase":
                purchase_type = purchase.get("type") or ""
                
                if purchase_type == "stars":
                    # Выдача звёзд через Fragment
                    recipient = (purchase.get("login") or "").strip().lstrip("@")
                    stars_amount = int(purchase.get("stars_amount") or 0)
                    
                    if recipient and stars_amount >= 50:
                        try:
                            # Вызываем внутреннюю функцию выдачи звёзд
                            if TON_WALLET_ENABLED:
                                _, recipient_address = await _fragment_get_recipient_address(recipient)
                                req_id = await _fragment_init_buy(recipient_address, stars_amount)
                                tx_address, amount_nanoton, payload_b64 = await _fragment_get_buy_link(req_id)
                                payload_decoded = _fragment_encoded(payload_b64)
                                tx_hash, send_err = await _ton_wallet_send_safe(tx_address, amount_nanoton, payload_decoded)
                                if tx_hash:
                                    logger.info(f"CryptoBot webhook: stars delivered via Fragment, invoice_id={invoice_id}, recipient={recipient}, stars={stars_amount}, tx={tx_hash}")
                                    order_meta["delivered"] = True
                                    if isinstance(orders, dict):
                                        orders[str(invoice_id)]["delivered"] = True
                                    _save_cryptobot_order_to_file(str(invoice_id), order_meta)
                                    
                                    # Записываем покупку в базу данных (рейтинг) + начисляем рефералы
                                    try:
                                        import db as _db
                                        purchase_type_str = "stars"
                                        product_name = purchase.get("productName") or purchase.get("product_name") or f"{stars_amount} звёзд"
                                        order_id_custom = str(purchase.get("order_id") or "").strip() or None
                                        
                                        if _db.is_enabled():
                                            await _db.user_upsert(user_id, purchase.get("username") or "", purchase.get("first_name") or "")
                                            await _db.purchase_add(user_id, amount_rub, stars_amount, purchase_type_str, product_name, order_id_custom)
                                        else:
                                            # Fallback на JSON файл
                                            path = _get_users_data_path()
                                            users_data = _read_json_file(path) or {}
                                            if user_id not in users_data:
                                                users_data[user_id] = {
                                                    "id": int(user_id) if user_id.isdigit() else user_id,
                                                    "username": purchase.get("username") or "",
                                                    "first_name": purchase.get("first_name") or "",
                                                    "purchases": [],
                                                }
                                            u = users_data[user_id]
                                            if "purchases" not in u:
                                                u["purchases"] = []
                                            u["purchases"].append({
                                                "stars_amount": stars_amount,
                                                "amount": amount_rub,
                                                "type": purchase_type_str,
                                                "productName": product_name,
                                                "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                            })
                                            try:
                                                with open(path, "w", encoding="utf-8") as f:
                                                    json.dump(users_data, f, ensure_ascii=False, indent=2)
                                            except Exception as file_err:
                                                logger.warning(f"Failed to write users_data.json (webhook stars): {file_err}")
                                        
                                        # Начисление рефералов
                                        try:
                                            await _apply_referral_earnings_for_purchase(
                                                user_id=user_id,
                                                amount_rub=amount_rub,
                                                username=purchase.get("username") or "",
                                                first_name=purchase.get("first_name") or "",
                                            )
                                        except Exception as ref_err:
                                            logger.warning(f"Failed to update referral earnings (webhook stars): {ref_err}")
                                        
                                        logger.info(f"CryptoBot webhook: stars purchase recorded, invoice_id={invoice_id}, user_id={user_id}, amount_rub={amount_rub}")
                                    except Exception as record_err:
                                        logger.exception(f"CryptoBot webhook: error recording stars purchase for invoice_id={invoice_id}: {record_err}")
                                else:
                                    logger.error(f"CryptoBot webhook: failed to deliver stars, invoice_id={invoice_id}, error={send_err}")
                        except Exception as e:
                            logger.exception(f"CryptoBot webhook: error delivering stars for invoice_id={invoice_id}: {e}")
                
                elif purchase_type == "premium":
                    # Автоматическая выдача Telegram Premium через Fragment (как со звёздами)
                    recipient = (purchase.get("login") or "").strip().lstrip("@")
                    months = int(purchase.get("months") or 0)
                    logger.info(
                        "CryptoBot webhook: premium purchase detected, invoice_id=%s, recipient=%s, months=%s",
                        invoice_id,
                        recipient,
                        months,
                    )

                    delivered_ok = False
                    if recipient and months in (3, 6, 12):
                        try:
                            if FRAGMENT_SITE_ENABLED and TON_WALLET_ENABLED:
                                api = await _get_fragment_api_client()
                                # Покупаем/дарим Premium получателю через Fragment
                                result = await api.gift_premium(recipient, months, show_sender=False)
                                if getattr(result, "success", False):
                                    tx_hash = getattr(result, "transaction_hash", None)
                                    logger.info(
                                        "CryptoBot webhook: premium delivered via Fragment, invoice_id=%s, recipient=%s, months=%s, tx=%s",
                                        invoice_id,
                                        recipient,
                                        months,
                                        tx_hash,
                                    )
                                    delivered_ok = True
                                else:
                                    logger.error(
                                        "CryptoBot webhook: gift_premium failed, invoice_id=%s, recipient=%s, months=%s, error=%s",
                                        invoice_id,
                                        recipient,
                                        months,
                                        getattr(result, "error", None),
                                    )
                            else:
                                logger.warning(
                                    "CryptoBot webhook: Fragment Premium not configured (FRAGMENT_SITE_ENABLED=%s, TON_WALLET_ENABLED=%s)",
                                    FRAGMENT_SITE_ENABLED,
                                    TON_WALLET_ENABLED,
                                )
                        except Exception as e:
                            logger.exception(
                                "CryptoBot webhook: error delivering premium via Fragment, invoice_id=%s, recipient=%s, months=%s: %s",
                                invoice_id,
                                recipient,
                                months,
                                e,
                            )
                    else:
                        logger.warning(
                            "CryptoBot webhook: invalid premium params, invoice_id=%s, recipient=%s, months=%s",
                            invoice_id,
                            recipient,
                            months,
                        )

                    if delivered_ok:
                        # Фиксируем, что премиум выдан, но в рейтинг НЕ добавляем (рейтинг только за звёзды)
                        order_meta["delivered"] = True
                        if isinstance(orders, dict):
                            orders[str(invoice_id)]["delivered"] = True
                        _save_cryptobot_order_to_file(str(invoice_id), order_meta)

                        # Начисление реферальных бонусов за покупку премиума (без влияния на рейтинг)
                        try:
                            await _apply_referral_earnings_for_purchase(
                                user_id=user_id,
                                amount_rub=amount_rub,
                                username=purchase.get("username") or "",
                                first_name=purchase.get("first_name") or "",
                            )
                        except Exception as ref_err:
                            logger.warning(f"Failed to update referral earnings (webhook premium): {ref_err}")

                        logger.info(
                            "CryptoBot webhook: premium delivered and referral updated, invoice_id=%s, user_id=%s, amount_rub=%s",
                            invoice_id,
                            user_id,
                            amount_rub,
                        )
                
                elif purchase_type == "spin":
                    order_meta["delivered"] = True
                    if isinstance(orders, dict):
                        orders[str(invoice_id)]["delivered"] = True
                    _save_cryptobot_order_to_file(str(invoice_id), order_meta)
                    amount_rub_spin = float(order_meta.get("amount_rub") or 100)  # 1 USDT ≈ 100 RUB
                    try:
                        await _apply_referral_earnings_for_purchase(
                            user_id=user_id,
                            amount_rub=amount_rub_spin,
                            username=purchase.get("username") or "",
                            first_name=purchase.get("first_name") or "",
                        )
                    except Exception as ref_err:
                        logger.warning(f"Failed to update referral earnings (webhook spin): {ref_err}")
                    logger.info("CryptoBot webhook: spin delivered, invoice_id=%s, user_id=%s", invoice_id, user_id)
                
                elif purchase_type == "steam":
                    # Пополнение Steam: выдача через FunPay‑бота (отдельный сервис)
                    account = (purchase.get("login") or "").strip()
                    amount_steam = purchase.get("amount_steam")
                    if amount_steam is None:
                        amount_steam = purchase.get("amount") or amount_rub
                    try:
                        amount_steam = float(amount_steam)
                    except (TypeError, ValueError):
                        amount_steam = amount_rub
                    logger.info(
                        "CryptoBot webhook: steam purchase detected, invoice_id=%s, account=%s, amount_steam=%.2f, amount_rub=%.2f",
                        invoice_id,
                        account,
                        amount_steam,
                        amount_rub,
                    )

                    # Настройки FunPay/уведомлений:
                    # - FUNPAY_STEAM_URL: ссылка на ваш лот/профиль FunPay для покупки пополнения Steam
                    # - STEAM_NOTIFY_CHAT_ID: ID чата/канала, куда слать задания вашему FunPay‑боту
                    funpay_url = os.getenv("FUNPAY_STEAM_URL", "").strip()
                    steam_notify_chat_id = int(os.getenv("STEAM_NOTIFY_CHAT_ID", "0") or "0")

                    # Формируем текст задачи: FunPay‑бот смотрит этот чат и сам создаёт/обрабатывает заказы.
                    notify_lines = [
                        "💻 Новый заказ пополнения Steam (FunPay)",
                        "",
                        f"👤 Аккаунт Steam: <code>{account or '—'}</code>",
                        f"💰 Сумма на кошелёк Steam: <b>{amount_steam:.0f} ₽</b>",
                        f"💵 Оплачено: <b>{amount_rub:.2f} ₽</b>",
                        f"🧾 CryptoBot invoice_id: <code>{invoice_id}</code>",
                    ]
                    if funpay_url:
                        notify_lines.append("")
                        notify_lines.append(f"🛒 Лот / профиль FunPay: {funpay_url}")
                        notify_lines.append("➡️ Оформите пополнение через FunPay‑бота для этого аккаунта Steam.")
                    notify_text = "\n".join(notify_lines)

                    if steam_notify_chat_id:
                        try:
                            await bot.send_message(
                                chat_id=steam_notify_chat_id,
                                text=notify_text,
                                parse_mode="HTML",
                                disable_web_page_preview=True,
                            )
                            if isinstance(orders, dict):
                                orders[str(invoice_id)]["delivered"] = True
                            order_meta["delivered"] = True
                            _save_cryptobot_order_to_file(str(invoice_id), order_meta)
                        except Exception as send_err:
                            logger.warning(
                                "Failed to send Steam FunPay notify to chat %s: %s",
                                steam_notify_chat_id,
                                send_err,
                            )
                    else:
                        logger.warning(
                            "STEAM_NOTIFY_CHAT_ID not set; Steam FunPay task will not be sent. "
                            "Set STEAM_NOTIFY_CHAT_ID in Railway (e.g. your Telegram chat ID). Text:\n%s",
                            notify_text,
                        )
                    
                    # Начисление рефералов за покупку Steam (без влияния на рейтинг)
                    try:
                        await _apply_referral_earnings_for_purchase(
                            user_id=user_id,
                            amount_rub=amount_rub,
                            username=purchase.get("username") or "",
                            first_name=purchase.get("first_name") or "",
                        )
                    except Exception as ref_err:
                        logger.warning(f"Failed to update referral earnings (webhook steam): {ref_err}")
                    
                    logger.info(
                        "CryptoBot webhook: steam purchase processed and referral updated, invoice_id=%s, user_id=%s, amount_rub=%s",
                        invoice_id,
                        user_id,
                        amount_rub,
                    )
            elif context == "deposit":
                import db as _db_dep
                if _db_dep.is_enabled() and amount_rub > 0:
                    await _db_dep.balance_add_rub(user_id, amount_rub)
                    await _db_dep.user_upsert(
                        user_id,
                        (order_meta.get("purchase") or {}).get("username") or "",
                        (order_meta.get("purchase") or {}).get("first_name") or "",
                    )
                    await _db_dep.purchase_add(
                        user_id, amount_rub, 0, "balance",
                        f"Пополнение баланса на {amount_rub:.0f} ₽",
                        None,
                    )
                order_meta["delivered"] = True
                if isinstance(orders, dict):
                    orders[str(invoice_id)] = order_meta
                _save_cryptobot_order_to_file(str(invoice_id), order_meta)
                logger.info("CryptoBot webhook: balance deposit delivered, invoice_id=%s, user_id=%s, amount_rub=%s", invoice_id, user_id, amount_rub)
            
            return _json_response({"ok": True, "message": "processed"})
            
        except Exception as e:
            invoice_id_str = str(invoice_id) if invoice_id else "unknown"
            logger.exception(f"CryptoBot webhook error for invoice_id={invoice_id_str}: {e}")
            return _json_response({"error": "internal_error", "message": str(e)}, status=500)

    # Platega.io: создание транзакции (карты / СБП)
    async def platega_create_transaction_handler(request):
        """POST /api/platega/create-transaction — создаёт транзакцию в Platega, возвращает redirect URL."""
        if not PLATEGA_MERCHANT_ID or not PLATEGA_SECRET:
            return _json_response(
                {"error": "not_configured", "message": "PLATEGA_MERCHANT_ID и PLATEGA_SECRET не заданы."},
                status=503,
            )
        try:
            body = await request.json()
        except Exception:
            return _json_response({"error": "bad_request", "message": "Invalid JSON"}, status=400)
        context = (body.get("context") or "").strip() or "purchase"
        user_id = str(body.get("user_id") or body.get("userId") or "").strip() or "unknown"
        purchase = body.get("purchase") or {}
        if context != "purchase":
            return _json_response({"error": "bad_request", "message": "Только context=purchase поддерживается"}, status=400)
        if not _validate_user_id(user_id):
            return _json_response({"error": "bad_request", "message": "Некорректный user_id"}, status=400)
        ptype = (purchase.get("type") or "").strip()
        amount = 0.0
        description = ""
        rates_db = {}
        db_enabled = False
        try:
            import db as _db_pg
            db_enabled = _db_pg.is_enabled()
            if db_enabled:
                rates_db = await _db_pg.rates_get()
                logger.info(f"Platega: rates from DB: {rates_db}")
        except Exception as e:
            logger.warning(f"Platega rates_get error: {e}")
        star_from_db = rates_db.get("star_price_rub")
        steam_from_db = rates_db.get("steam_rate_rub")
        _star = float(star_from_db) if star_from_db is not None and float(star_from_db) > 0 else _get_star_price_rub()
        _steam = float(steam_from_db) if steam_from_db is not None and float(steam_from_db) > 0 else _get_steam_rate_rub()
        _premium = {
            3: float(rates_db.get("premium_3")) if rates_db.get("premium_3") is not None and float(rates_db.get("premium_3", 0)) > 0 else PREMIUM_PRICES_RUB.get(3, 983),
            6: float(rates_db.get("premium_6")) if rates_db.get("premium_6") is not None and float(rates_db.get("premium_6", 0)) > 0 else PREMIUM_PRICES_RUB.get(6, 1311),
            12: float(rates_db.get("premium_12")) if rates_db.get("premium_12") is not None and float(rates_db.get("premium_12", 0)) > 0 else PREMIUM_PRICES_RUB.get(12, 2377)
        }
        logger.info(f"Platega create-transaction: ptype={ptype}, DB enabled={db_enabled}, star_rate={_star}, steam_rate={_steam}")
        if ptype == "stars":
            try:
                stars_amount = int(purchase.get("stars_amount") or purchase.get("starsAmount") or 0)
            except (TypeError, ValueError):
                stars_amount = 0
            login_val, login_err = _validate_login(purchase.get("login") or "", "Получатель")
            if login_err:
                return _json_response({"error": "bad_request", "message": login_err}, status=400)
            stars_err = _validate_stars_amount(stars_amount)
            if stars_err:
                return _json_response({"error": "bad_request", "message": stars_err}, status=400)
            amount = round(stars_amount * _star, 2)
            if amount < 1:
                amount = 1.0
            purchase["login"] = login_val
            description = f"Звёзды Telegram — {stars_amount} шт. для @{login_val}"
        elif ptype == "premium":
            months = int(purchase.get("months") or 0)
            if months not in VALIDATION_LIMITS["premium_months"]:
                return _json_response({"error": "bad_request", "message": "Premium: допустимые периоды 3, 6 или 12 мес."}, status=400)
            if months not in _premium:
                return _json_response({"error": "bad_request", "message": "Неверная длительность Premium"}, status=400)
            amount = float(_premium[months])
            description = f"Telegram Premium — {months} мес."
        elif ptype == "steam":
            try:
                amount_steam = float(purchase.get("amount_steam") or purchase.get("amount") or 0)
            except (TypeError, ValueError):
                amount_steam = 0.0
            login_val, login_err = _validate_login(purchase.get("login") or "", "Логин Steam")
            if login_err:
                return _json_response({"error": "bad_request", "message": login_err}, status=400)
            steam_err = _validate_steam_amount(amount_steam)
            if steam_err:
                return _json_response({"error": "bad_request", "message": steam_err}, status=400)
            amount_rub = round(amount_steam * _steam, 2)
            rub_err = _validate_amount_rub(amount_rub)
            if rub_err:
                return _json_response({"error": "bad_request", "message": rub_err}, status=400)
            amount = float(amount_rub)
            purchase["login"] = login_val
            description = f"Пополнение Steam для {login_val} на {amount_steam:.0f} ₽ (к оплате {amount:.2f} ₽)"
        else:
            return _json_response({"error": "bad_request", "message": "Поддерживаются только звёзды, Premium и Steam"}, status=400)
        payment_method_int = int(body.get("platega_method") or body.get("payment_method") or 10)
        if payment_method_int not in (2, 10):
            payment_method_int = 10
        # Комиссия Platega: СБП (2) и Карты (10) — из админки / env
        commission_pct = _get_platega_sbp_commission() if payment_method_int == 2 else _get_platega_cards_commission()
        amount = round(amount * (1 + commission_pct / 100), 2)
        base_url = (WEB_APP_URL or "https://jetstoreapp.ru").rstrip("/")
        return_url = body.get("return_url") or f"{base_url}?platega=success"
        failed_url = body.get("failed_url") or f"{base_url}?platega=fail"
        payload_str = json.dumps({"order_id": purchase.get("order_id"), "user_id": user_id}, ensure_ascii=False)[:2048]
        use_kopecks = (os.getenv("PLATEGA_AMOUNT_IN_KOPECKS", "0") or "0").strip().lower() in ("1", "true", "yes")
        if use_kopecks:
            amount_send = max(1, int(round(amount * 100)))
        else:
            amount_send = round(amount, 2)
        # API Platega требует PascalCase: PaymentDetails (обязательно), PaymentMethod, Description, Return, FailedUrl, Payload
        platega_body = {
            "PaymentMethod": payment_method_int,
            "PaymentDetails": {"Amount": amount_send, "Currency": "RUB"},
            "Description": description[:1024],
            "Return": return_url,
            "FailedUrl": failed_url,
            "Payload": payload_str,
        }
        headers = {
            "Content-Type": "application/json",
            "X-MerchantId": PLATEGA_MERCHANT_ID,
            "X-Secret": PLATEGA_SECRET,
        }
        logger.info("Platega create-transaction: PaymentMethod=%s, amount_send=%s", payment_method_int, amount_send)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{PLATEGA_BASE_URL}/transaction/process",
                    headers=headers,
                    json=platega_body,
                ) as resp:
                    text = await resp.text()
                    try:
                        data = json.loads(text) if text else {}
                    except json.JSONDecodeError:
                        logger.warning("Platega create-transaction: response not JSON, status=%s, body=%s", resp.status, text[:300])
                        return _json_response({"error": "platega_error", "message": f"Ответ не JSON: {text[:200]}"}, status=502)
                    if resp.status != 200:
                        err = data.get("error") or data.get("message") or data.get("detail") or text[:300]
                        if isinstance(data.get("errors"), dict):
                            err_parts = [f"{k}: {v}" for k, v in data["errors"].items()]
                            if err_parts:
                                err = "; ".join(str(x) for x in err_parts)
                        logger.warning("Platega create-transaction failed: status=%s, body=%s", resp.status, text[:500])
                        return _json_response({"error": "platega_error", "message": str(err)}, status=502)
                    transaction_id = data.get("transactionId") or data.get("transaction_id")
                    redirect_url = data.get("redirect") or data.get("payment_url") or ""
                    if not transaction_id or not redirect_url:
                        return _json_response({"error": "platega_error", "message": "Нет transactionId или redirect в ответе"}, status=502)
                    order_meta = {
                        "context": context,
                        "user_id": user_id,
                        "amount_rub": float(amount),
                        "purchase": dict(purchase),
                        "order_id": purchase.get("order_id"),
                        "created_at": time.time(),
                        "delivered": False,
                    }
                    try:
                        po = request.app.get("platega_orders")
                        if isinstance(po, dict):
                            po[str(transaction_id)] = order_meta
                    except Exception:
                        pass
                    _save_platega_order_to_file(str(transaction_id), order_meta)
                    logger.info("Platega transaction created: transaction_id=%s, amount=%s", transaction_id, amount)
                    return _json_response({
                        "success": True,
                        "transaction_id": transaction_id,
                        "redirect": redirect_url,
                    })
        except aiohttp.ClientError as e:
            logger.error("Platega create transaction network error: %s", e)
            return _json_response({"error": "network_error", "message": str(e)}, status=502)
        except Exception as e:
            logger.exception("Platega create transaction error: %s", e)
            return _json_response({"error": "internal_error", "message": str(e)}, status=500)

    # Platega.io: callback при изменении статуса транзакции
    async def platega_callback_handler(request):
        """POST от Platega при смене статуса. Проверяем X-MerchantId, X-Secret; при CONFIRMED — выдача товара."""
        if request.method == "OPTIONS":
            return Response(status=204, headers=_cors_headers())
        if request.method != "POST":
            return web.Response(status=200, text="OK")
        merchant_id = request.headers.get("X-MerchantId") or request.headers.get("x-merchantid")
        secret = request.headers.get("X-Secret") or request.headers.get("x-secret")
        if not merchant_id or not secret or merchant_id != PLATEGA_MERCHANT_ID or secret != PLATEGA_SECRET:
            logger.warning("Platega callback: invalid or missing X-MerchantId / X-Secret")
            return web.Response(status=403, text="Forbidden")
        try:
            body = await request.json()
        except Exception:
            return web.Response(status=400, text="Bad Request")
        tid = body.get("id")
        status_val = (body.get("status") or "").strip().upper()
        if not tid:
            return web.Response(status=200, text="OK")
        if status_val != "CONFIRMED":
            return web.Response(status=200, text="OK")
        order_meta = None
        try:
            orders = request.app.get("platega_orders")
            if isinstance(orders, dict):
                order_meta = orders.get(str(tid))
            if not order_meta:
                order_meta = _load_platega_order_from_file(str(tid))
                if order_meta and isinstance(orders, dict):
                    orders[str(tid)] = order_meta
        except Exception as e:
            logger.warning("Platega callback: load order failed for %s: %s", tid, e)
        if not order_meta:
            logger.warning("Platega callback: order_meta not found for transaction_id=%s", tid)
            return web.Response(status=200, text="OK")
        if order_meta.get("delivered"):
            return web.Response(status=200, text="OK")
        purchase = order_meta.get("purchase") or {}
        ptype = (purchase.get("type") or "").strip().lower()
        user_id = str(order_meta.get("user_id") or "unknown")
        try:
            amount_rub = float(order_meta.get("amount_rub") or 0.0)
        except (TypeError, ValueError):
            amount_rub = 0.0
        try:
            if ptype == "stars":
                recipient = (purchase.get("login") or "").strip().lstrip("@")
                stars_amount = int(purchase.get("stars_amount") or 0)
                if recipient and stars_amount >= 50 and TON_WALLET_ENABLED:
                    _, recipient_address = await _fragment_get_recipient_address(recipient)
                    req_id = await _fragment_init_buy(recipient_address, stars_amount)
                    tx_address, amount_nanoton, payload_b64 = await _fragment_get_buy_link(req_id)
                    payload_decoded = _fragment_encoded(payload_b64)
                    tx_hash, send_err = await _ton_wallet_send_safe(tx_address, amount_nanoton, payload_decoded)
                    if tx_hash:
                        order_meta["delivered"] = True
                        try:
                            po = request.app.get("platega_orders")
                            if isinstance(po, dict):
                                po[str(tid)] = order_meta
                        except Exception:
                            pass
                        _save_platega_order_to_file(str(tid), order_meta)
                        import db as _db
                        order_id_custom = str(purchase.get("order_id") or "").strip() or None
                        if _db.is_enabled():
                            await _db.user_upsert(user_id, purchase.get("username") or "", purchase.get("first_name") or "")
                            await _db.purchase_add(user_id, amount_rub, stars_amount, "stars", f"{stars_amount} звёзд", order_id_custom)
                        await _apply_referral_earnings_for_purchase(
                            user_id=user_id, amount_rub=amount_rub,
                            username=purchase.get("username") or "", first_name=purchase.get("first_name") or "",
                        )
                        logger.info("Platega callback: stars delivered, transaction_id=%s", tid)
            elif ptype == "premium":
                order_meta["delivered"] = True
                try:
                    po = request.app.get("platega_orders")
                    if isinstance(po, dict):
                        po[str(tid)] = order_meta
                except Exception:
                    pass
                _save_platega_order_to_file(str(tid), order_meta)
                await _apply_referral_earnings_for_purchase(
                    user_id=user_id, amount_rub=amount_rub,
                    username=purchase.get("username") or "", first_name=purchase.get("first_name") or "",
                )
                logger.info("Platega callback: premium recorded, transaction_id=%s", tid)
            elif ptype == "steam":
                account = (purchase.get("login") or "").strip()
                amount_steam = purchase.get("amount_steam") or purchase.get("amount") or amount_rub
                try:
                    amount_steam = float(amount_steam)
                except (TypeError, ValueError):
                    amount_steam = amount_rub
                steam_notify_chat_id = int(os.getenv("STEAM_NOTIFY_CHAT_ID", "0") or "0")
                notify_lines = [
                    "💻 Новый заказ Steam (Platega)",
                    "",
                    f"👤 Аккаунт Steam: <code>{account or '—'}</code>",
                    f"💰 Сумма на кошелёк Steam: <b>{amount_steam:.0f} ₽</b>",
                    f"💵 Оплачено: <b>{amount_rub:.2f} ₽</b>",
                    f"🧾 Platega transaction_id: <code>{tid}</code>",
                ]
                funpay_url = os.getenv("FUNPAY_STEAM_URL", "").strip()
                if funpay_url:
                    notify_lines.append("")
                    notify_lines.append(f"🛒 Лот FunPay: {funpay_url}")
                notify_text = "\n".join(notify_lines)
                if steam_notify_chat_id:
                    await bot.send_message(
                        chat_id=steam_notify_chat_id,
                        text=notify_text,
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                    )
                await _apply_referral_earnings_for_purchase(
                    user_id=user_id, amount_rub=amount_rub,
                    username=purchase.get("username") or "", first_name=purchase.get("first_name") or "",
                )
                order_meta["delivered"] = True
                try:
                    po = request.app.get("platega_orders")
                    if isinstance(po, dict):
                        po[str(tid)] = order_meta
                except Exception:
                    pass
                _save_platega_order_to_file(str(tid), order_meta)
                logger.info("Platega callback: steam processed, transaction_id=%s", tid)
        except Exception as e:
            logger.exception("Platega callback delivery error for %s: %s", tid, e)
        return web.Response(status=200, text="OK")

    # FreeKassa: создание заказа (СБП / карты) и получение ссылки на оплату
    async def freekassa_create_order_handler(request):
        """POST /api/freekassa/create-order — создаёт заказ в FreeKassa и возвращает ссылку на оплату."""
        if not FREEKASSA_SHOP_ID or not FREEKASSA_API_KEY:
            return _json_response(
                {"error": "not_configured", "message": "FREEKASSA_SHOP_ID и FREEKASSA_API_KEY не заданы."},
                status=503,
            )
        try:
            body = await request.json()
        except Exception:
            return _json_response({"error": "bad_request", "message": "Invalid JSON"}, status=400)

        context = (body.get("context") or "").strip() or "purchase"
        user_id = str(body.get("user_id") or body.get("userId") or "").strip() or "unknown"
        purchase = body.get("purchase") or {}
        method = (body.get("method") or "").strip().lower()
        fk_i = int(body.get("i") or 0)

        # Защита: валидация user_id
        if not _validate_user_id(user_id):
            return _json_response({"error": "bad_request", "message": "Некорректный user_id"}, status=400)

        if context != "purchase":
            return _json_response({"error": "bad_request", "message": "Только context=purchase поддерживается"}, status=400)

        if method not in ("sbp", "card"):
            return _json_response({"error": "bad_request", "message": "Допустимые методы: sbp, card"}, status=400)

        ptype = (purchase.get("type") or "").strip()
        amount = 0.0
        description = ""

        # Курсы из БД (если PostgreSQL подключён) или из файла/env
        rates_db = {}
        db_enabled = False
        try:
            import db as _db_pg
            db_enabled = _db_pg.is_enabled()
            logger.info(f"FreeKassa: DB enabled={db_enabled}")
            if db_enabled:
                rates_db = await _db_pg.rates_get()
                logger.info(f"FreeKassa: rates from DB: {rates_db}")
            else:
                logger.warning("FreeKassa: PostgreSQL not enabled, using file/env rates")
        except Exception as e:
            logger.error(f"FreeKassa rates_get error: {e}", exc_info=True)
        
        # Используем курс из БД, если есть и > 0, иначе fallback на файл/env
        star_from_db = rates_db.get("star_price_rub")
        steam_from_db = rates_db.get("steam_rate_rub")
        star_fallback = _get_star_price_rub()
        steam_fallback = _get_steam_rate_rub()
        
        _star = float(star_from_db) if star_from_db is not None and float(star_from_db) > 0 else star_fallback
        _steam = float(steam_from_db) if steam_from_db is not None and float(steam_from_db) > 0 else steam_fallback
        
        if star_from_db is not None:
            logger.info(f"FreeKassa: Using star_rate from DB: {star_from_db} (fallback was: {star_fallback})")
        else:
            logger.warning(f"FreeKassa: No star_rate in DB, using fallback: {star_fallback}")
        
        if steam_from_db is not None:
            logger.info(f"FreeKassa: Using steam_rate from DB: {steam_from_db} (fallback was: {steam_fallback})")
        else:
            logger.warning(f"FreeKassa: No steam_rate in DB, using fallback: {steam_fallback}")
        
        _premium = {
            3: float(rates_db.get("premium_3")) if rates_db.get("premium_3") is not None and float(rates_db.get("premium_3", 0)) > 0 else PREMIUM_PRICES_RUB.get(3, 983),
            6: float(rates_db.get("premium_6")) if rates_db.get("premium_6") is not None and float(rates_db.get("premium_6", 0)) > 0 else PREMIUM_PRICES_RUB.get(6, 1311),
            12: float(rates_db.get("premium_12")) if rates_db.get("premium_12") is not None and float(rates_db.get("premium_12", 0)) > 0 else PREMIUM_PRICES_RUB.get(12, 2377)
        }
        logger.info(f"FreeKassa create-order: ptype={ptype}, DB enabled={db_enabled}, final star_rate={_star}, final steam_rate={_steam}")

        if ptype == "stars":
            try:
                stars_amount = int(purchase.get("stars_amount") or purchase.get("starsAmount") or 0)
            except (TypeError, ValueError):
                stars_amount = 0
            login_val, login_err = _validate_login(purchase.get("login") or "", "Получатель")
            if login_err:
                return _json_response({"error": "bad_request", "message": login_err}, status=400)
            stars_err = _validate_stars_amount(stars_amount)
            if stars_err:
                return _json_response({"error": "bad_request", "message": stars_err}, status=400)
            amount = round(stars_amount * _star, 2)
            if amount < 1:
                amount = 1.0
            purchase["login"] = login_val
            description = f"Звёзды Telegram — {stars_amount} шт. для @{login_val}"
        elif ptype == "premium":
            months = int(purchase.get("months") or 0)
            if months not in VALIDATION_LIMITS["premium_months"]:
                return _json_response({"error": "bad_request", "message": "Premium: допустимые периоды 3, 6 или 12 мес."}, status=400)
            if months not in _premium:
                return _json_response({"error": "bad_request", "message": "Неверная длительность Premium"}, status=400)
            amount = float(_premium[months])
            description = f"Telegram Premium — {months} мес."
        elif ptype == "steam":
            try:
                amount_steam = float(purchase.get("amount_steam") or purchase.get("amount") or 0)
            except (TypeError, ValueError):
                amount_steam = 0.0
            login_val, login_err = _validate_login(purchase.get("login") or "", "Логин Steam")
            if login_err:
                return _json_response({"error": "bad_request", "message": login_err}, status=400)
            steam_err = _validate_steam_amount(amount_steam)
            if steam_err:
                return _json_response({"error": "bad_request", "message": steam_err}, status=400)
            amount_rub = round(amount_steam * _steam, 2)
            rub_err = _validate_amount_rub(amount_rub)
            if rub_err:
                return _json_response({"error": "bad_request", "message": rub_err}, status=400)
            amount = float(amount_rub)
            purchase["login"] = login_val
            description = f"Пополнение Steam для {login_val} на {amount_steam:.0f} ₽ (к оплате {amount:.2f} ₽)"
        elif ptype == "spin":
            amount = 100.0
            description = "1 спин рулетки — 100 ₽"
        elif ptype == "balance":
            try:
                amount = float(purchase.get("amount") or 0)
            except (TypeError, ValueError):
                amount = 0.0
            if amount < 100:
                return _json_response({"error": "bad_request", "message": "Минимальная сумма пополнения баланса 100 ₽"}, status=400)
            if amount > 100_000:
                return _json_response({"error": "bad_request", "message": "Максимальная сумма пополнения 100 000 ₽"}, status=400)
            amount = round(amount, 2)
            description = f"Пополнение баланса JET Store на {amount:.0f} ₽"
        else:
            return _json_response({"error": "bad_request", "message": "Поддерживаются только звёзды, Premium, Steam, спин рулетки и пополнение баланса"}, status=400)

        # FreeKassa сама добавляет комиссию (СБП ~5%, карты ~6%) — в amount передаём чистую сумму без надбавки
        if fk_i not in (36, 44, 43):
            # 44 — СБП (QR), 36 — карты РФ, 43 — SberPay
            fk_i = 44 if method == "sbp" else 36

        # Наш order_id (MERCHANT_ORDER_ID / paymentId) — используем уже сгенерированный в мини‑аппе
        payment_id_raw, oid_err = _validate_order_id(str(purchase.get("order_id") or ""))
        if oid_err:
            return _json_response({"error": "bad_request", "message": oid_err}, status=400)
        # Убираем # из начала, если есть, и оставляем только буквы, цифры, дефисы и подчёркивания
        payment_id = payment_id_raw.lstrip("#").strip()
        # Убираем все недопустимые символы (оставляем только буквы, цифры, дефисы, подчёркивания)
        payment_id = re.sub(r'[^a-zA-Z0-9_-]', '', payment_id)
        if not payment_id:
            # Если после очистки ничего не осталось, генерируем новый ID из исходного
            payment_id = payment_id_raw.lstrip("#").replace("#", "").replace(" ", "").replace("-", "_")
            if not payment_id:
                import time as _time
                payment_id = f"order_{user_id}_{int(_time.time())}"

        # Email: реальный email клиента или Telegram ID в виде tgid@telegram.org
        email = ""
        try:
            uid_int = int(str(user_id))
            email = f"{uid_int}@telegram.org"
        except Exception:
            email = f"{user_id or 'client'}@telegram.org"

        # IP: реальный IP клиента (если доступен) или IP сервера (но не 127.0.0.1)
        ip = _get_client_ip(request)
        if ip.startswith("127.") or ip in ("::1", ""):
            ip = os.getenv("FREEKASSA_FALLBACK_IP", "8.8.8.8")

        import time as _time
        import hmac
        import hashlib

        try:
            shop_id_int = int(FREEKASSA_SHOP_ID)
        except Exception:
            return _json_response({"error": "bad_config", "message": "FREEKASSA_SHOP_ID должен быть числом"}, status=503)

        # Формируем данные для запроса
        # ВАЖНО: amount должен быть числом с десятичными знаками для RUB, i должно быть числом
        amount_value = round(float(amount), 2)
        if amount_value < 0.01:
            amount_value = 0.01
        nonce_value = int((_time.time() + 10800) * 1000)
        
        # Валидация данных перед отправкой
        if not email or "@" not in email:
            logger.error("FreeKassa: invalid email: %s", email)
            return _json_response({"error": "bad_request", "message": "Некорректный email"}, status=400)
        if not ip or len(ip.strip()) == 0:
            logger.error("FreeKassa: invalid IP: %s", ip)
            return _json_response({"error": "bad_request", "message": "Некорректный IP"}, status=400)
        if not payment_id or len(payment_id.strip()) == 0:
            logger.error("FreeKassa: invalid paymentId: %s", payment_id)
            return _json_response({"error": "bad_request", "message": "Некорректный paymentId"}, status=400)
        
        data = {
            "shopId": shop_id_int,
            "nonce": nonce_value,
            "paymentId": str(payment_id).strip(),  # Убеждаемся, что это строка
            "i": int(fk_i),
            "email": str(email).strip(),
            "ip": str(ip).strip(),
            "amount": float(amount_value),  # Число с десятичными знаками
            "currency": "RUB",
        }
        
        logger.info("FreeKassa data validation: shopId=%d, paymentId=%s, email=%s, ip=%s, amount=%.2f, i=%d", 
                   shop_id_int, payment_id, email, ip, amount_value, fk_i)
        
        # Проверяем, что API ключ не пустой
        if not FREEKASSA_API_KEY or len(FREEKASSA_API_KEY.strip()) == 0:
            logger.error("FreeKassa API_KEY is empty!")
            return _json_response({"error": "bad_config", "message": "FREEKASSA_API_KEY не задан"}, status=503)

        # Подпись: HMAC-SHA256 от отсортированных значений (ключи по алфавиту, значения через |)
        # По документации: сортируем по ключам в алфавитном порядке, конкатенируем значения через |
        # Хешируем sha256 используя API_KEY (НЕ SECRET1!)
        # ВАЖНО: для подписи НЕ включаем поле "signature"
        # Форматируем значения: числа без лишних нулей (212.0 -> "212"), строки как есть
        def format_sign_value(v):
            if isinstance(v, float):
                # Убираем лишние нули: 212.0 -> "212", 212.5 -> "212.5"
                if v == int(v):
                    return str(int(v))
                return str(v)
            return str(v)
        
        items = sorted(data.items(), key=lambda kv: kv[0])
        sign_source = "|".join(format_sign_value(v) for _, v in items)
        
        # По документации FreeKassa API используется именно API_KEY для подписи
        if not FREEKASSA_API_KEY or len(FREEKASSA_API_KEY.strip()) < 10:
            logger.error(f"FreeKassa API_KEY invalid: length={len(FREEKASSA_API_KEY) if FREEKASSA_API_KEY else 0}")
            return _json_response({"error": "bad_config", "message": "FREEKASSA_API_KEY должен быть задан"}, status=503)
        
        sign = hmac.new(FREEKASSA_API_KEY.encode("utf-8"), sign_source.encode("utf-8"), hashlib.sha256).hexdigest()
        data["signature"] = sign
        
        logger.info("FreeKassa: Using API_KEY for signature (length=%d)", len(FREEKASSA_API_KEY))

        logger.info("FreeKassa create-order: paymentId=%s, i=%s, amount=%.2f, shopId=%s", payment_id, fk_i, amount_value, shop_id_int)
        logger.info("FreeKassa sign_source (sorted values joined by |): %s", sign_source)
        logger.info("FreeKassa signature (HMAC-SHA256): %s", sign)
        logger.debug("FreeKassa full data (without signature): %s", {k: v for k, v in data.items() if k != "signature"})

        try:
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                # По документации: POST запрос с JSON телом на https://api.fk.life/v1/orders/create
                url = "https://api.fk.life/v1/orders/create"
                logger.info("FreeKassa: Sending POST request to %s", url)
                logger.info("FreeKassa request data (without signature): %s", json.dumps({k: v for k, v in data.items() if k != "signature"}, ensure_ascii=False))
                logger.debug("FreeKassa request data (full): %s", json.dumps(data, ensure_ascii=False))
                try:
                    async with session.post(url, json=data, headers={"Content-Type": "application/json"}) as resp:
                        text = await resp.text()
                        logger.info("FreeKassa response: status=%d, headers=%s", resp.status, dict(resp.headers))
                        logger.debug("FreeKassa response body: %s", text[:500])
                        try:
                            resp_data = json.loads(text) if text else {}
                        except json.JSONDecodeError:
                            logger.error("FreeKassa create-order: response not JSON, status=%s, body=%s", resp.status, text[:500])
                            return _json_response({"error": "freekassa_error", "message": f"Ответ не JSON: {text[:200]}"}, status=502)
                        if resp.status != 200 or (resp_data.get("type") or "").lower() != "success":
                            err = resp_data.get("message") or resp_data.get("error") or text[:300]
                            logger.error("FreeKassa create-order failed: status=%d, type=%s, message=%s, full_response=%s", 
                                       resp.status, resp_data.get("type"), err, text[:1000])
                            return _json_response({"error": "freekassa_error", "message": str(err)}, status=502)
                        
                        # Успешный ответ от FreeKassa
                        order_id_fk = resp_data.get("orderId")
                        location = resp_data.get("location") or ""
                        if not location:
                            # В некоторых реализациях URL может приходить как plain text
                            if isinstance(resp_data, str) and resp_data.startswith("http"):
                                location = resp_data
                        if not location:
                            logger.error("FreeKassa: no location in response: %s", resp_data)
                            return _json_response({"error": "freekassa_error", "message": "Не получена ссылка на оплату (location)"}, status=502)
                        
                        # Обработка успешного ответа
                        order_meta = {
                            "context": context,
                            "user_id": user_id,
                            "amount_rub": float(amount),
                            "purchase": dict(purchase),
                            "fk_order_id": order_id_fk,
                            "method": method,
                            "i": fk_i,
                            "created_at": _time.time(),
                            "delivered": False,
                            # Сохраняем оригинальный order_id с # для отображения пользователю
                            "original_order_id": payment_id_raw,
                        }
                        try:
                            orders_fk = request.app.get("freekassa_orders")
                            if isinstance(orders_fk, dict):
                                # Сохраняем с очищенным payment_id (без #) как ключ, чтобы вебхук мог найти
                                orders_fk[str(payment_id)] = order_meta
                        except Exception:
                            pass
                        _save_freekassa_order_to_file(str(payment_id), order_meta)
                        logger.info("FreeKassa order created: paymentId=%s (original=%s), fk_order_id=%s, amount=%s", payment_id, payment_id_raw, order_id_fk, amount)
                        return _json_response(
                            {
                                "success": True,
                                "order_id": payment_id_raw,  # Возвращаем оригинальный order_id с # для фронтенда
                                "fk_order_id": order_id_fk,
                                "payment_url": location,
                            }
                        )
                except asyncio.TimeoutError:
                    logger.error("FreeKassa create-order: timeout waiting for response")
                    return _json_response({"error": "freekassa_error", "message": "Таймаут при обращении к FreeKassa API"}, status=504)
                except aiohttp.ClientError as e:
                    logger.error("FreeKassa create-order: client error: %s", e, exc_info=True)
                    return _json_response({"error": "freekassa_error", "message": f"Ошибка сети: {str(e)}"}, status=502)
        except aiohttp.ClientError as e:
            logger.error("FreeKassa create-order network error: %s", e)
            return _json_response({"error": "network_error", "message": str(e)}, status=502)
        except Exception as e:
            logger.exception("FreeKassa create-order error: %s", e)
            return _json_response({"error": "internal_error", "message": str(e)}, status=500)

    # FreeKassa: webhook-уведомление об оплате
    async def freekassa_notify_handler(request):
        """
        URL оповещения FreeKassa.
        Проверяем подпись SIGN, после чего выдаём товар и отвечаем 'YES'. IP не проверяем.
        """
        if request.method == "POST":
            try:
                content_type = (request.headers.get("Content-Type") or "").lower()
                if "application/json" in content_type:
                    params = await request.json()
                    if not isinstance(params, dict):
                        params = {}
                else:
                    post_data = await request.post()
                    params = dict(post_data) if post_data else {}
            except Exception:
                params = {}
        else:
            params = dict(request.rel_url.query)

        merchant_id = str(params.get("MERCHANT_ID") or params.get("merchantId") or params.get("merchant_id") or "").strip()
        amount_str = str(params.get("AMOUNT") or params.get("amount") or params.get("sum") or "").strip()
        merchant_order_id = str(
            params.get("MERCHANT_ORDER_ID")
            or params.get("merchantOrderId")
            or params.get("merchant_order_id")
            or params.get("paymentId")
            or params.get("payment_id")
            or ""
        ).strip()
        sign_recv = str(params.get("SIGN") or params.get("sign") or params.get("signature") or "").strip().lower()

        if not (merchant_id and amount_str and merchant_order_id and sign_recv):
            logger.warning("FreeKassa notify: missing required params: %s", dict(params))
            return web.Response(status=400, text="bad request")

        if not FREEKASSA_SECRET2:
            logger.warning("FreeKassa notify: FREEKASSA_SECRET2 not configured")
            return web.Response(status=500, text="secret not configured")

        import hashlib as _hashlib

        sign_src = f"{merchant_id}:{amount_str}:{FREEKASSA_SECRET2}:{merchant_order_id}"
        expected_sign = _hashlib.md5(sign_src.encode("utf-8")).hexdigest().lower()
        if expected_sign != sign_recv:
            logger.warning(
                "FreeKassa notify: invalid SIGN for order %s (expected %s, got %s)",
                merchant_order_id,
                expected_sign,
                sign_recv,
            )
            return web.Response(status=400, text="wrong sign")

        # Ключ, под которым мы сохраняем заказ в памяти/файле (как в create-order и payment_check)
        import re as _re_notify
        payment_key = merchant_order_id.lstrip("#").strip()
        payment_key = _re_notify.sub(r"[^a-zA-Z0-9_-]", "", payment_key)
        if not payment_key:
            payment_key = merchant_order_id.lstrip("#").replace("#", "").replace(" ", "").replace("-", "_")

        order_meta = None
        try:
            orders_fk = request.app.get("freekassa_orders")
            if isinstance(orders_fk, dict):
                order_meta = orders_fk.get(str(payment_key)) or orders_fk.get(str(merchant_order_id))
            if not order_meta:
                order_meta = _load_freekassa_order_from_file(str(payment_key)) or _load_freekassa_order_from_file(str(merchant_order_id))
                if order_meta and isinstance(orders_fk, dict):
                    orders_fk[str(payment_key)] = order_meta
        except Exception as e:
            logger.warning("FreeKassa notify: load order failed for %s: %s", merchant_order_id, e)

        if not order_meta:
            logger.warning("FreeKassa notify: order_meta not found for MERCHANT_ORDER_ID=%s", merchant_order_id)
            logger.warning("FreeKassa notify: available orders in memory: %s", list((request.app.get("freekassa_orders") or {}).keys())[:10])
            return web.Response(status=200, text="YES")

        if order_meta.get("delivered"):
            logger.info("FreeKassa notify: order already delivered, MERCHANT_ORDER_ID=%s", merchant_order_id)
            return web.Response(status=200, text="YES")

        purchase = order_meta.get("purchase") or {}
        ptype = (purchase.get("type") or "").strip().lower()
        user_id = str(order_meta.get("user_id") or "unknown")
        logger.info("FreeKassa notify: processing order MERCHANT_ORDER_ID=%s, purchase_type=%s, user_id=%s", merchant_order_id, ptype, user_id)
        try:
            amount_rub = float(order_meta.get("amount_rub") or amount_str or 0.0)
        except (TypeError, ValueError):
            amount_rub = 0.0

        try:
            if ptype == "stars":
                recipient = (purchase.get("login") or "").strip().lstrip("@")
                stars_amount = int(purchase.get("stars_amount") or 0)
                use_ton_wallet = bool(recipient and stars_amount >= 50 and TON_WALLET_ENABLED)

                tx_hash = None
                send_err = None
                if use_ton_wallet:
                    _, recipient_address = await _fragment_get_recipient_address(recipient)
                    req_id = await _fragment_init_buy(recipient_address, stars_amount)
                    tx_address, amount_nanoton, payload_b64 = await _fragment_get_buy_link(req_id)
                    payload_decoded = _fragment_encoded(payload_b64)
                    tx_hash, send_err = await _ton_wallet_send_safe(tx_address, amount_nanoton, payload_decoded)
                    if not tx_hash:
                        logger.error(
                            "FreeKassa notify: stars delivery failed via TON wallet, MERCHANT_ORDER_ID=%s, recipient=%s, stars=%s, error=%s",
                            merchant_order_id, recipient, stars_amount, send_err or "unknown"
                        )

                # Даже если TON-кошелёк не сработал или отключён, не блокируем подтверждение оплаты:
                # считаем заказ доставленным для бэкенда и рефералки, а звёзды можно выдать вручную.
                order_meta["delivered"] = True
                try:
                    orders_fk = request.app.get("freekassa_orders")
                    if isinstance(orders_fk, dict):
                        orders_fk[str(merchant_order_id)] = order_meta
                except Exception:
                    pass
                _save_freekassa_order_to_file(str(merchant_order_id), order_meta)

                import db as _db
                order_id_custom = str(order_meta.get("original_order_id") or purchase.get("order_id") or "").strip() or None
                if _db.is_enabled():
                    await _db.user_upsert(
                        user_id,
                        purchase.get("username") or "",
                        purchase.get("first_name") or "",
                    )
                    await _db.purchase_add(
                        user_id,
                        amount_rub,
                        stars_amount,
                        "stars",
                        f"{stars_amount} звёзд",
                        order_id_custom,
                    )
                logger.info(
                    "FreeKassa notify: purchase_add recorded for stars order, MERCHANT_ORDER_ID=%s, user_id=%s, order_id=%s, stars=%s, tx_hash=%s",
                    merchant_order_id,
                    user_id,
                    order_id_custom,
                    stars_amount,
                    tx_hash,
                )
                await _apply_referral_earnings_for_purchase(
                    user_id=user_id,
                    amount_rub=amount_rub,
                    username=purchase.get("username") or "",
                    first_name=purchase.get("first_name") or "",
                )
                logger.info("FreeKassa notify: stars purchase marked as delivered, MERCHANT_ORDER_ID=%s", merchant_order_id)
            elif ptype == "premium":
                order_meta["delivered"] = True
                try:
                    orders_fk = request.app.get("freekassa_orders")
                    if isinstance(orders_fk, dict):
                        orders_fk[str(merchant_order_id)] = order_meta
                except Exception:
                    pass
                _save_freekassa_order_to_file(str(merchant_order_id), order_meta)
                await _apply_referral_earnings_for_purchase(
                    user_id=user_id,
                    amount_rub=amount_rub,
                    username=purchase.get("username") or "",
                    first_name=purchase.get("first_name") or "",
                )
                logger.info("FreeKassa notify: premium recorded, MERCHANT_ORDER_ID=%s", merchant_order_id)
            elif ptype == "spin":
                order_meta["delivered"] = True
                try:
                    orders_fk = request.app.get("freekassa_orders")
                    if isinstance(orders_fk, dict):
                        orders_fk[str(merchant_order_id)] = order_meta
                except Exception:
                    pass
                _save_freekassa_order_to_file(str(merchant_order_id), order_meta)
                await _apply_referral_earnings_for_purchase(
                    user_id=user_id,
                    amount_rub=amount_rub,
                    username=purchase.get("username") or "",
                    first_name=purchase.get("first_name") or "",
                )
                logger.info("FreeKassa notify: spin delivered, MERCHANT_ORDER_ID=%s", merchant_order_id)
            elif ptype == "balance":
                order_meta["delivered"] = True
                try:
                    orders_fk = request.app.get("freekassa_orders")
                    if isinstance(orders_fk, dict):
                        orders_fk[str(merchant_order_id)] = order_meta
                except Exception:
                    pass
                _save_freekassa_order_to_file(str(merchant_order_id), order_meta)
                import db as _db_bal
                if _db_bal.is_enabled() and amount_rub > 0:
                    await _db_bal.balance_add_rub(user_id, amount_rub)
                    await _db_bal.user_upsert(
                        user_id,
                        purchase.get("username") or "",
                        purchase.get("first_name") or "",
                    )
                    await _db_bal.purchase_add(
                        user_id, amount_rub, 0, "balance",
                        f"Пополнение баланса на {amount_rub:.0f} ₽",
                        purchase.get("order_id"),
                    )
                    logger.info("FreeKassa notify: balance deposit delivered, MERCHANT_ORDER_ID=%s, amount_rub=%s", merchant_order_id, amount_rub)
                else:
                    if amount_rub > 0 and not _db_bal.is_enabled():
                        logger.warning(
                            "FreeKassa notify: DATABASE_URL не задан — баланс НЕ зачислен в БД! "
                            "user_id=%s, amount_rub=%s. Задайте DATABASE_URL для работы баланса.",
                            user_id, amount_rub
                        )
                    logger.info("FreeKassa notify: balance deposit delivered, MERCHANT_ORDER_ID=%s, amount_rub=%s", merchant_order_id, amount_rub)
            elif ptype == "steam":
                account = (purchase.get("login") or "").strip()
                amount_steam = purchase.get("amount_steam") or purchase.get("amount") or amount_rub
                try:
                    amount_steam = float(amount_steam)
                except (TypeError, ValueError):
                    amount_steam = amount_rub
                steam_notify_chat_id = int(os.getenv("STEAM_NOTIFY_CHAT_ID", "0") or "0")
                notify_lines = [
                    "💻 Новый заказ Steam (FreeKassa)",
                    "",
                    f"👤 Аккаунт Steam: <code>{account or '—'}</code>",
                    f"💰 Сумма на кошелёк Steam: <b>{amount_steam:.0f} ₽</b>",
                    f"💵 Оплачено: <b>{amount_rub:.2f} ₽</b>",
                    f"🧾 FreeKassa MERCHANT_ORDER_ID: <code>{merchant_order_id}</code>",
                ]
                funpay_url = os.getenv("FUNPAY_STEAM_URL", "").strip()
                if funpay_url:
                    notify_lines.append("")
                    notify_lines.append(f"🛒 Лот FunPay: {funpay_url}")
                notify_text = "\n".join(notify_lines)
                if steam_notify_chat_id:
                    await bot.send_message(
                        chat_id=steam_notify_chat_id,
                        text=notify_text,
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                    )
                await _apply_referral_earnings_for_purchase(
                    user_id=user_id,
                    amount_rub=amount_rub,
                    username=purchase.get("username") or "",
                    first_name=purchase.get("first_name") or "",
                )
                order_meta["delivered"] = True
                try:
                    orders_fk = request.app.get("freekassa_orders")
                    if isinstance(orders_fk, dict):
                        orders_fk[str(merchant_order_id)] = order_meta
                except Exception:
                    pass
                _save_freekassa_order_to_file(str(merchant_order_id), order_meta)
                logger.info("FreeKassa notify: steam order recorded, MERCHANT_ORDER_ID=%s", merchant_order_id)
        except Exception as e:
            logger.exception("FreeKassa notify error for MERCHANT_ORDER_ID=%s: %s", merchant_order_id, e)

        return web.Response(status=200, text="YES")

    app.router.add_post("/api/cryptobot/create-invoice", cryptobot_create_invoice_handler)
    app.router.add_route("OPTIONS", "/api/cryptobot/create-invoice", lambda r: Response(status=204, headers=_cors_headers()))
    app.router.add_post("/api/cryptobot/check-invoice", cryptobot_check_invoice_handler)
    app.router.add_route("OPTIONS", "/api/cryptobot/check-invoice", lambda r: Response(status=204, headers=_cors_headers()))
    # Webhook CryptoBot: принимаем ЛЮБОЙ метод, чтобы не ловить 405 от тестов
    app.router.add_route("*", "/api/cryptobot/webhook", cryptobot_webhook_handler)

    # Platega.io: карты и СБП — OPTIONS регистрируем первым, чтобы CORS preflight не давал Not Found
    async def platega_create_options(request):
        return Response(status=204, headers=_cors_headers())
    app.router.add_route("OPTIONS", "/api/platega/create-transaction", platega_create_options)
    app.router.add_post("/api/platega/create-transaction", platega_create_transaction_handler)
    # Callback Platega: один маршрут на любой метод (как webhook CryptoBot), чтобы POST точно находился
    app.router.add_route("*", "/api/platega/callback", platega_callback_handler)

    # FreeKassa: API и вебхук
    app.router.add_post("/api/freekassa/create-order", freekassa_create_order_handler)
    app.router.add_route("OPTIONS", "/api/freekassa/create-order", lambda r: Response(status=204, headers=_cors_headers()))
    app.router.add_get("/api/freekassa/notify", freekassa_notify_handler)
    app.router.add_post("/api/freekassa/notify", freekassa_notify_handler)

    # Health-check: чтобы не было 404 на /api/health
    async def health_handler(request):
        return _json_response({"status": "ok"})

    # Простой индекс по корню, чтобы GET / не давал 404
    async def index_handler(request):
        return Response(
            text="JetStore backend is running. Use /api/* endpoints.",
            content_type="text/plain",
        )

    # Корневой маршрут
    app.router.add_get("/", index_handler)

    # Обычный healthcheck по относительному пути
    app.router.add_get("/api/health", health_handler)
    # На случай, если провайдер стучится с полным URL в path
    app.router.add_get("/https://isxrgtme4d.onrender.com/api/health", health_handler)
    
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
    
    # API баланса (источник правды — БД; доступ только с валидным Telegram init_data)
    SPIN_PRICE_RUB = 100.0
    SPIN_PRICE_USDT = 1.5
    # Лимиты ставок новой рулетки (должны совпадать с MIN_BET_RUB / MAX_BET_RUB на фронте)
    MIN_BET_RUB = 50.0
    MAX_BET_RUB = 10000.0
    
    async def api_balance_get_handler(request):
        """GET /api/balance — вернуть баланс пользователя. Заголовок X-Telegram-Init-Data обязателен."""
        init_data = (request.headers.get("X-Telegram-Init-Data") or request.query.get("init_data") or "").strip()
        user_id = _validate_telegram_init_data(init_data)
        if not user_id:
            return _json_response({"error": "unauthorized", "message": "Некорректные или устаревшие данные Telegram"}, status=401)
        import db as _db_bal
        if not _db_bal.is_enabled():
            return _json_response({"balance_rub": 0.0, "balance_usdt": 0.0})
        bal = await _db_bal.balance_get(user_id)
        return _json_response({"balance_rub": bal["balance_rub"], "balance_usdt": bal["balance_usdt"]})
    
    async def api_balance_deduct_handler(request):
        """POST /api/balance/deduct — списать с баланса (только тип spin).
        Тело: { type: 'spin', currency: 'RUB'|'USDT', amount?: number }.
        Если amount не передан — списывается фиксированная цена спина SPIN_PRICE_RUB/SPIN_PRICE_USDT (старое поведение)."""
        try:
            body = await request.json() if request.can_read_body else {}
        except Exception:
            body = {}
        init_data = (request.headers.get("X-Telegram-Init-Data") or body.get("init_data") or "").strip()
        user_id = _validate_telegram_init_data(init_data)
        if not user_id:
            return _json_response({"error": "unauthorized", "message": "Некорректные или устаревшие данные Telegram"}, status=401)
        deduct_type = (body.get("type") or "").strip().lower()
        currency = (body.get("currency") or "RUB").strip().upper()
        if deduct_type != "spin":
            return _json_response({"error": "bad_request", "message": "Допустим только type: spin"}, status=400)
        if currency not in ("RUB", "USDT"):
            return _json_response({"error": "bad_request", "message": "Допустимы currency: RUB или USDT"}, status=400)
        import db as _db_bal
        if not _db_bal.is_enabled():
            return _json_response({"error": "service_unavailable", "message": "Баланс временно недоступен"}, status=503)

        # Поддержка как старого формата (фиксированная цена спина),
        # так и нового (динамическая ставка amount от клиента).
        amount_rub = SPIN_PRICE_RUB if currency == "RUB" else 0.0
        amount_usdt = SPIN_PRICE_USDT if currency == "USDT" else 0.0
        if currency == "RUB":
            try:
                client_amount = float(body.get("amount") or 0)
            except (TypeError, ValueError):
                client_amount = 0.0
            # Если клиент передал положительную ставку — списываем именно её
            # (например, новая круговая рулетка с выбором ставки).
            if client_amount > 0:
                amount_rub = round(client_amount, 2)
        if currency == "RUB":
            new_bal = await _db_bal.balance_deduct_rub(user_id, amount_rub)
        else:
            new_bal = await _db_bal.balance_deduct_usdt(user_id, amount_usdt)
        if new_bal is None:
            return _json_response(
                {"error": "insufficient_funds", "message": "Недостаточно средств на балансе"},
                status=400,
            )
        return _json_response({
            "success": True,
            "balance_rub": new_bal["balance_rub"],
            "balance_usdt": new_bal["balance_usdt"],
            "spins_added": 1,
        })
    
    # Допустимые суммы выигрыша рулетки (для проверки старой рулетки)
    SPIN_PRIZES_RUB = [5, 10, 25, 50, 75, 100, 150, 200, 300, 500]
    SPIN_PRIZES_USDT = [0.02, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 25]

    # Новая круговая рулетка: конфигурация множителей (как на фронтенде)
    ROULETTE_SEGMENTS = [
        {"multiplier": 0.2, "weight": 22},   # gray
        {"multiplier": 0.7, "weight": 10},   # yellow
        {"multiplier": 2.4, "weight": 6},    # red
        {"multiplier": 10.0, "weight": 2},   # green
    ]

    def _roulette_pick_multiplier() -> float:
        total = sum(seg["weight"] for seg in ROULETTE_SEGMENTS)
        if total <= 0:
            return 0.2
        import random
        r = random.randint(0, total - 1)
        for seg in ROULETTE_SEGMENTS:
            if r < seg["weight"]:
                return float(seg["multiplier"])
            r -= seg["weight"]
        return 0.2
    
    async def api_balance_credit_handler(request):
        """POST /api/balance/credit — зачислить выигрыш рулетки. Тело: { reason: 'spin_win', currency: 'RUB'|'USDT', amount: number }."""
        try:
            body = await request.json() if request.can_read_body else {}
        except Exception:
            body = {}
        init_data = (request.headers.get("X-Telegram-Init-Data") or body.get("init_data") or "").strip()
        user_id = _validate_telegram_init_data(init_data)
        if not user_id:
            return _json_response({"error": "unauthorized", "message": "Некорректные или устаревшие данные Telegram"}, status=401)
        reason = (body.get("reason") or "").strip().lower()
        if reason != "spin_win":
            return _json_response({"error": "bad_request", "message": "Допустим только reason: spin_win"}, status=400)
        currency = (body.get("currency") or "RUB").strip().upper()
        if currency not in ("RUB", "USDT"):
            return _json_response({"error": "bad_request", "message": "Допустимы currency: RUB или USDT"}, status=400)
        try:
            amount = float(body.get("amount") or 0)
        except (TypeError, ValueError):
            amount = 0.0
        if currency == "RUB":
            if amount not in SPIN_PRIZES_RUB:
                return _json_response({"error": "bad_request", "message": "Недопустимая сумма выигрыша (рубли)"}, status=400)
            amount = round(amount, 2)
        else:
            if not any(abs(amount - p) < 1e-6 for p in SPIN_PRIZES_USDT):
                return _json_response({"error": "bad_request", "message": "Недопустимая сумма выигрыша (USDT)"}, status=400)
            amount = round(amount, 6)
        import db as _db_bal
        if not _db_bal.is_enabled():
            return _json_response({"error": "service_unavailable", "message": "Баланс временно недоступен"}, status=503)
        if currency == "RUB":
            await _db_bal.balance_add_rub(user_id, amount)
        else:
            await _db_bal.balance_add_usdt(user_id, amount)
        new_bal = await _db_bal.balance_get(user_id)
        return _json_response({
            "success": True,
            "balance_rub": new_bal["balance_rub"],
            "balance_usdt": new_bal["balance_usdt"],
            "credited": amount,
        })
    
    # Безопасный спин рулетки: сервер сам списывает ставку и начисляет выигрыш
    async def api_roulette_spin_handler(request):
        """
        POST /api/roulette/spin
        Тело: { amount: number, currency: 'RUB' }.
        Сервер:
          - проверяет init_data (Telegram WebApp)
          - проверяет баланс
          - случайно выбирает множитель из ROULETTE_SEGMENTS
          - списывает ставку, начисляет выигрыш
          - возвращает { success, balance_rub, multiplier, win_amount }.
        """
        try:
            body = await request.json() if request.can_read_body else {}
        except Exception:
            body = {}
        init_data = (request.headers.get("X-Telegram-Init-Data") or body.get("init_data") or "").strip()
        user_id = _validate_telegram_init_data(init_data)
        if not user_id:
            return _json_response(
                {"success": False, "error": "unauthorized", "message": "Некорректные или устаревшие данные Telegram"},
                status=401,
            )
        try:
            amount = float(body.get("amount") or 0)
        except (TypeError, ValueError):
            amount = 0.0
        currency = (body.get("currency") or "RUB").strip().upper()
        if currency != "RUB":
            return _json_response(
                {"success": False, "error": "bad_request", "message": "Поддерживается только RUB"},
                status=400,
            )
        # Минимальная и максимальная ставка — как в фронтенде
        if amount < MIN_BET_RUB:
            return _json_response(
                {"success": False, "error": "bad_amount", "message": f"Минимальная ставка {MIN_BET_RUB} ₽"},
                status=400,
            )
        if amount > MAX_BET_RUB:
            return _json_response(
                {"success": False, "error": "bad_amount", "message": f"Максимальная ставка {MAX_BET_RUB} ₽"},
                status=400,
            )
        amount = round(amount, 2)

        import db as _db_ruo
        if not _db_ruo.is_enabled():
            return _json_response(
                {"success": False, "error": "service_unavailable", "message": "Баланс временно недоступен"},
                status=503,
            )

        # Проверяем баланс
        bal = await _db_ruo.balance_get(user_id)
        if (bal.get("balance_rub") or 0) < amount:
            return _json_response(
                {"success": False, "error": "insufficient_funds", "message": "Недостаточно средств на балансе"},
                status=400,
            )

        # Выбираем множитель и считаем выигрыш
        multiplier = _roulette_pick_multiplier()
        win_amount = int(round(amount * multiplier))

        # Атомарно списываем ставку и начисляем выигрыш (2 шага, но обе операции только на сервере)
        new_bal = await _db_ruo.balance_deduct_rub(user_id, amount)
        if new_bal is None:
            return _json_response(
                {"success": False, "error": "insufficient_funds", "message": "Недостаточно средств на балансе"},
                status=400,
            )
        if win_amount > 0:
            await _db_ruo.balance_add_rub(user_id, win_amount)
        final_bal = await _db_ruo.balance_get(user_id)

        # При желании здесь можно дописать запись транзакции в отдельную таблицу.
        return _json_response({
            "success": True,
            "balance_rub": final_bal["balance_rub"],
            "multiplier": multiplier,
            "win_amount": win_amount,
        })

    def _balance_cors(r):
        return Response(status=204, headers=_cors_headers())

    def _roulette_cors(r):
        return Response(status=204, headers=_cors_headers())

    app.router.add_get("/api/balance", api_balance_get_handler)
    app.router.add_route("OPTIONS", "/api/balance", _balance_cors)
    app.router.add_post("/api/balance/deduct", api_balance_deduct_handler)
    app.router.add_route("OPTIONS", "/api/balance/deduct", _balance_cors)
    app.router.add_post("/api/balance/credit", api_balance_credit_handler)
    app.router.add_route("OPTIONS", "/api/balance/credit", _balance_cors)

    # Новая рулетка
    app.router.add_post("/api/roulette/spin", api_roulette_spin_handler)
    app.router.add_route("OPTIONS", "/api/roulette/spin", _roulette_cors)
    
    # API записи покупки: рейтинг + рефералы + users_data.json
    # (функция _get_users_data_path уже определена выше)
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
            
            # В рейтинг попадают только звёзды: игнорируем другие типы покупок
            if not referral_only and purchase_type == "stars":
                import db as _db
                if _db.is_enabled():
                    await _db.user_upsert(user_id, username, first_name)
                    # Для старого/ручного эндпоинта order_id не передаём (None)
                    await _db.purchase_add(user_id, amount_rub, stars_amount, purchase_type, product_name, None)
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
                await _apply_referral_earnings_for_purchase(
                    user_id=user_id,
                    amount_rub=amount_rub,
                    username=username,
                    first_name=first_name,
                )
            
            return _json_response({"success": True})
        except Exception as e:
            logger.error("purchases_record error: %s", e)
            return _json_response({"error": str(e)}, status=500)
    
    app.router.add_post("/api/purchases/record", purchases_record_handler)
    app.router.add_route("OPTIONS", "/api/purchases/record", lambda r: Response(status=204, headers=_cors_headers()))
    
    # СТАТИКА: В продакшене НИКОГДА не включайте SERVE_STATIC!
    # API-сервер должен отдавать только JSON API. Статику (HTML/JS/CSS) лучше на GitHub Pages / Nginx / CDN.
    # SERVE_STATIC=1 раздаёт только каталог html/, но в проде это создаёт лишнюю поверхность атаки.
    _serve_static = (os.environ.get("SERVE_STATIC") or "").strip().lower() in ("1", "true", "yes")
    if _serve_static:
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
    print("   • /start - Главное меню (открыть приложение)")
    print("   • /admin - Админ панель")
    print("   • /id - Узнать свой ID и статус")
    print("   • /users - Статистика пользователей (админы)")
    print("=" * 50)
    if not ADMIN_IDS:
        print("⚠️  ADMIN_IDS не задан в env — админы не назначены")
        print("    Укажите ADMIN_IDS=123456789 в переменных окружения Railway")
    else:
        print(f"👑 Админы (из ADMIN_IDS): {len(ADMIN_IDS)} ID")
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
    
    # Webhook на Render: устраняет TelegramConflictError (несколько инстансов / getUpdates конфликт)
    # RENDER_EXTERNAL_URL задаётся Render автоматически (например https://jet-store-bot-xxx.onrender.com)
    webhook_base = (os.getenv("WEBHOOK_BASE_URL") or os.getenv("RENDER_EXTERNAL_URL") or "").rstrip("/")
    use_webhook = bool(webhook_base)
    
    if use_webhook:
        webhook_path = "/webhook"
        webhook_handler = SimpleRequestHandler(dispatcher=dp, bot=bot, handle_in_background=True)
        webhook_handler.register(http_app, path=webhook_path)
        setup_application(http_app, dp, bot=bot)
        async def _on_webhook_startup(bot: Bot):
            await bot.set_webhook(f"{webhook_base}{webhook_path}")
            logger.info("Webhook установлен: %s%s", webhook_base, webhook_path)
        dp.startup.register(_on_webhook_startup)
        print(f"🔗 Режим WEBHOOK: обновления на {webhook_base}{webhook_path}")
    
    runner = web.AppRunner(http_app)
    await runner.setup()
    port = int(os.getenv("PORT") or "3000")
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"🌐 HTTP API сервер запущен на порту {port}")
    print("   Эндпоинт: /api/telegram/user, /api/cryptobot/create-invoice")
    print("=" * 50)
    
    try:
        if use_webhook:
            logger.info("Бот в режиме webhook — ожидаю обновления")
            while True:
                await asyncio.sleep(3600)
        else:
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