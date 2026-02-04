import asyncio
import logging
import json
import os
import re
import base64
import time
from io import BytesIO
from datetime import datetime
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
BOT_TOKEN = os.getenv("BOT_TOKEN", "8528977779:AAHbPeWIA8rNuDyHc_eI7F7c2qr3M8Xw3_o")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "6928639672").split(",") if x.strip()]
WEB_APP_URL = os.getenv("WEB_APP_URL", "https://jetstoreapp.ru")
ADM_WEB_APP_URL = os.getenv("ADM_WEB_APP_URL", "https://jetstoreapp.ru/html/admin.html")

# Группа/чат, куда слать уведомления о продаже звёзд
SELL_STARS_NOTIFY_CHAT_ID = int(os.getenv("SELL_STARS_NOTIFY_CHAT_ID", "0") or "0")

# Курс выплаты за 1 звезду (RUB), используем тот же, что в мини-аппе
STAR_BUY_RATE_RUB = float(os.getenv("STAR_BUY_RATE_RUB", "0.65") or "0.65")

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

_donatehub_token: str | None = None
_donatehub_token_ts: float = 0.0

def _cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "*",
    }

def _json_response(payload: dict | list, status: int = 200):
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

telethon_client: TelegramClient | None = None

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

async def lookup_user_via_telethon(username: str) -> dict | None:
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
    
    # Продажа звёзд (sellstars:amount)
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

        # Уведомление пользователю
        await message.answer(
            "✅ Оплата звёздами получена!\n\n"
            f"Мы выплатим тебе примерно <b>{payout_rub:.2f} ₽</b> за {stars} ⭐.\n"
            "Ожидай обработки заявки.",
            parse_mode="HTML"
        )

        # Уведомление в группу/канал (если задан CHAT_ID)
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
    # Хранилище оплаченных заказов Fragment (заполняется вебхуком order.completed)
    app["fragment_completed_orders"] = set()
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

    async def api_config_handler(request):
        """Публичная конфигурация для фронтенда (бот, домен)"""
        try:
            me = await bot.get_me()
            bot_username = me.username or "JetStoreApp_bot"
            return _json_response({
                "bot_username": bot_username,
                "web_app_url": WEB_APP_URL,
                "domain": "jetstoreapp.ru"
            })
        except Exception as e:
            logger.error(f"/api/config error: {e}")
            return _json_response({"bot_username": "JetStoreApp_bot", "web_app_url": WEB_APP_URL, "domain": "jetstoreapp.ru"})

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
        completed = request.app.get("fragment_completed_orders") or set()
        if order_id and order_id in completed:
            return _json_response({"paid": True, "order_id": order_id, "delivered_by_fragment": True})
        if invoice_id and method == "cryptobot" and CRYPTO_PAY_TOKEN:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"{CRYPTO_PAY_BASE}/getInvoices",
                        headers={"Content-Type": "application/json", "Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN},
                        params={"invoice_ids": str(invoice_id), "status": "paid"}
                    ) as resp:
                        cdata = await resp.json(content_type=None) if resp.content_type else {}
                        if cdata.get("ok"):
                            items = cdata.get("result")
                            if isinstance(items, list):
                                for inv in items:
                                    if str(inv.get("invoice_id")) == str(invoice_id) and inv.get("status") == "paid":
                                        return _json_response({"paid": True, "invoice_id": invoice_id})
            except Exception as e:
                logger.warning(f"Crypto Pay check invoice {invoice_id}: {e}")
        if invoice_id and method == "cryptobot":
            return _json_response({"paid": False})
        if is_stars or is_premium:
            return _json_response({"paid": True})
        if transaction_id:
            pass
        return _json_response({"paid": False})
    
    app.router.add_post("/api/payment/check", payment_check_handler)
    app.router.add_route("OPTIONS", "/api/payment/check", lambda r: Response(status=204, headers=_cors_headers()))
    
    # Fragment.com — выдача звёзд после покупки (iStar API)
    # Документация: https://istar.fragmentapi.com/docs
    _fragment_cfg = _read_json_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), "fragment_config.json"))
    FRAGMENT_API_KEY = _get_env_clean("FRAGMENT_API_KEY") or _fragment_cfg.get("api_key", "")
    FRAGMENT_BASE = _fragment_cfg.get("base_url", "https://v1.fragmentapi.com/api/v1/partner") or "https://v1.fragmentapi.com/api/v1/partner"
    
    async def fragment_deliver_stars_handler(request):
        """Выдача звёзд через fragment.com после успешной оплаты"""
        if not FRAGMENT_API_KEY:
            return _json_response({"error": "not_configured", "message": "FRAGMENT_API_KEY not set (fragment_config.json)"}, status=503)
        try:
            body = await request.json()
        except Exception:
            return _json_response({"error": "bad_request", "message": "Invalid JSON"}, status=400)
        
        stars_amount = body.get("stars_amount") or body.get("quantity")
        recipient = (body.get("recipient") or body.get("username") or "").strip().lstrip("@")
        
        if not stars_amount:
            return _json_response({"error": "bad_request", "message": "stars_amount is required"}, status=400)
        stars_amount = int(stars_amount)
        if stars_amount < 50:
            return _json_response({"error": "bad_request", "message": "Minimum 50 stars"}, status=400)
        if stars_amount > 1_000_000:
            return _json_response({"error": "bad_request", "message": "Maximum 1,000,000 stars"}, status=400)
        if not recipient:
            return _json_response({"error": "bad_request", "message": "recipient (username) is required"}, status=400)
        
        headers = {"Content-Type": "application/json", "API-Key": FRAGMENT_API_KEY}
        
        try:
            async with aiohttp.ClientSession() as session:
                # 1) Валидация получателя
                async with session.get(
                    f"{FRAGMENT_BASE}/star/recipient/search",
                    params={"username": recipient, "quantity": stars_amount},
                    headers={"API-Key": FRAGMENT_API_KEY}
                ) as resp:
                    val_data = await resp.json(content_type=None) if resp.content_type else {}
                    if resp.status >= 400:
                        return _json_response({
                            "error": "fragment_validation",
                            "message": val_data.get("message", val_data.get("error", "Invalid recipient")),
                            "details": val_data
                        }, status=400)
                    recipient_hash = val_data.get("recipient")
                    if not recipient_hash:
                        return _json_response({"error": "fragment_validation", "message": "Recipient not found"}, status=400)
                
                # 2) Создание заказа на выдачу звёзд
                payload = {"username": recipient, "recipient_hash": recipient_hash, "quantity": stars_amount, "wallet_type": "TON"}
                async with session.post(f"{FRAGMENT_BASE}/orders/star", headers=headers, json=payload) as resp:
                    data = await resp.json(content_type=None) if resp.content_type else {}
                    if resp.status >= 400:
                        return _json_response({
                            "error": "fragment_error",
                            "message": data.get("message", data.get("error", "Fragment API error")),
                            "details": data
                        }, status=502)
                    return _json_response({"success": True, "order": data, "stars_amount": stars_amount, "recipient": recipient})
        except Exception as e:
            logger.error(f"Fragment deliver stars error: {e}")
            return _json_response({"error": "internal_error", "message": str(e)}, status=500)
    
    app.router.add_post("/api/fragment/deliver-stars", fragment_deliver_stars_handler)
    app.router.add_route('OPTIONS', '/api/fragment/deliver-stars', lambda r: Response(status=204, headers=_cors_headers()))

    async def fragment_deliver_premium_handler(request):
        """Выдача Premium через fragment.com (iStar API), оплата TonKeeper"""
        if not FRAGMENT_API_KEY:
            return _json_response({"error": "not_configured", "message": "FRAGMENT_API_KEY not set (fragment_config.json)"}, status=503)
        try:
            body = await request.json()
        except Exception:
            return _json_response({"error": "bad_request", "message": "Invalid JSON"}, status=400)
        recipient = (body.get("recipient") or body.get("username") or "").strip().lstrip("@")
        months = body.get("months", 3)
        try:
            months = int(months)
        except (TypeError, ValueError):
            months = 3
        if months not in (3, 6, 12):
            months = 3
        if not recipient:
            return _json_response({"error": "bad_request", "message": "recipient (username) is required"}, status=400)
        headers = {"Content-Type": "application/json", "API-Key": FRAGMENT_API_KEY}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{FRAGMENT_BASE}/premium/recipient/search",
                    params={"username": recipient, "months": months},
                    headers={"API-Key": FRAGMENT_API_KEY}
                ) as resp:
                    val_data = await resp.json(content_type=None) if resp.content_type else {}
                    if resp.status >= 400:
                        return _json_response({
                            "error": "fragment_validation",
                            "message": val_data.get("message", val_data.get("error", "Invalid recipient")),
                            "details": val_data
                        }, status=400)
                    recipient_hash = val_data.get("recipient")
                    if not recipient_hash:
                        return _json_response({"error": "fragment_validation", "message": "Recipient not found"}, status=400)
                payload = {"username": recipient, "recipient_hash": recipient_hash, "months": months, "wallet_type": "TON"}
                async with session.post(f"{FRAGMENT_BASE}/orders/premium", headers=headers, json=payload) as resp:
                    data = await resp.json(content_type=None) if resp.content_type else {}
                    if resp.status >= 400:
                        return _json_response({
                            "error": "fragment_error",
                            "message": data.get("message", data.get("error", "Fragment API error")),
                            "details": data
                        }, status=502)
                    return _json_response({"success": True, "order": data, "months": months, "recipient": recipient})
        except Exception as e:
            logger.error(f"Fragment deliver premium error: {e}")
            return _json_response({"error": "internal_error", "message": str(e)}, status=500)

    app.router.add_post("/api/fragment/deliver-premium", fragment_deliver_premium_handler)
    app.router.add_route("OPTIONS", "/api/fragment/deliver-premium", lambda r: Response(status=204, headers=_cors_headers()))

    # Создание заказа Fragment (звёзды/премиум) — пользователь оплачивает в Fragment/TonKeeper, затем вебхук → payment_check по order_id
    async def fragment_create_star_order_handler(request):
        """Создать заказ на звёзды: возвращает order_id и payment_url (если API отдаёт), фронт открывает ссылку оплаты TonKeeper"""
        if not FRAGMENT_API_KEY:
            return _json_response({"error": "not_configured", "message": "FRAGMENT_API_KEY not set"}, status=503)
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
        headers = {"Content-Type": "application/json", "API-Key": FRAGMENT_API_KEY}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{FRAGMENT_BASE}/star/recipient/search",
                    params={"username": recipient, "quantity": stars_amount},
                    headers={"API-Key": FRAGMENT_API_KEY}
                ) as resp:
                    val_data = await resp.json(content_type=None) if resp.content_type else {}
                    if resp.status >= 400 or not val_data.get("recipient"):
                        return _json_response({
                            "error": "fragment_validation",
                            "message": val_data.get("message", "Recipient not found")
                        }, status=400)
                    recipient_hash = val_data.get("recipient")
                payload = {"username": recipient, "recipient_hash": recipient_hash, "quantity": stars_amount, "wallet_type": "TON"}
                async with session.post(f"{FRAGMENT_BASE}/orders/star", headers=headers, json=payload) as resp:
                    data = await resp.json(content_type=None) if resp.content_type else {}
                    if resp.status >= 400:
                        return _json_response({
                            "error": "fragment_error",
                            "message": data.get("message", data.get("error", "Fragment API error"))
                        }, status=502)
                    order_id = data.get("order_id") or data.get("id") or ""
                    payment_url = data.get("payment_link") or data.get("payment_url") or data.get("pay_url") or ""
                    return _json_response({
                        "success": True, "order_id": order_id, "payment_url": payment_url or None,
                        "order": data, "stars_amount": stars_amount, "recipient": recipient
                    })
        except Exception as e:
            logger.error(f"Fragment create star order error: {e}")
            return _json_response({"error": "internal_error", "message": str(e)}, status=500)

    async def fragment_create_premium_order_handler(request):
        """Создать заказ на Premium: возвращает order_id и payment_url (если есть), фронт открывает оплату TonKeeper"""
        if not FRAGMENT_API_KEY:
            return _json_response({"error": "not_configured", "message": "FRAGMENT_API_KEY not set"}, status=503)
        try:
            body = await request.json()
        except Exception:
            return _json_response({"error": "bad_request", "message": "Invalid JSON"}, status=400)
        recipient = (body.get("recipient") or body.get("username") or "").strip().lstrip("@")
        months = body.get("months", 3)
        try:
            months = int(months)
        except (TypeError, ValueError):
            months = 3
        if months not in (3, 6, 12):
            months = 3
        if not recipient:
            return _json_response({"error": "bad_request", "message": "recipient is required"}, status=400)
        headers = {"Content-Type": "application/json", "API-Key": FRAGMENT_API_KEY}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{FRAGMENT_BASE}/premium/recipient/search",
                    params={"username": recipient, "months": months},
                    headers={"API-Key": FRAGMENT_API_KEY}
                ) as resp:
                    val_data = await resp.json(content_type=None) if resp.content_type else {}
                    if resp.status >= 400 or not val_data.get("recipient"):
                        return _json_response({
                            "error": "fragment_validation",
                            "message": val_data.get("message", "Recipient not found")
                        }, status=400)
                    recipient_hash = val_data.get("recipient")
                payload = {"username": recipient, "recipient_hash": recipient_hash, "months": months, "wallet_type": "TON"}
                async with session.post(f"{FRAGMENT_BASE}/orders/premium", headers=headers, json=payload) as resp:
                    data = await resp.json(content_type=None) if resp.content_type else {}
                    if resp.status >= 400:
                        return _json_response({
                            "error": "fragment_error",
                            "message": data.get("message", data.get("error", "Fragment API error"))
                        }, status=502)
                    order_id = data.get("order_id") or data.get("id") or ""
                    payment_url = data.get("payment_link") or data.get("payment_url") or data.get("pay_url") or ""
                    return _json_response({
                        "success": True, "order_id": order_id, "payment_url": payment_url or None,
                        "order": data, "months": months, "recipient": recipient
                    })
        except Exception as e:
            logger.error(f"Fragment create premium order error: {e}")
            return _json_response({"error": "internal_error", "message": str(e)}, status=500)

    app.router.add_post("/api/fragment/create-star-order", fragment_create_star_order_handler)
    app.router.add_route("OPTIONS", "/api/fragment/create-star-order", lambda r: Response(status=204, headers=_cors_headers()))
    app.router.add_post("/api/fragment/create-premium-order", fragment_create_premium_order_handler)
    app.router.add_route("OPTIONS", "/api/fragment/create-premium-order", lambda r: Response(status=204, headers=_cors_headers()))

    # Вебхук Fragment (iStar): order.completed / order.failed — сохраняем оплаченные заказы для payment_check
    async def fragment_webhook_handler(request):
        try:
            body = await request.json()
        except Exception:
            return _json_response({"error": "invalid_payload"}, status=400)
        event_type = body.get("event_type") or (request.headers.get("X-iStar-Event") or "").strip()
        order = body.get("order") or {}
        order_id = (order.get("id") or "").strip()
        if event_type == "order.completed" and order_id:
            completed = request.app.get("fragment_completed_orders")
            if completed is not None:
                completed.add(order_id)
                logger.info(f"Fragment webhook: order {order_id} marked as completed")
        elif event_type == "order.failed" and order_id:
            logger.warning(f"Fragment webhook: order {order_id} failed")
        return _json_response({"ok": True})
    
    app.router.add_post("/api/fragment/webhook", fragment_webhook_handler)
    app.router.add_route("OPTIONS", "/api/fragment/webhook", lambda r: Response(status=204, headers=_cors_headers()))

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
        if not CRYPTO_PAY_TOKEN:
            return _json_response({"error": "not_configured", "message": "CRYPTO_PAY_TOKEN не задан. Добавьте в переменные окружения Railway/Render."}, status=503)
        try:
            body = await request.json()
        except Exception:
            return _json_response({"error": "bad_request", "message": "Invalid JSON"}, status=400)
        amount = body.get("amount") or body.get("total_amount")
        try:
            amount = float(amount)
        except (TypeError, ValueError):
            return _json_response({"error": "bad_request", "message": "amount is required (number)"}, status=400)
        if amount < 1:
            return _json_response({"error": "bad_request", "message": "Minimum amount 1 RUB"}, status=400)
        if amount > 1_000_000:
            return _json_response({"error": "bad_request", "message": "Maximum amount 1,000,000 RUB"}, status=400)
        description = (body.get("description") or f"Оплата {amount:.0f} ₽").strip()[:1024]
        payload_data = body.get("payload") or ""
        if isinstance(payload_data, dict):
            payload_data = json.dumps(payload_data, ensure_ascii=False)[:4096]
        else:
            payload_data = str(payload_data)[:4096]

        # По документации Crypto Pay API: https://help.send.tg/en/articles/10279948-crypto-pay-api
        payload_obj = {
            "currency_type": "fiat",
            "fiat": "RUB",
            "amount": f"{amount:.2f}",  # Строка в формате float, напр. "125.50"
            "description": description,
            "accepted_assets": "USDT,TON,BTC,ETH,TRX,USDC",
            "payload": payload_data,
            "paid_btn_name": "callback",
            "paid_btn_url": WEB_APP_URL or "https://jetstoreapp.ru",
        }
        headers = {
            "Content-Type": "application/json",
            "Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN,
        }
        logger.info(f"CryptoBot createInvoice: amount={amount}, fiat=RUB")
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
                    logger.info(f"CryptoBot invoice created: invoice_id={inv.get('invoice_id')}, pay_url_len={len(pay_url) if pay_url else 0}")
                    return _json_response({
                        "success": True, "invoice_id": inv.get("invoice_id"),
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
    
    # Раздача статических файлов мини-аппа (index.html, script.js, style.css, assets/* и т.д.)
    # Открывать: http://localhost:3000/
    # ВАЖНО: добавляем ПОСЛЕ /api/*, чтобы статика не перехватывала API-роуты
    static_dir = os.path.dirname(os.path.abspath(__file__))
    app.router.add_static('/', static_dir, show_index=True)
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
    port = int(os.getenv("PORT", "3000"))
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

if __name__ == "__main__":
    asyncio.run(main())