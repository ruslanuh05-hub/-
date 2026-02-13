# db.py — PostgreSQL для JET Store Bot
# Использует asyncpg, DATABASE_URL из env (Railway, Heroku и др.)
import os
import json
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

_pool = None
_db_enabled = False


async def init_pool() -> bool:
    """Инициализация пула подключений. Возвращает True если PostgreSQL доступен."""
    global _pool, _db_enabled
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        logger.warning("DATABASE_URL не задан — PostgreSQL отключён, используются JSON-файлы")
        return False
    try:
        import asyncpg
        _pool = await asyncpg.create_pool(url, min_size=1, max_size=10, command_timeout=60)
        await _ensure_schema()
        _db_enabled = True
        logger.info("PostgreSQL подключён")
        return True
    except ImportError:
        logger.warning("asyncpg не установлен: pip install asyncpg")
        return False
    except Exception as e:
        logger.warning("Ошибка подключения к PostgreSQL: %s", e)
        return False


async def close_pool():
    """Закрытие пула."""
    global _pool, _db_enabled
    _db_enabled = False
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("PostgreSQL отключён")


def is_enabled() -> bool:
    return _db_enabled


async def _ensure_schema():
    """Создание таблиц при первом запуске."""
    async with _pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                username TEXT DEFAULT '',
                first_name TEXT DEFAULT '',
                last_name TEXT DEFAULT '',
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        logger.info("PostgreSQL: ensured table 'users'")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS purchases (
                id SERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                amount_rub NUMERIC(12,2) NOT NULL,
                stars_amount INTEGER DEFAULT 0,
                type TEXT DEFAULT 'stars',
                product_name TEXT DEFAULT '',
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        logger.info("PostgreSQL: ensured table 'purchases'")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_purchases_user ON purchases(user_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_purchases_created ON purchases(created_at)")
        logger.info("PostgreSQL: ensured indexes idx_purchases_user, idx_purchases_created")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS referrals (
                user_id TEXT PRIMARY KEY,
                parent1 TEXT,
                parent2 TEXT,
                parent3 TEXT,
                referrals_l1 JSONB DEFAULT '[]',
                referrals_l2 JSONB DEFAULT '[]',
                referrals_l3 JSONB DEFAULT '[]',
                earned_rub NUMERIC(12,2) DEFAULT 0,
                volume_rub NUMERIC(12,2) DEFAULT 0,
                username TEXT DEFAULT '',
                first_name TEXT DEFAULT '',
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        logger.info("PostgreSQL: ensured table 'referrals'")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS rating_prefs (
                user_id TEXT PRIMARY KEY,
                show_in_rating BOOLEAN DEFAULT TRUE,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        logger.info("PostgreSQL: ensured table 'rating_prefs'")
    logger.info("Схема PostgreSQL проверена (users, purchases, referrals, rating_prefs)")


# --- Referrals ---

async def ref_get_or_create(user_id: str) -> dict:
    """Получить или создать запись реферала."""
    if not _db_enabled:
        return None
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM referrals WHERE user_id = $1", user_id
        )
        if row:
            return _row_to_ref(row)
        await conn.execute("""
            INSERT INTO referrals (user_id, referrals_l1, referrals_l2, referrals_l3)
            VALUES ($1, '[]', '[]', '[]')
            ON CONFLICT (user_id) DO NOTHING
        """, user_id)
        row = await conn.fetchrow("SELECT * FROM referrals WHERE user_id = $1", user_id)
        return _row_to_ref(row) if row else {
            "parent1": None, "parent2": None, "parent3": None,
            "referrals_l1": [], "referrals_l2": [], "referrals_l3": [],
            "earned_rub": 0.0, "volume_rub": 0.0,
        }


def _row_to_ref(row) -> dict:
    r = dict(row)
    for k in ("referrals_l1", "referrals_l2", "referrals_l3"):
        v = r.get(k)
        if isinstance(v, list):
            r[k] = v
        elif isinstance(v, str):
            try:
                r[k] = json.loads(v) if v else []
            except Exception:
                r[k] = []
        else:
            r[k] = [] if v is None else list(v) if hasattr(v, "__iter__") and not isinstance(v, str) else []
    return {
        "parent1": r.get("parent1"),
        "parent2": r.get("parent2"),
        "parent3": r.get("parent3"),
        "referrals_l1": r.get("referrals_l1") or [],
        "referrals_l2": r.get("referrals_l2") or [],
        "referrals_l3": r.get("referrals_l3") or [],
        "earned_rub": float(r.get("earned_rub") or 0),
        "volume_rub": float(r.get("volume_rub") or 0),
        "username": r.get("username") or "",
        "first_name": r.get("first_name") or "",
    }


async def ref_save(user_id: str, data: dict):
    """Сохранить запись реферала."""
    if not _db_enabled:
        return
    async with _pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO referrals (user_id, parent1, parent2, parent3, referrals_l1, referrals_l2, referrals_l3, earned_rub, volume_rub, username, first_name)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (user_id) DO UPDATE SET
                parent1 = EXCLUDED.parent1,
                parent2 = EXCLUDED.parent2,
                parent3 = EXCLUDED.parent3,
                referrals_l1 = EXCLUDED.referrals_l1,
                referrals_l2 = EXCLUDED.referrals_l2,
                referrals_l3 = EXCLUDED.referrals_l3,
                earned_rub = EXCLUDED.earned_rub,
                volume_rub = EXCLUDED.volume_rub,
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                updated_at = NOW()
        """, user_id,
            data.get("parent1"), data.get("parent2"), data.get("parent3"),
            json.dumps(data.get("referrals_l1") or []),
            json.dumps(data.get("referrals_l2") or []),
            json.dumps(data.get("referrals_l3") or []),
            float(data.get("earned_rub") or 0),
            float(data.get("volume_rub") or 0),
            data.get("username") or "",
            data.get("first_name") or "",
        )


async def ref_load_all() -> dict:
    """Загрузить все реферальные данные (user_id -> dict)."""
    if not _db_enabled:
        return {}
    async with _pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM referrals")
    return {r["user_id"]: _row_to_ref(r) for r in rows}


async def ref_add_earned(user_id: str, volume_delta: float, earned_delta: float):
    """Добавить объём и заработок родителю."""
    if not _db_enabled:
        return
    async with _pool.acquire() as conn:
        await conn.execute("""
            UPDATE referrals SET
                volume_rub = volume_rub + $2,
                earned_rub = earned_rub + $3,
                updated_at = NOW()
            WHERE user_id = $1
        """, user_id, volume_delta, earned_delta)


async def ref_set_earned(user_id: str, earned_rub: float):
    """Установить earned_rub (при выводе)."""
    if not _db_enabled:
        return
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE referrals SET earned_rub = $2, updated_at = NOW() WHERE user_id = $1",
            user_id, earned_rub
        )


# --- Users & Purchases ---

async def user_upsert(user_id: str, username: str = "", first_name: str = ""):
    """Создать/обновить пользователя."""
    if not _db_enabled:
        return
    async with _pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (id, username, first_name)
            VALUES ($1, $2, $3)
            ON CONFLICT (id) DO UPDATE SET
                username = COALESCE(NULLIF($2,''), users.username),
                first_name = COALESCE(NULLIF($3,''), users.first_name)
        """, user_id, username or "", first_name or "")


async def purchase_add(user_id: str, amount_rub: float, stars_amount: int, ptype: str, product_name: str, order_id: str | None = None):
    """Добавить покупку. order_id — наш внешний ID (#ABC123), может быть None."""
    if not _db_enabled:
        return
    async with _pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO purchases (user_id, amount_rub, stars_amount, type, product_name, order_id)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, user_id, amount_rub, stars_amount or int(amount_rub / 0.65), ptype or "stars", product_name or "", order_id or None)


async def get_users_with_purchases() -> dict:
    """Все пользователи с покупками (для рейтинга). user_id -> {username, first_name, purchases: [...]}"""
    if not _db_enabled:
        return {}
    async with _pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT u.id, u.username, u.first_name, p.id as pid, p.amount_rub, p.stars_amount, p.type, p.product_name, p.created_at
            FROM users u
            JOIN purchases p ON p.user_id = u.id
            ORDER BY p.created_at DESC
        """)
    users = {}
    for r in rows:
        uid = r["id"]
        if uid not in users:
            users[uid] = {"username": r["username"] or "", "first_name": r["first_name"] or "", "purchases": []}
        users[uid]["purchases"].append({
            "amount": float(r["amount_rub"]),
            "stars_amount": r["stars_amount"] or 0,
            "type": r["type"] or "stars",
            "productName": r["product_name"] or "",
            "date": r["created_at"].strftime("%Y-%m-%d %H:%M:%S") if r["created_at"] else "",
        })
    return users


# --- Rating prefs ---

async def rating_get_all() -> dict:
    """user_id -> {show_in_rating: bool}"""
    if not _db_enabled:
        return {}
    async with _pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id, show_in_rating FROM rating_prefs")
    return {r["user_id"]: {"show_in_rating": r["show_in_rating"]} for r in rows}


async def rating_set(user_id: str, show: bool):
    """Установить видимость в рейтинге."""
    if not _db_enabled:
        return
    async with _pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO rating_prefs (user_id, show_in_rating)
            VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET show_in_rating = $2, updated_at = NOW()
        """, user_id, show)
