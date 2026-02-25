#!/usr/bin/env python3
"""
Миграция JSON-файлов в PostgreSQL.
Запуск: DATABASE_URL=postgresql://... python migrate_to_postgres.py
"""
import asyncio
import json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def read_json(path: str) -> dict:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f) or {}
    except Exception as e:
        print(f"Ошибка чтения {path}: {e}")
    return {}


async def main():
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        print("Задайте DATABASE_URL")
        sys.exit(1)
    import asyncpg
    conn = await asyncpg.connect(url)
    try:
        # Создаём схему при необходимости
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, username TEXT DEFAULT '', first_name TEXT DEFAULT '', created_at TIMESTAMPTZ DEFAULT NOW());
            CREATE TABLE IF NOT EXISTS purchases (id SERIAL PRIMARY KEY, user_id TEXT NOT NULL, amount_rub NUMERIC(12,2) NOT NULL, stars_amount INTEGER DEFAULT 0, type TEXT DEFAULT 'stars', product_name TEXT DEFAULT '', created_at TIMESTAMPTZ DEFAULT NOW());
            CREATE TABLE IF NOT EXISTS referrals (user_id TEXT PRIMARY KEY, parent1 TEXT, parent2 TEXT, parent3 TEXT, referrals_l1 JSONB DEFAULT '[]', referrals_l2 JSONB DEFAULT '[]', referrals_l3 JSONB DEFAULT '[]', earned_rub NUMERIC(12,2) DEFAULT 0, volume_rub NUMERIC(12,2) DEFAULT 0, username TEXT DEFAULT '', first_name TEXT DEFAULT '', updated_at TIMESTAMPTZ DEFAULT NOW());
            CREATE TABLE IF NOT EXISTS rating_prefs (user_id TEXT PRIMARY KEY, show_in_rating BOOLEAN DEFAULT TRUE, updated_at TIMESTAMPTZ DEFAULT NOW());
        """)
        # Referrals
        ref_path = os.path.join(SCRIPT_DIR, "referrals_data.json")
        refs = read_json(ref_path)
        if refs:
            for uid, data in refs.items():
                await conn.execute("""
                    INSERT INTO referrals (user_id, parent1, parent2, parent3, referrals_l1, referrals_l2, referrals_l3, earned_rub, volume_rub)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (user_id) DO UPDATE SET
                        parent1 = EXCLUDED.parent1, parent2 = EXCLUDED.parent2, parent3 = EXCLUDED.parent3,
                        referrals_l1 = EXCLUDED.referrals_l1, referrals_l2 = EXCLUDED.referrals_l2, referrals_l3 = EXCLUDED.referrals_l3,
                        earned_rub = EXCLUDED.earned_rub, volume_rub = EXCLUDED.volume_rub
                """, uid,
                    data.get("parent1"), data.get("parent2"), data.get("parent3"),
                    json.dumps(data.get("referrals_l1") or []),
                    json.dumps(data.get("referrals_l2") or []),
                    json.dumps(data.get("referrals_l3") or []),
                    float(data.get("earned_rub") or 0),
                    float(data.get("volume_rub") or 0),
                )
            print(f"Мигрировано рефералов: {len(refs)}")
        # Users & Purchases
        for name in ["users_data.json"]:
            path = os.path.join(SCRIPT_DIR, name)
            if not os.path.exists(path):
                path = os.path.join(os.path.dirname(SCRIPT_DIR), name)
            data = read_json(path)
            if not data:
                continue
            for uid, u in data.items():
                if not isinstance(u, dict):
                    continue
                await conn.execute("""
                    INSERT INTO users (id, username, first_name) VALUES ($1, $2, $3)
                    ON CONFLICT (id) DO UPDATE SET username = COALESCE(NULLIF($2,''), users.username), first_name = COALESCE(NULLIF($3,''), users.first_name)
                """, uid, u.get("username") or "", u.get("first_name") or "")
                for p in u.get("purchases") or []:
                    if not isinstance(p, dict):
                        continue
                    await conn.execute("""
                        INSERT INTO purchases (user_id, amount_rub, stars_amount, type, product_name)
                        VALUES ($1, $2, $3, $4, $5)
                    """, uid,
                        float(p.get("amount") or p.get("amount_rub") or 0),
                        int(p.get("stars_amount") or p.get("starsAmount") or 0),
                        p.get("type") or "stars",
                        p.get("productName") or p.get("product_name") or "",
                    )
            print(f"Мигрировано пользователей из {name}: {len(data)}")
        # Rating prefs
        rpath = os.path.join(SCRIPT_DIR, "rating_data.json")
        rdata = read_json(rpath)
        if rdata:
            for uid, prefs in rdata.items():
                if isinstance(prefs, dict) and "show_in_rating" in prefs:
                    await conn.execute("""
                        INSERT INTO rating_prefs (user_id, show_in_rating) VALUES ($1, $2)
                        ON CONFLICT (user_id) DO UPDATE SET show_in_rating = EXCLUDED.show_in_rating
                    """, uid, bool(prefs["show_in_rating"]))
            print(f"Мигрировано настроек рейтинга: {len(rdata)}")
        print("Миграция завершена.")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
