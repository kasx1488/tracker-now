#!/usr/bin/env python3
"""
Solana Token Analyzer — Stage 1 + Stage 2
Sources: GMGN OpenAPI + Solscan

Usage:
  python3 analyze.py <CA>
  python3 analyze.py <CA> --deep
  python3 analyze.py --wallet <ADDR>
"""
import os, sys, re, uuid, time, argparse, requests, sqlite3, json, random
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict


def _generate_sol_aut() -> str:
    """
    Генерируем sol-aut заголовок для Solscan API v2.
    Реверс-инжиниринг: random 32-char строка из safe charset с вставкой 'B9dls0fK'.
    """
    chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789==--"
    t = "".join(random.choice(chars) for _ in range(16))
    r = "".join(random.choice(chars) for _ in range(16))
    n = random.randint(0, 30)
    i = t + r
    return i[:n] + "B9dls0fK" + i[n:]

# ── Env ───────────────────────────────────────────────────────────────────────
def load_env():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())
load_env()

GMGN_BASE    = "https://openapi.gmgn.ai"
CHAIN        = "sol"
DEBUG        = False  # включается флагом --debug

HELIUS_KEY   = os.environ.get("HELIUS_KEY", "")   # helius.dev → бесплатный ключ

# ── GMGN Key Pool ──────────────────────────────────────────────────────────────
# Поддерживаем три формата в .env:
#   GMGN_API_KEYS=key1,key2,key3          ← приоритет (несколько ключей)
#   GMGN_API_KEY=key1                     ← обратная совместимость (один ключ)
#   GMGN_API_KEY_2=key2 + GMGN_API_KEY_3=key3 ← дополнительные ключи

def _load_gmgn_keys():
    keys = []
    # Формат 1: GMGN_API_KEYS=k1,k2,k3
    multi = os.environ.get("GMGN_API_KEYS", "")
    if multi:
        keys = [k.strip() for k in multi.split(",") if k.strip()]
    # Формат 2/3: отдельные переменные
    if not keys:
        for var in ("GMGN_API_KEY", "GMGN_API_KEY_2", "GMGN_API_KEY_3",
                    "GMGN_API_KEY_4", "GMGN_API_KEY_5"):
            k = os.environ.get(var, "").strip()
            if k and k not in keys:
                keys.append(k)
    return keys

import threading as _threading

class _GmgnKeyPool:
    """
    Пул GMGN API ключей с round-robin ротацией и per-key cooldown.

    Логика выбора ключа:
      1. Перебираем ключи по кругу (_idx % N)
      2. Если ключ в cooldown (получил 429) — пропускаем, берём следующий
      3. Если ВСЕ ключи в cooldown — ждём пока освободится ближайший
      4. Каждый ключ имеет свой rate-limiter (GMGN_MIN_DELAY между запросами)

    Многопоточно безопасен: разные треды получают разные ключи → нет конфликтов.
    """
    MIN_DELAY    = 0.8   # минимум секунд между запросами одним ключом
    COOLDOWN_429 = 90    # секунд бана после 429 (один ключ)
    COOLDOWN_ERR = 30    # секунд бана после сетевой ошибки

    def __init__(self, keys):
        self._keys      = keys if keys else [""]     # пустая строка = нет ключа
        self._n         = len(self._keys)
        self._lock      = _threading.Lock()
        self._idx       = 0
        # Per-key state
        self._last_req  = [0.0]  * self._n           # timestamp последнего запроса
        self._cooldown  = [0.0]  * self._n           # timestamp до которого ключ на паузе
        self._use_count = [0]    * self._n            # счётчик использований
        self._err_count = [0]    * self._n            # счётчик ошибок

    @property
    def count(self):
        return self._n

    def acquire(self):
        """
        Возвращает (key, key_index) — следующий доступный ключ.
        Блокирует если все ключи в cooldown.
        После вызова acquire() НУЖНО вызвать release(idx) для обновления таймера.
        """
        with self._lock:
            now = time.time()
            # Ищем ключ: не в cooldown + минимальная задержка соблюдена
            for attempt in range(self._n * 2):
                i = self._idx % self._n
                self._idx += 1
                # Ключ в cooldown?
                if self._cooldown[i] > now:
                    continue
                # Ключ соблюл rate limit?
                wait = self.MIN_DELAY - (now - self._last_req[i])
                if wait > 0:
                    # Этот ключ занят — если есть другие свободные, пропустим
                    # Если это единственный — подождём
                    has_free = any(
                        self._cooldown[j] <= now and
                        (self.MIN_DELAY - (now - self._last_req[j])) <= 0
                        for j in range(self._n) if j != i
                    )
                    if has_free:
                        continue
                    # Нет других свободных — ждём этот
                    time.sleep(wait)
                self._last_req[i] = time.time()
                self._use_count[i] += 1
                return self._keys[i], i

            # Все ключи в cooldown — ждём ближайший выход из cooldown
            wake = min(self._cooldown)
            wait_s = max(0.1, wake - time.time())
            if DEBUG:
                print(f"  DEBUG KeyPool: все ключи в cooldown, жду {wait_s:.1f}s")
            time.sleep(wait_s)
            # Рекурсивный вызов после ожидания
            # Сбрасываем idx чтобы начать с первого освободившегося
            i = self._cooldown.index(min(self._cooldown))
            self._cooldown[i] = 0.0
            self._last_req[i] = time.time()
            self._use_count[i] += 1
            return self._keys[i], i

    def ban(self, idx, duration=None):
        """Помечаем ключ как забаненный на duration секунд (429 или ошибка)."""
        secs = duration if duration is not None else self.COOLDOWN_429
        with self._lock:
            self._cooldown[idx]  = time.time() + secs
            self._err_count[idx] += 1
        if DEBUG or self._n > 1:
            key_short = self._keys[idx][:12] + "…" if len(self._keys[idx]) > 12 else self._keys[idx]
            print(f"  ⏳ GMGN ключ #{idx+1} ({key_short}) → cooldown {secs}s")

    def status(self):
        """Краткий статус всех ключей (для --debug)."""
        now = time.time()
        lines = []
        for i, k in enumerate(self._keys):
            cd = self._cooldown[i]
            state = (f"cooldown {cd-now:.0f}s" if cd > now else "ОК")
            lines.append(
                f"  ключ #{i+1} {k[:14]}… → {state} "
                f"| использований: {self._use_count[i]} | ошибок: {self._err_count[i]}"
            )
        return "\n".join(lines)


_gmgn_pool    = _GmgnKeyPool(_load_gmgn_keys())
GMGN_API_KEY  = _gmgn_pool._keys[0]   # обратная совместимость (одиночный ключ)

# ── Cloudscraper для GMGN (обход Cloudflare) ──────────────────────────────────
# GMGN openapi.gmgn.ai и gmgn.ai/vas/api стоят за Cloudflare.
# requests возвращает HTML "Just a moment..." — cloudscraper имитирует браузер.
try:
    import cloudscraper as _cloudscraper
    _gmgn_scraper = _cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "mobile": False},
        delay=1,
    )
    _CLOUDSCRAPER_OK = True
except ImportError:
    _gmgn_scraper = None
    _CLOUDSCRAPER_OK = False
    print("  ⚠ cloudscraper не установлен. Установи: pip install cloudscraper")

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich import box
    RICH = True
except ImportError:
    RICH = False

try:
    from free_solscan_api.api import send_api_request as _solscan_req
    SOLSCAN_OK = True
except ImportError:
    _solscan_req = None
    SOLSCAN_OK = False

KNOWN_WALLETS = {
    "5tzFkiKscXHK5ZXCGbCAbZseha4ZRmHZmFeFiCNkGXTG": "Binance Hot",
    "AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2": "Binance",
    "2ojv9BAiHUrvsm9gxDe7fJSzbNZSJcxZvf8dqmWGHG8S": "Coinbase",
    "H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS": "Kraken",
    "FWznbcNXWQuHTawe9RxvQ2LdCENssh12dsznf4RiouN5": "OKX",
    "9un5wqE3q4oCjyrDkwsdD48KteCJitQX5978Vh7KKxHo": "OKX 2",
    "GJRs4FwHtemZ5ZE9x3FNvJ8TMwitKTh21yxdRPqn7npE": "Bybit Hot",
    "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB":  "Jupiter",
}

# ── Solscan API (free_solscan_api — api-v2.solscan.io/v2) ────────────────────
def _solscan(path: str, params: dict):
    """
    Прямой вызов любого endpoint Solscan v2.
    Использует cloudscraper + авто-генерацию sol-aut заголовка.
    Возвращает dict с данными или None при ошибке.
    """
    if not SOLSCAN_OK or not _solscan_req:
        return None
    try:
        result = _solscan_req(path, url_params=params)
        return result
    except Exception as e:
        if DEBUG:
            print(f"  DEBUG solscan {path}: {e}")
        return None


def solscan_token_creator(ca: str) -> str:
    """
    Получаем создателя токена через Solscan /token/meta.
    Возвращает адрес creator/mintAuthority или пустую строку.
    Работает даже для старых токенов, не требует временного окна.
    """
    if not SOLSCAN_OK:
        return ""
    data = _solscan("/token/meta", {"address": ca})
    if not isinstance(data, dict):
        return ""
    for field in ("creator", "mintAuthority", "freezeAuthority", "updateAuthority"):
        addr = data.get(field) or ""
        if addr and addr not in SYSTEM_PROGRAMS and 30 <= len(addr) <= 50:
            if DEBUG:
                print(f"  DEBUG solscan_token_creator ({field}): {addr[:20]}…")
            return addr
    return ""


def solscan_wallet_info(addr: str) -> dict:
    """
    Профиль кошелька через Solscan /account.
    Возвращает dict с balance, txCount, ownerProgram и т.д.
    """
    if not SOLSCAN_OK:
        return {}
    data = _solscan("/account", {"address": addr})
    return data if isinstance(data, dict) else {}


def solscan_wallet_transfers(addr: str, page: int = 1, page_size: int = 100,
                              flow: str = None) -> list:
    """
    История SOL-переводов кошелька через Solscan /account/transfer.
    flow="in" → только входящие, flow="out" → исходящие, None → все.
    Используется как дополнительный источник для funding chain анализа.
    """
    if not SOLSCAN_OK:
        return []
    data = _solscan("/account/transfer", {
        "address": addr,
        "page": page,
        "page_size": page_size,
        "remove_spam": "true",
        "exclude_amount_zero": "true",
        "flow": flow,
    })
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("data") or data.get("transfers") or []
    return []


def solscan_token_holders(ca: str, page: int = 1, page_size: int = 100) -> list:
    """
    Топ холдеры токена через Solscan /token/holders.
    Возвращает список dict {address, amount, decimals, rank}.
    """
    if not SOLSCAN_OK:
        return []
    data = _solscan("/token/holders", {
        "address": ca,
        "page": page,
        "page_size": page_size,
    })
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("data") or data.get("holders") or []
    return []


# ── Monitor tree integration ─────────────────────────────────────────────────
def load_monitor_tree(db_path=None):
    """
    Загружает дерево кошельков из monitor.db (создаётся monitor.py).
    Возвращает dict: {address: {depth, path, master, master_label}}

    Включает:
      - wallets таблица: прослойки глубины 1-4 (hop-кошельки)
      - masters таблица: корневые мастер-кошельки (depth=0)
    Если файла нет — возвращает пустой dict (graceful fallback).
    """
    if db_path is None:
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "monitor.db")
    if not os.path.exists(db_path):
        return {}
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        tree = {}

        # 1. Прослойки (depth 1-4)
        rows = conn.execute(
            "SELECT w.address, w.depth, w.path_json, w.master, m.label "
            "FROM wallets w LEFT JOIN masters m ON w.master = m.address"
        ).fetchall()
        for row in rows:
            tree[row["address"]] = {
                "depth":        row["depth"],
                "path":         json.loads(row["path_json"] or "[]"),
                "master":       row["master"] or "",
                "master_label": row["label"]  or "",
            }

        # 2. Мастер-кошельки (depth=0) — сами себе путь
        masters = conn.execute("SELECT address, label FROM masters").fetchall()
        for m in masters:
            addr  = m["address"]
            label = m["label"] or ""
            if addr not in tree:          # не перезаписываем если уже есть как wallet
                tree[addr] = {
                    "depth":        0,
                    "path":         [addr],
                    "master":       addr,
                    "master_label": label,
                }

        conn.close()
        return tree
    except Exception:
        return {}

def load_monitor_token(ca, db_path=None):
    """
    Проверяет был ли CA уже задетектирован monitor.py (таблица tokens).
    Если да — возвращает мастер-кошелёк и метку.
    Это самый надёжный способ: monitor уже установил связь токен → мастер.
    """
    if db_path is None:
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "monitor.db")
    if not os.path.exists(db_path):
        return None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT t.wallet, t.master, t.symbol, t.name, t.detected_at, m.label "
            "FROM tokens t LEFT JOIN masters m ON t.master = m.address "
            "WHERE t.ca = ? LIMIT 1",
            (ca,)
        ).fetchone()
        conn.close()
        if row and row["master"]:
            return {
                "deployer":     row["wallet"]  or "",
                "master":       row["master"]  or "",
                "master_label": row["label"]   or "",
                "symbol":       row["symbol"]  or "",
                "detected_at":  row["detected_at"],
            }
    except Exception:
        pass
    return None


def _init_monitor_db(conn):
    """Создаёт все таблицы monitor.db если их нет. Вызывается при каждом открытии."""
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS masters (
        address     TEXT PRIMARY KEY,
        label       TEXT DEFAULT '',
        added_at    INTEGER,
        active      INTEGER DEFAULT 1,
        source_ca   TEXT DEFAULT ''
    );
    CREATE TABLE IF NOT EXISTS wallets (
        address         TEXT PRIMARY KEY,
        master          TEXT,
        role            TEXT DEFAULT 'unknown',
        depth           INTEGER,
        parent          TEXT,
        path_json       TEXT,
        amount_received REAL,
        first_seen      INTEGER,
        last_sig        TEXT,
        last_tx_scan    INTEGER DEFAULT 0,
        last_tok_scan   INTEGER DEFAULT 0,
        init_mode       INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS cluster_wallets (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        master_addr     TEXT,
        source_ca       TEXT,
        cluster_label   TEXT,
        wallet_addr     TEXT,
        role            TEXT,
        depth           INTEGER,
        funded_target   TEXT,
        amount_sol      REAL,
        added_at        INTEGER,
        UNIQUE(master_addr, wallet_addr)
    );
    CREATE TABLE IF NOT EXISTS cluster_tokens (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        master_addr     TEXT,
        token_ca        TEXT,
        token_symbol    TEXT,
        token_name      TEXT,
        deploy_ts       INTEGER DEFAULT 0,
        added_at        INTEGER,
        UNIQUE(master_addr, token_ca)
    );
    """)
    # Миграции: добавляем колонки если их нет (для старых БД)
    for col_def in [
        ("masters",  "source_ca",  "TEXT DEFAULT ''"),
        ("wallets",  "role",       "TEXT DEFAULT 'unknown'"),
    ]:
        try:
            conn.execute(f"ALTER TABLE {col_def[0]} ADD COLUMN {col_def[1]} {col_def[2]}")
            conn.commit()
        except Exception:
            pass  # колонка уже есть


def save_master_to_monitor(addr: str, label: str = "", source_ca: str = "",
                           db_path=None) -> bool:
    """
    Сохраняет мастер-кошелёк в monitor.db для постоянного отслеживания.
    Создаёт таблицы если их нет (monitor.py не нужен для этого).
    Возвращает True если добавлен, False если уже существует.
    """
    if not addr or len(addr) < 30:
        return False
    if db_path is None:
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "monitor.db")
    try:
        conn = sqlite3.connect(db_path)
        _init_monitor_db(conn)
        existing = conn.execute(
            "SELECT address FROM masters WHERE address=?", (addr,)
        ).fetchone()
        if existing:
            if label:
                conn.execute(
                    "UPDATE masters SET label=? WHERE address=? AND (label IS NULL OR label='')",
                    (label, addr)
                )
                conn.commit()
            conn.close()
            return False   # уже есть
        ts = int(time.time())
        conn.execute(
            "INSERT INTO masters (address, label, added_at, active, source_ca) VALUES (?,?,?,1,?)",
            (addr, label, ts, source_ca)
        )
        conn.execute(
            "INSERT OR IGNORE INTO wallets "
            "(address, master, role, depth, parent, path_json, amount_received, first_seen) "
            "VALUES (?,?,'master_candidate',0,NULL,'[]',0,?)",
            (addr, addr, ts)
        )
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        if DEBUG:
            print(f"  DEBUG save_master_to_monitor: {e}")
        return False


def save_cluster_to_monitor(master_addr: str, cluster_label: str, source_ca: str,
                            deployer: str, chain_nodes: list, db_path=None):
    """
    Сохраняет ВЕСЬ кластер в monitor.db:
      - deployer → role=deployer, depth=-1
      - каждый узел цепи (прослойки, мастер-кандидат) → со своей ролью/глубиной
      - запись в cluster_tokens что source_ca принадлежит этому кластеру

    chain_nodes: список dict из helius_trace_funding_deep
      {funder, depth, funded_target, amount_sol, is_proxy, f_created, ...}
    """
    if not master_addr or len(master_addr) < 30:
        return
    if db_path is None:
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "monitor.db")
    try:
        conn = sqlite3.connect(db_path)
        _init_monitor_db(conn)
        ts = int(time.time())

        # Убеждаемся что master в таблице masters
        conn.execute(
            "INSERT OR IGNORE INTO masters (address, label, added_at, active, source_ca) "
            "VALUES (?,?,?,1,?)",
            (master_addr, cluster_label, ts, source_ca)
        )

        # Сохраняем деплоер (depth=-1 относительно цепи финансирования)
        if deployer and len(deployer) >= 30:
            conn.execute(
                "INSERT OR IGNORE INTO cluster_wallets "
                "(master_addr, source_ca, cluster_label, wallet_addr, role, depth, "
                " funded_target, amount_sol, added_at) "
                "VALUES (?,?,?,?,'deployer',-1,NULL,0,?)",
                (master_addr, source_ca, cluster_label, deployer, ts)
            )
            conn.execute(
                "INSERT OR IGNORE INTO wallets "
                "(address, master, role, depth, parent, path_json, amount_received, first_seen) "
                "VALUES (?,?,'deployer',-1,?,?,0,?)",
                (deployer, master_addr, master_addr, f'["{master_addr}"]', ts)
            )

        # Сохраняем каждый узел цепи
        for node in (chain_nodes or []):
            w     = node.get("funder") or ""
            if not w or len(w) < 30:
                continue
            d     = node.get("depth", 0)
            role  = "master_candidate" if w == master_addr else (
                    "proxy" if node.get("is_proxy") else "master")
            amt   = float(node.get("amount_sol") or 0)
            tgt   = node.get("funded_target") or ""
            parent = tgt if tgt else master_addr

            conn.execute(
                "INSERT OR IGNORE INTO cluster_wallets "
                "(master_addr, source_ca, cluster_label, wallet_addr, role, depth, "
                " funded_target, amount_sol, added_at) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (master_addr, source_ca, cluster_label, w, role, d, tgt, amt, ts)
            )
            conn.execute(
                "INSERT OR IGNORE INTO wallets "
                "(address, master, role, depth, parent, path_json, amount_received, first_seen) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (w, master_addr, role, d + 1, parent,
                 json.dumps([master_addr]), amt, ts)
            )

        # Сохраняем source_ca как токен кластера
        if source_ca:
            sym = cluster_label.split(":")[0] if ":" in cluster_label else cluster_label
            conn.execute(
                "INSERT OR IGNORE INTO cluster_tokens "
                "(master_addr, token_ca, token_symbol, added_at) VALUES (?,?,?,?)",
                (master_addr, source_ca, sym, ts)
            )

        conn.commit()
        conn.close()
        if DEBUG:
            print(f"  DEBUG save_cluster: кластер {cluster_label} сохранён "
                  f"({len(chain_nodes)} узлов + деплоер)")
    except Exception as e:
        if DEBUG:
            print(f"  DEBUG save_cluster_to_monitor: {e}")


def get_cluster_track_record(master_addr: str, db_path=None) -> list:
    """
    Возвращает список токенов которые задеплоил этот кластер (из cluster_tokens).
    Список dict: {token_ca, token_symbol, deploy_ts, added_at}
    """
    if not master_addr:
        return []
    if db_path is None:
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "monitor.db")
    try:
        conn = sqlite3.connect(db_path)
        _init_monitor_db(conn)
        rows = conn.execute(
            "SELECT token_ca, token_symbol, deploy_ts, added_at "
            "FROM cluster_tokens WHERE master_addr=? ORDER BY added_at DESC",
            (master_addr,)
        ).fetchall()
        conn.close()
        return [{"token_ca": r[0], "token_symbol": r[1],
                 "deploy_ts": r[2], "added_at": r[3]} for r in rows]
    except Exception:
        return []


def get_all_cluster_wallets(master_addr: str, db_path=None) -> list:
    """
    Возвращает все кошельки кластера из cluster_wallets.
    """
    if not master_addr:
        return []
    if db_path is None:
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "monitor.db")
    try:
        conn = sqlite3.connect(db_path)
        rows = conn.execute(
            "SELECT wallet_addr, role, depth, funded_target, amount_sol, source_ca "
            "FROM cluster_wallets WHERE master_addr=? ORDER BY depth",
            (master_addr,)
        ).fetchall()
        conn.close()
        return [{"wallet": r[0], "role": r[1], "depth": r[2],
                 "funded_target": r[3], "amount_sol": r[4], "source_ca": r[5]}
                for r in rows]
    except Exception:
        return []


def check_insider_connections(wallets_list, tree):
    """
    Проверяет список кошельков на принадлежность к monitor-дереву.
    wallets_list: список dict с ключом "wallet"
    tree: dict из load_monitor_tree()
    Возвращает список dict с добавленными полями depth/path/master_label.
    """
    if not tree:
        return []
    result = []
    for item in wallets_list:
        addr = item.get("wallet") or item if isinstance(item, str) else ""
        if addr and addr in tree:
            info = tree[addr]
            merged = dict(item)
            merged["_insider_depth"]  = info["depth"]
            merged["_insider_path"]   = info["path"]
            merged["_insider_master"] = info["master"]
            merged["_insider_label"]  = info["master_label"]
            result.append(merged)
    return result

# ── Time sync ─────────────────────────────────────────────────────────────────
_time_offset = None

def get_server_time():
    """Синхронизируем часы с сервером GMGN. Нужно для AUTH_TIMESTAMP."""
    global _time_offset
    if _time_offset is not None:
        return

    # Попытка 1: читаем Date-заголовок от GMGN
    try:
        from email.utils import parsedate_to_datetime
        r = requests.get(
            f"{GMGN_BASE}/v1/user/info",
            headers={"X-APIKEY": "x"},
            params={"timestamp": "1", "client_id": "x"},
            timeout=6,
        )
        date_str = r.headers.get("Date", "")
        if date_str:
            server_ts = int(parsedate_to_datetime(date_str).timestamp())
            _time_offset = server_ts - int(time.time())
            return
    except Exception:
        pass

    # Попытка 2: worldtimeapi
    try:
        r = requests.get("https://worldtimeapi.org/api/timezone/UTC", timeout=6)
        data = r.json()
        server_ts = int(data.get("unixtime", 0))
        if server_ts > 1_000_000_000:
            _time_offset = server_ts - int(time.time())
            return
    except Exception:
        pass

    # Попытка 3: Cloudflare
    try:
        r = requests.get("https://cloudflare.com/cdn-cgi/trace", timeout=6)
        for line in r.text.splitlines():
            if line.startswith("ts="):
                server_ts = int(float(line.split("=")[1]))
                _time_offset = server_ts - int(time.time())
                return
    except Exception:
        pass

    _time_offset = 0  # нет источника — используем локальное время

def server_now():
    get_server_time()
    return int(time.time()) + (_time_offset or 0)

# ── GMGN API ──────────────────────────────────────────────────────────────────

def gmgn_get(path, params=None):
    """
    GMGN OpenAPI запрос с ротацией ключей.

    При 429 — текущий ключ уходит в cooldown (90с), функция немедленно
    повторяет запрос со следующим ключом из пула (до N*2 попыток).
    Каждый ключ имеет свой rate-limiter (0.8с между запросами).
    """
    import json as _json

    p = {"timestamp": server_now(), "client_id": str(uuid.uuid4())}
    if params:
        p.update(params)

    max_tries = max(3, _gmgn_pool.count * 2)   # минимум 3 попытки, максимум 2 полных круга

    for attempt in range(max_tries):
        key, kid = _gmgn_pool.acquire()

        try:
            _sess = _gmgn_scraper if _CLOUDSCRAPER_OK else requests
            r = _sess.get(
                f"{GMGN_BASE}{path}",
                headers={"X-APIKEY": key, "Content-Type": "application/json"},
                params=p, timeout=15,
            )
            # GMGN иногда возвращает голый null/true для несуществующих endpoints
            raw = r.text.strip()
            if raw in ("null", "true", "false", ""):
                if DEBUG:
                    print(f"  DEBUG {path}: ответ = {raw!r} (endpoint не существует)")
                return None

            # Cloudflare challenge — openapi.gmgn.ai заблокирован.
            # Не баним ключ (он не виноват) и не ретраим — сразу None.
            if raw.startswith("<!DOCTYPE") or "Just a moment" in raw:
                if DEBUG:
                    print(f"  DEBUG {path}: Cloudflare challenge (scraper не помог)")
                return None

            try:
                d = _json.loads(raw)
            except _json.JSONDecodeError:
                if DEBUG:
                    print(f"  DEBUG {path}: не JSON → {raw[:120]!r}")
                return None

            if DEBUG:
                print(f"\n── DEBUG {path} [key#{kid+1}] ──")
                print(_json.dumps(d, ensure_ascii=False, indent=2)[:3000])
                print("──────────────────")

            if isinstance(d, dict):
                code = d.get("code")

                # 429 Rate limit → баним ЭТОТ ключ, следующая итерация возьмёт другой
                if code == 429:
                    _gmgn_pool.ban(kid, _GmgnKeyPool.COOLDOWN_429)
                    if _gmgn_pool.count == 1:
                        # Один ключ — просто ждём
                        wait_s = min(30, 5 * (attempt + 1))
                        print(f"  ⏳ GMGN 429 (единственный ключ) → жду {wait_s}s")
                        time.sleep(wait_s)
                    else:
                        print(f"  ⏳ GMGN 429 ключ #{kid+1} → переключаюсь на следующий")
                    continue   # следующая итерация → acquire() возьмёт другой ключ

                # 401/403 Неверный ключ → помечаем как сломанный
                if code in (401, 403) or r.status_code in (401, 403):
                    _gmgn_pool.ban(kid, 3600)   # на 1 час
                    print(f"  ⚠ GMGN ключ #{kid+1} невалиден (401/403) → пробую следующий")
                    continue

                if code == 0:
                    return d.get("data")

                # Другие ошибки — не повторяем, просто возвращаем None
                msg = d.get("message") or d.get("msg") or ""
                if msg and "not found" not in msg.lower() and "invalid" not in msg.lower():
                    if DEBUG:
                        print(f"  GMGN {path} [key#{kid+1}]: {msg}")
                return None

            return None

        except requests.exceptions.Timeout:
            if DEBUG:
                print(f"  DEBUG {path} [key#{kid+1}]: timeout (попытка {attempt+1})")
            _gmgn_pool.ban(kid, _GmgnKeyPool.COOLDOWN_ERR)
            continue
        except Exception as e:
            if DEBUG:
                print(f"  DEBUG {path} [key#{kid+1}]: exception {e}")
            return None

    return None

def g_info(ca):
    return gmgn_get("/v1/token/info",     {"chain": CHAIN, "address": ca}) or {}

def g_sec(ca):
    return gmgn_get("/v1/token/security", {"chain": CHAIN, "address": ca}) or {}

def g_stat(ca):
    """Дополнительная статистика — bundle, sniper, phishing могут быть здесь."""
    for path in [
        "/v1/token/stat",
        "/v1/token/extra_info",
        "/v1/token/analysis",
        "/v1/token/snipe_info",
        "/v1/token/holder_stat",
        "/v1/token/security_detail",
        "/v1/market/token_bundle_info",
        "/v1/market/token_sniper_info",
        "/v1/market/token_launch_info",
        "/v1/token/launch_info",
        "/v1/token/audit",
    ]:
        d = gmgn_get(path, {"chain": CHAIN, "address": ca})
        if d:
            return d
    return {}

def g_pool(ca):
    return gmgn_get("/v1/token/pool_info",{"chain": CHAIN, "address": ca}) or {}

# ── GMGN Frontend API (без API ключа) ────────────────────────────────────────
def _vas_decompress(content: bytes) -> str:
    """
    Декомпрессия ответа vas/api.
    Сервер игнорирует Accept-Encoding: identity и шлёт brotli (\x1b) или gzip (\x1f\x8b).
    Пробуем brotli → zlib → raw UTF-8.
    """
    if not content:
        return ""
    # Brotli: магический байт 0x1b (но не всегда — проверяем несколько вариантов)
    if content[0] == 0x1b or (len(content) > 1 and content[:2] in (b'\x1b\x9c', b'\x1b\xbc')):
        try:
            import brotli
            return brotli.decompress(content).decode("utf-8")
        except Exception:
            pass
        # brotlidecpy — альтернативная библиотека
        try:
            import brotlidecpy
            return brotlidecpy.decompress(content).decode("utf-8")
        except Exception:
            pass
    # gzip / deflate
    if content[:2] == b'\x1f\x8b':
        import zlib
        try:
            return zlib.decompress(content, 16 + zlib.MAX_WBITS).decode("utf-8")
        except Exception:
            pass
    # Plain text
    return content.decode("utf-8", errors="replace")


def gmgn_vas_holders(ca, limit=200):
    """
    Топ холдеры из фронтенд API GMGN (gmgn.ai/vas/api/v1/token_holders).
    Содержит: bundle tags, transfer_in, CEX funding, wallet age, profit.
    НЕ требует API ключа — использует device_id как браузер.

    ВАЖНО: используем plain requests (не cloudscraper) — cloudscraper добавляет
    свои Accept-Encoding заголовки и мешает нам контролировать сжатие.
    Вместо этого декодируем brotli/gzip вручную через _vas_decompress().
    """
    import json as _json
    params = {
        "device_id": str(uuid.uuid4()),
        "client_id": "gmgn_web_20260515-13238-14a62b6",
        "limit": limit,
        "cost": 20,
        "orderby": "amount_percentage",
        "direction": "desc",
    }
    # cloudscraper нужен для обхода Cloudflare (без него → 403).
    # Сервер шлёт brotli несмотря на заголовки — читаем r.content и декомпрессируем сами.
    _sess = _gmgn_scraper if _CLOUDSCRAPER_OK else requests.Session()
    try:
        r = _sess.get(
            f"https://gmgn.ai/vas/api/v1/token_holders/sol/{ca}",
            params=params,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept":           "application/json, text/plain, */*",
                "Accept-Language":  "en-US,en;q=0.9",
                "Accept-Encoding":  "identity",
                "Origin":           "https://gmgn.ai",
                "Referer":          f"https://gmgn.ai/sol/token/{ca}",
                "Sec-Fetch-Dest":   "empty",
                "Sec-Fetch-Mode":   "cors",
                "Sec-Fetch-Site":   "same-origin",
                "X-APIKEY":         GMGN_API_KEY,
            },
            timeout=15,
        )
        if DEBUG:
            print(f"  DEBUG vas_holders: HTTP {r.status_code}, content_len={len(r.content)}, "
                  f"encoding={r.headers.get('Content-Encoding','none')}, "
                  f"first_bytes={r.content[:4].hex() if r.content else 'empty'}")

        # Декомпрессия вручную — cloudscraper может проигнорировать Accept-Encoding: identity
        raw = _vas_decompress(r.content).strip()

        if not raw or raw in ("null", "true", "false"):
            if DEBUG:
                print("  DEBUG vas_holders: пустой ответ после декомпрессии")
            return []
        if raw.startswith("<!DOCTYPE") or "Just a moment" in raw:
            if DEBUG:
                print("  DEBUG vas_holders: Cloudflare challenge")
            return []
        try:
            d, _ = _json.JSONDecoder().raw_decode(raw)
        except _json.JSONDecodeError as e:
            if DEBUG:
                print(f"  DEBUG vas_holders: JSON error {e} | raw[:80]={raw[:80]!r}")
            return []
        code = d.get("code")
        if DEBUG:
            # Показываем структуру: code, кол-во холдеров, первый элемент
            _data  = d.get("data") or {}
            _lst   = _data.get("list") or _data.get("holders") or []
            _first = _lst[0] if _lst else {}
            print(f"  DEBUG vas_holders: code={code!r}, holders={len(_lst)}, "
                  f"first_keys={list(_first.keys())[:10]}")
        # code может быть int 0, string "0", или отсутствовать (тогда берём data напрямую)
        _data = d.get("data") or {}
        if code in (0, "0") or (code is None and _data):
            lst = _data.get("list") or _data.get("holders") or _data.get("data") or []
            if isinstance(lst, list) and lst:
                return lst
            # Иногда сам d и есть список
            if isinstance(d, list):
                return d
        if DEBUG:
            print(f"  DEBUG vas_holders: code={code!r} не подошёл или список пуст")
        return []
    except Exception as e:
        if DEBUG:
            print(f"  DEBUG vas_holders: exception {e}")
    return []


def analyze_vas_holders(holders):
    """
    Анализируем список холдеров из gmgn.ai/vas/api/v1/token_holders.

    Поля из ответа GMGN (проверено на MOMUS):
      amount_percentage  — доля supply (дробная: 0.229 = 22.9%)
      maker_token_tags   — ["bundler", "rat_trader", "top_holder", ...]
      transfer_in        — True если получил токены переводом (не с DEX)
      token_transfer_in  — {"address": <кто прислал>, "timestamp": ...}
      buy_tx_count_cur   — сколько раз докупал с рынка
      history_bought_cost— USD потрачено на докупку
      native_transfer    — {"name": "Binance", ...} — откуда пришли SOL
      is_new             — свежесозданный кошелёк
      start_holding_at   — когда первый раз получил токен
      created_at         — когда создан кошелёк
      usd_value          — текущая стоимость позиции в USD
      profit             — нереализованный + реализованный PnL
    """
    if not holders:
        return {}

    bundle_pct        = 0.0
    transfer_in_pct   = 0.0
    distributor_count = defaultdict(float)  # addr → суммарный % supply которым переводил
    pattern_b         = []
    cex_names         = []

    for h in holders:
        # amount_percentage может быть дробным (0.229 = 22.9%) или уже в % (22.9)
        _raw_pct = float(h.get("amount_percentage") or h.get("share_percentage") or
                         h.get("percent") or h.get("pct") or 0)
        pct = _raw_pct if _raw_pct <= 1.0 else _raw_pct / 100  # нормализуем к доле
        maker_tags  = (h.get("maker_token_tags") or h.get("tags") or
                       h.get("wallet_tags") or [])
        wallet_addr = (h.get("account_address") or h.get("wallet_address") or
                       h.get("address") or h.get("wallet") or "")

        # ── Bundle ────────────────────────────────────────────────────────
        if "bundler" in maker_tags:
            bundle_pct += pct

        # ── Transfer-in ("фишинг" паттерн) ────────────────────────────────
        ti        = h.get("token_transfer_in") or {}
        dist_addr = ti.get("address") or ""

        if h.get("transfer_in") and dist_addr and dist_addr != wallet_addr:
            transfer_in_pct += pct
            distributor_count[dist_addr] += pct  # кто сколько % раздал

            buy_count   = int(h.get("buy_tx_count_cur")   or 0)
            bought_cost = float(h.get("history_bought_cost") or 0)

            # Паттерн B: получил токены переводом И ещё докупил с рынка
            if buy_count > 0 or bought_cost > 10:
                nt  = h.get("native_transfer") or {}
                cex = nt.get("name") or ""
                pattern_b.append({
                    "wallet":        wallet_addr,
                    "received_from": dist_addr,
                    "buy_count":     buy_count,
                    "bought_cost":   bought_cost,
                    "usd_value":     float(h.get("usd_value")  or 0),
                    "pct":           pct * 100,
                    "is_new":        bool(h.get("is_new")),
                    "cex":           cex,
                    "start_holding": h.get("start_holding_at"),
                    "created_at":    h.get("created_at"),
                    "profit":        float(h.get("profit") or 0),
                    "tags":          h.get("tags") or [],
                    "wallet_tag":    h.get("wallet_tag_v2") or "",
                })

        # ── CEX funding ───────────────────────────────────────────────────
        nt = h.get("native_transfer") or {}
        if nt.get("name"):
            cex_names.append(nt["name"])

    # Главный дистрибьютор — тот, кто переводил токены наибольшему % supply
    main_distributor = (max(distributor_count, key=distributor_count.get)
                        if distributor_count else None)
    dist_pct = distributor_count.get(main_distributor, 0) * 100 if main_distributor else 0

    # Сортируем Паттерн B: сначала больше покупок, потом по cost
    pattern_b.sort(key=lambda x: (-x["buy_count"], -x["bought_cost"]))

    return {
        "bundle_pct":       bundle_pct * 100,       # в %
        "transfer_in_pct":  transfer_in_pct * 100,   # в %
        "main_distributor": main_distributor,
        "dist_pct":         dist_pct,                # % supply который раздал дистрибьютор
        "pattern_b":        pattern_b,
        "cex_names":        cex_names,
        "n_holders":        len(holders),
    }

def g_holders(ca, n=100):
    d = gmgn_get("/v1/market/token_top_holders", {"chain": CHAIN, "address": ca, "limit": n})
    if isinstance(d, list): return d
    if isinstance(d, dict):
        # API возвращает {"list": [...]} или {"holders": [...]}
        lst = d.get("list") or d.get("holders") or d.get("data") or []
        if isinstance(lst, list):
            # нормализуем ключ wallet
            out = []
            for item in lst:
                addr = item.get("address") or item.get("account_address") or ""
                if addr:
                    out.append({**item, "wallet": addr, "account_address": addr})
            return out
    return []

def g_trades(ca, limit=200):
    """Сделки по токену (ранние покупатели)."""
    for path in ["/v1/token/trades", "/v1/token/swaps", "/v1/market/token_trades",
                 "/v1/token/top_traders"]:
        d = gmgn_get(path, {
            "chain": CHAIN, "address": ca, "limit": limit,
            "sort_by": "block_time", "sort_order": "asc",
        })
        if d is None:
            continue
        if isinstance(d, list) and d:
            return d
        if isinstance(d, dict):
            for key in ["trades", "swaps", "data", "records", "traders"]:
                v = d.get(key)
                if isinstance(v, list) and v:
                    return v
    return []

# ── DexScreener (бесплатный, без auth) ───────────────────────────────────────
def dexscreener_get(ca):
    """
    Возвращает лучшую пару для токена с DexScreener.
    Даёт: price, MC (fdv), volume, liquidity, dex, age, website, socials.
    """
    try:
        r = requests.get(
            f"https://api.dexscreener.com/latest/dex/tokens/{ca}",
            timeout=10,
        )
        data = r.json()
        pairs = data.get("pairs") or []
        if not pairs:
            return {}
        # Берём пару с наибольшей ликвидностью
        pairs_sol = [p for p in pairs if p.get("chainId") == "solana"]
        if not pairs_sol:
            pairs_sol = pairs
        best = max(pairs_sol, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
        return best
    except Exception:
        return {}

def g_activity(w, n=80):
    d = gmgn_get("/v1/user/wallet_activity", {"chain": CHAIN, "wallet_address": w, "limit": n})
    if isinstance(d, list): return d
    if isinstance(d, dict): return d.get("activities", d.get("data", []))
    return []

def g_holdings(w):
    d = gmgn_get("/v1/user/wallet_holdings", {"chain": CHAIN, "wallet_address": w})
    if isinstance(d, list): return d
    if isinstance(d, dict): return d.get("holdings", d.get("data", []))
    return []

def g_stats(w):
    return gmgn_get("/v1/user/wallet_stats", {"chain": CHAIN, "wallet_address": w, "period": "30d"}) or {}

def g_created(w):
    d = gmgn_get("/v1/user/created_tokens", {"chain": CHAIN, "wallet_address": w})
    if isinstance(d, list): return d
    if isinstance(d, dict): return d.get("tokens", d.get("data", []))
    return []

def gmgn_top_traders(ca, limit=100):
    """
    Топ трейдеры по токену — включает тех кто УЖЕ ВЫШЕЛ (realized PnL).
    Фолбэк когда gmgn_vas_holders пуст (токен старый или неактивный).
    Возвращает список с полями: address/wallet, realized_profit, buy/sell counts, tags.
    """
    for path, extra in [
        ("/v1/market/token_top_holders", {"orderby": "amount_percentage", "direction": "desc", "limit": limit}),
        ("/v1/token/top_traders",  {"orderby": "realized_profit", "direction": "desc"}),
        ("/v1/token/top_holders",  {}),
        ("/v1/market/token_trades",{"sort_by": "profit", "sort_order": "desc"}),
    ]:
        d = gmgn_get(path, {"chain": CHAIN, "address": ca, "limit": limit, **extra})
        if d is None:
            continue
        lst = None
        if isinstance(d, list) and d:
            lst = d
        elif isinstance(d, dict):
            for k in ["traders", "holders", "data", "list", "records"]:
                v = d.get(k)
                if isinstance(v, list) and v:
                    lst = v
                    break
        if lst:
            # Нормализуем ключи: разные эндпоинты называют адрес по-разному
            out = []
            for item in lst:
                addr = (item.get("address") or item.get("wallet") or
                        item.get("account_address") or "")
                if not addr:
                    continue
                out.append({**item, "wallet": addr, "account_address": addr,
                             "_source": path})
            if out:
                return out
    return []

# ── Helius ────────────────────────────────────────────────────────────────────
def helius_get_creator(ca):
    """
    Получаем создателя токена через Helius DAS API (getAsset).
    Возвращает адрес создателя или None.

    DAS-ответ содержит:
      result.authorities[0].address  ← обычно создатель минта
      result.creators[0].address     ← для NFT/Metaplex токенов
      result.mint_extensions.mint_close_authority.close_authority
    """
    if not HELIUS_KEY:
        return None
    result = _helius_rpc("getAsset", [ca])
    if not isinstance(result, dict):
        return None

    if DEBUG:
        import json as _j
        print(f"\n── DEBUG getAsset ──")
        print(_j.dumps(result, ensure_ascii=False, indent=2)[:2000])
        print("──────────────────")

    # Приоритет: authorities → creators → ownership.owner
    authorities = result.get("authorities") or []
    for auth in authorities:
        addr = auth.get("address") or ""
        if addr and addr not in SYSTEM_PROGRAMS:
            return addr

    creators = result.get("creators") or []
    for cr in creators:
        addr = cr.get("address") or ""
        if addr and addr not in SYSTEM_PROGRAMS:
            return addr

    owner = (result.get("ownership") or {}).get("owner") or ""
    if owner and owner not in SYSTEM_PROGRAMS:
        return owner

    return None


def rugcheck_creator(ca: str) -> str:
    """
    Rugcheck публичный API — возвращает creator токена.
    Не требует ключа, работает для всех Solana токенов включая Meteora.
    Endpoint: GET https://api.rugcheck.xyz/v1/tokens/{mint}/report
    Также берём score и risks для Этапа 1.
    """
    import json as _j
    try:
        r = requests.get(
            f"https://api.rugcheck.xyz/v1/tokens/{ca}/report",
            headers={"Accept": "application/json"},
            timeout=15,
        )
        if r.status_code != 200:
            if DEBUG:
                print(f"  DEBUG rugcheck_creator: HTTP {r.status_code}")
            return None
        d = r.json()
        if DEBUG:
            # Показываем только ключевые поля
            _short = {k: v for k, v in d.items()
                      if k in ("creator", "mintAuthority", "score", "risks")}
            print(f"  DEBUG rugcheck_creator: {_j.dumps(_short, ensure_ascii=False)[:400]}")
        for field in ("creator", "mintAuthority", "freezeAuthority", "updateAuthority"):
            addr = d.get(field) or ""
            if addr and addr not in SYSTEM_PROGRAMS and 30 <= len(addr) <= 50:
                if DEBUG:
                    print(f"  DEBUG rugcheck_creator ({field}): {addr[:20]}…")
                return addr
        # Иногда creator вложен в token объект
        tok = d.get("token") or {}
        for field in ("creator", "mintAuthority", "updateAuthority"):
            addr = tok.get(field) or ""
            if addr and addr not in SYSTEM_PROGRAMS and 30 <= len(addr) <= 50:
                if DEBUG:
                    print(f"  DEBUG rugcheck_creator token.{field}: {addr[:20]}…")
                return addr
    except Exception as e:
        if DEBUG:
            print(f"  DEBUG rugcheck_creator: {e}")
    return None


def public_rpc_creator_from_addr(addr: str, label: str = "") -> str:
    """
    Ищем деплоера через публичные Solana RPC (несколько провайдеров).
    Пагинируем ВСЕ подписи до самой старой (до 50 страниц × 1000).
    Самая старая транзакция = создание аккаунта → fee payer = деплоер.

    Для Meteora DAMM v2: initializeMint происходит через CPI внутри
    транзакции создания пула — не ищем initializeMint, просто берём feePayer
    самой старой транзакции (она и есть создание).
    """
    PUBLIC_RPCS = [
        "https://api.mainnet-beta.solana.com",
        "https://rpc.ankr.com/solana",
    ]

    for rpc_url in PUBLIC_RPCS:
        result = _public_rpc_oldest_feepayer(rpc_url, addr, label)
        if result:
            return result
    return None


def _public_rpc_oldest_feepayer(rpc_url: str, addr: str, label: str) -> str:
    """Вспомогательная: пагинируем до самой старой tx и возвращаем её feePayer."""
    try:
        # Шаг 1: пагинация до конца истории аккаунта
        batch = None
        for limit_val in (1000, 100, 10):
            try:
                resp = requests.post(
                    rpc_url,
                    json={"jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress",
                          "params": [addr, {"limit": limit_val, "commitment": "finalized"}]},
                    timeout=20,
                )
                result = resp.json().get("result")
                if isinstance(result, list):
                    batch = result
                    break
            except Exception:
                pass
            if DEBUG:
                print(f"  DEBUG public_rpc({label}) [{rpc_url[:30]}]: "
                      f"limit={limit_val} → нет результата")

        if not batch:
            return None

        total_sigs = len(batch)
        # Пагинируем до самой старой транзакции (до 50 страниц)
        for _page in range(50):
            if len(batch) < 1000:
                break  # Последняя страница — меньше 1000 результатов
            oldest_sig = batch[-1]["signature"]
            time.sleep(0.3)  # пауза между запросами (rate limit)
            try:
                next_resp = requests.post(
                    rpc_url,
                    json={"jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress",
                          "params": [addr, {"limit": 1000, "before": oldest_sig,
                                            "commitment": "finalized"}]},
                    timeout=20,
                )
                nxt = next_resp.json().get("result")
            except Exception:
                nxt = None
            if not isinstance(nxt, list) or not nxt:
                break  # Конец истории
            batch = nxt
            total_sigs += len(batch)

        if DEBUG:
            print(f"  DEBUG public_rpc({label}): итого ~{total_sigs} подписей, "
                  f"старейшая страница = {len(batch)} tx")

        # Шаг 2: проверяем 20 старейших транзакций (от старой к новой)
        # Сохраняем первый валидный feePayer как fallback — вернём его если initMint не найден
        candidates    = list(reversed(batch))[:20]
        best_feepayer = None   # oldest valid feePayer из успешных запросов

        for sig_info in candidates:
            sig = sig_info.get("signature") or ""
            if not sig:
                continue
            time.sleep(0.3)  # пауза — public RPC rate limit ~3 req/s
            try:
                tx_resp = requests.post(
                    rpc_url,
                    json={"jsonrpc": "2.0", "id": 1, "method": "getTransaction",
                          "params": [sig, {"encoding": "jsonParsed",
                                           "maxSupportedTransactionVersion": 0,
                                           "commitment": "finalized"}]},
                    timeout=20,
                )
                tx = tx_resp.json().get("result")
            except Exception as tx_e:
                if DEBUG:
                    print(f"  DEBUG public_rpc({label}): tx fetch error {tx_e}")
                continue

            if not tx:
                continue

            msg  = tx.get("transaction", {}).get("message", {})
            keys = msg.get("accountKeys") or []
            # accountKeys[0] = fee payer (всегда первый подписант)
            fee_payer = (keys[0].get("pubkey") or "") if keys else ""

            if not fee_payer or fee_payer in SYSTEM_PROGRAMS or not (30 <= len(fee_payer) <= 50):
                continue

            # Сохраняем самый первый (oldest) валидный feePayer как кандидата
            if best_feepayer is None:
                best_feepayer = fee_payer

            # Проверяем initializeMint в outer + inner инструкциях
            _is_init_mint = False
            for ix in (msg.get("instructions") or []):
                p = ix.get("parsed") or {}
                if isinstance(p, dict) and "initializeMint" in (p.get("type") or ""):
                    _is_init_mint = True
                    break
            if not _is_init_mint:
                for inner_group in (tx.get("meta", {}).get("innerInstructions") or []):
                    for iix in (inner_group.get("instructions") or []):
                        p = iix.get("parsed") or {}
                        if isinstance(p, dict) and "initializeMint" in (p.get("type") or ""):
                            _is_init_mint = True
                            break
                    if _is_init_mint:
                        break

            if DEBUG:
                print(f"  DEBUG public_rpc({label}): sig={sig[:16]}… "
                      f"initMint={_is_init_mint} feePayer={fee_payer[:20]}…")

            # Нашли точное совпадение — возвращаем сразу
            if _is_init_mint:
                if DEBUG:
                    print(f"  DEBUG public_rpc({label}): ✅ initializeMint → {fee_payer[:20]}…")
                return fee_payer

        # initializeMint не нашли — возвращаем feePayer самой старой успешно загруженной tx
        if best_feepayer:
            if DEBUG:
                print(f"  DEBUG public_rpc({label}): fallback oldest feePayer = {best_feepayer[:20]}…")
            return best_feepayer

    except Exception as e:
        if DEBUG:
            print(f"  DEBUG public_rpc({label}): outer error {e}")
    return None


def helius_get_mint_authority(ca):
    """
    Быстрый способ (1 RPC): получаем mintAuthority из account info.
    Работает если создатель не отказался от прав минта.
    """
    if not HELIUS_KEY:
        return None
    try:
        result = _helius_rpc("getAccountInfo", [ca, {"encoding": "jsonParsed"}])
        if not result:
            return None
        info = ((result.get("value") or {})
                .get("data", {})
                .get("parsed", {})
                .get("info", {}))
        for field in ("mintAuthority", "freezeAuthority"):
            addr = info.get(field) or ""
            if addr and addr not in SYSTEM_PROGRAMS and 30 <= len(addr) <= 50:
                if DEBUG:
                    print(f"  DEBUG mint_authority ({field}): {addr[:20]}…")
                return addr
    except Exception as e:
        if DEBUG:
            print(f"  DEBUG helius_get_mint_authority: {e}")
    return None


def helius_get_creator_from_tx(ca):
    """
    Fallback: ищем деплоера по самой первой транзакции минта.
    Пагинируем назад до самой СТАРОЙ транзакции (это deployтранзакция).

    CA (mint address) имеет мало txn (~5-50), пагинация быстрая.
    Лимит: 5 страниц × 1000 = не больше 5000 txn на CA.
    """
    if not HELIUS_KEY:
        return None
    try:
        # Пагинируем назад до самой старой транзакции
        batch = _helius_rpc(
            "getSignaturesForAddress",
            [ca, {"limit": 1000, "commitment": "finalized"}]
        )
        if DEBUG:
            _cnt = len(batch) if isinstance(batch, list) else repr(batch)
            print(f"  DEBUG creator_from_tx: getSignaturesForAddress → {_cnt} сигнатур")
        if not isinstance(batch, list) or not batch:
            if DEBUG:
                print(f"  DEBUG creator_from_tx: пусто — getSignaturesForAddress вернул {repr(batch)[:100]}")
            return None

        # Идём назад пока не дойдём до конца истории (< 1000 результатов = последняя страница)
        for _page in range(5):
            if len(batch) < 1000:
                break  # Это последняя страница — batch[-1] и есть самый старый tx
            oldest_in_page = batch[-1]["signature"]
            next_batch = _helius_rpc(
                "getSignaturesForAddress",
                [ca, {"limit": 1000, "before": oldest_in_page, "commitment": "finalized"}]
            )
            if not isinstance(next_batch, list) or not next_batch:
                break
            batch = next_batch

        # Самая старая транзакция = деплой минта
        target_sig = batch[-1]["signature"]
        if DEBUG:
            print(f"  DEBUG creator_from_tx: oldest sig = {target_sig[:20]}… (page {_page+1})")


        # Парсим через RPC getTransaction (jsonParsed — видны все аккаунты)
        tx_result = _helius_rpc(
            "getTransaction",
            [target_sig, {"encoding": "jsonParsed",
                          "maxSupportedTransactionVersion": 0,
                          "commitment": "finalized"}]
        )
        if not tx_result:
            # Fallback: Enhanced API
            url = f"https://api.helius.xyz/v0/transactions?api-key={HELIUS_KEY}"
            r = requests.post(
                url, json={"transactions": [target_sig]}, timeout=15
            )
            txs = r.json()
            if not isinstance(txs, list) or not txs:
                return None
            tx_data = txs[0]
            fee_payer = tx_data.get("feePayer") or ""
            if fee_payer and fee_payer not in SYSTEM_PROGRAMS and 30 <= len(fee_payer) <= 50:
                if DEBUG:
                    print(f"  DEBUG creator_from_tx (enhanced): {fee_payer[:20]}…")
                return fee_payer
            for acct in (tx_data.get("accountData") or []):
                addr = acct.get("account") or ""
                if addr and addr not in SYSTEM_PROGRAMS and 30 <= len(addr) <= 50:
                    return addr
            return None

        # RPC jsonParsed: feePayer = accountKeys[0] (если signer)
        try:
            msg  = tx_result["transaction"]["message"]
            keys = msg.get("accountKeys") or []
            for k in keys:
                addr   = k.get("pubkey") or ""
                signer = k.get("signer", False)
                if signer and addr and addr not in SYSTEM_PROGRAMS and 30 <= len(addr) <= 50:
                    if DEBUG:
                        print(f"  DEBUG creator_from_tx (rpc): {addr[:20]}…")
                    return addr
        except Exception:
            pass

    except Exception as e:
        if DEBUG:
            print(f"  DEBUG helius_get_creator_from_tx: {e}")
    return None


def helius_creator_from_pool(pair_address: str):
    """
    Для Meteora DAMM v2: токен-минт — PDA без прямой истории.
    Но у пула (pairAddress из DexScreener) история есть.
    Первая транзакция пула = транзакция деплоя → fee payer = деплоер.

    pair_address: адрес пула из DexScreener (pairAddress).
    """
    if not HELIUS_KEY or not pair_address:
        return None
    if DEBUG:
        print(f"  DEBUG creator_from_pool: ищем деплоера через пул {pair_address[:20]}…")
    try:
        # Пагинируем до самой старой транзакции пула
        batch = _helius_rpc(
            "getSignaturesForAddress",
            [pair_address, {"limit": 1000, "commitment": "finalized"}]
        )
        if DEBUG:
            _cnt = len(batch) if isinstance(batch, list) else repr(batch)
            print(f"  DEBUG creator_from_pool: getSignaturesForAddress(pool) → {_cnt} сигнатур")
        if not isinstance(batch, list) or not batch:
            return None

        for _page in range(5):
            if len(batch) < 1000:
                break
            oldest_sig = batch[-1]["signature"]
            next_batch = _helius_rpc(
                "getSignaturesForAddress",
                [pair_address, {"limit": 1000, "before": oldest_sig, "commitment": "finalized"}]
            )
            if not isinstance(next_batch, list) or not next_batch:
                break
            batch = next_batch

        target_sig = batch[-1]["signature"]
        if DEBUG:
            print(f"  DEBUG creator_from_pool: oldest pool sig = {target_sig[:20]}…")

        # Парсим транзакцию
        tx = _helius_rpc(
            "getTransaction",
            [target_sig, {"encoding": "jsonParsed",
                          "maxSupportedTransactionVersion": 0,
                          "commitment": "finalized"}]
        )
        if tx:
            keys = (tx.get("transaction", {}).get("message", {}).get("accountKeys") or [])
            for k in keys:
                addr = k.get("pubkey") or ""
                if k.get("signer") and addr and addr not in SYSTEM_PROGRAMS and 30 <= len(addr) <= 50:
                    if DEBUG:
                        print(f"  DEBUG creator_from_pool (rpc signer): {addr[:20]}…")
                    return addr

        # Fallback: Enhanced API
        url = f"https://api.helius.xyz/v0/transactions?api-key={HELIUS_KEY}"
        r = requests.post(url, json={"transactions": [target_sig]}, timeout=15)
        if r.status_code == 200:
            txs = r.json()
            if isinstance(txs, list) and txs:
                fp = txs[0].get("feePayer") or ""
                if fp and fp not in SYSTEM_PROGRAMS and 30 <= len(fp) <= 50:
                    if DEBUG:
                        print(f"  DEBUG creator_from_pool (enhanced feePayer): {fp[:20]}…")
                    return fp
    except Exception as e:
        if DEBUG:
            print(f"  DEBUG creator_from_pool: {e}")
    return None


def helius_enhanced_creator(ca):
    """
    Helius Enhanced Transactions API:
      GET /v0/addresses/{ca}/transactions?api-key=...&limit=100

    Возвращает список транзакций с полем feePayer.
    Пагинируем до конца (before=lastSig) — самая старая = деплой.
    Это ДРУГОЙ API от getSignaturesForAddress — возвращает данные даже
    когда стандартный RPC возвращает пустой список (Meteora токены).
    """
    if not HELIUS_KEY:
        return None
    base = f"https://api.helius.xyz/v0/addresses/{ca}/transactions"
    last_sig = None
    for page in range(10):
        params = {"api-key": HELIUS_KEY, "limit": 100}
        if last_sig:
            params["before"] = last_sig
        try:
            r = requests.get(base, params=params, timeout=20)
            if r.status_code == 429:
                if DEBUG:
                    print(f"  DEBUG helius_enhanced_creator: 429 rate limit, ждём 3s…")
                time.sleep(3)
                r = requests.get(base, params=params, timeout=20)
            if r.status_code != 200:
                if DEBUG:
                    print(f"  DEBUG helius_enhanced_creator: HTTP {r.status_code} page={page}")
                break
            txs = r.json()
            if not isinstance(txs, list) or not txs:
                if DEBUG:
                    print(f"  DEBUG helius_enhanced_creator: page={page} пустой → конец")
                break
            if DEBUG:
                print(f"  DEBUG helius_enhanced_creator: page={page}, {len(txs)} txn")
            if len(txs) < 100:
                # Последняя страница — самая старая tx
                oldest = txs[-1]
                fp = oldest.get("feePayer") or ""
                if fp and fp not in SYSTEM_PROGRAMS and 30 <= len(fp) <= 50:
                    if DEBUG:
                        print(f"  DEBUG helius_enhanced_creator: feePayer={fp[:20]}…")
                    return fp
                # Если feePayer пустой — ищем первый аккаунт из accountData
                for acct in (oldest.get("accountData") or []):
                    addr = acct.get("account") or ""
                    if addr and addr not in SYSTEM_PROGRAMS and 30 <= len(addr) <= 50:
                        if DEBUG:
                            print(f"  DEBUG helius_enhanced_creator: accountData[0]={addr[:20]}…")
                        return addr
                break
            # Ещё есть страницы — пагинируем
            last_sig = txs[-1].get("signature") or ""
            if not last_sig:
                break
        except Exception as e:
            if DEBUG:
                print(f"  DEBUG helius_enhanced_creator: {e}")
            break
    return None


def solscan_direct_creator(ca):
    """
    Прямой HTTP к Solscan v2 API (без библиотеки free_solscan_api).
    Используем sol-aut заголовок — реверс-инженерен из браузера Solscan.
    Endpoint: GET https://api-v2.solscan.io/v2/token/meta?address={ca}
    """
    import json as _j
    # Solscan требует sol-aut — без него 403
    sol_aut = _generate_sol_aut()
    _sess = requests.Session()
    try:
        r = _sess.get(
            "https://api-v2.solscan.io/v2/token/meta",
            params={"address": ca},
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept":          "application/json, text/plain, */*",
                "Accept-Encoding": "identity",
                "Origin":          "https://solscan.io",
                "Referer":         f"https://solscan.io/token/{ca}",
                "sol-aut":         sol_aut,
            },
            timeout=15,
        )
        if DEBUG:
            print(f"  DEBUG solscan_direct_creator: HTTP {r.status_code}, len={len(r.content)}")
        if r.status_code not in (200, 201):
            if DEBUG:
                print(f"  DEBUG solscan_direct_creator: {r.status_code} — {r.text[:120]!r}")
            return None
        raw = _vas_decompress(r.content).strip()
        if not raw:
            return None
        try:
            d, _ = _j.JSONDecoder().raw_decode(raw)
        except _j.JSONDecodeError:
            if DEBUG:
                print(f"  DEBUG solscan_direct_creator: JSON parse error | raw[:80]={raw[:80]!r}")
            return None
        if DEBUG:
            print(f"  DEBUG solscan_direct_creator: {_j.dumps(d, ensure_ascii=False)[:800]}")
        data = d.get("data") or d  # иногда обёрнуто в {data: {...}}
        for field in ("creator", "mintAuthority", "freezeAuthority", "updateAuthority",
                      "metadata_creator", "deployer", "owner"):
            addr = (data.get(field) or "") if isinstance(data, dict) else ""
            if addr and addr not in SYSTEM_PROGRAMS and 30 <= len(addr) <= 50:
                if DEBUG:
                    print(f"  DEBUG solscan_direct_creator ({field}): {addr[:20]}…")
                return addr
    except Exception as e:
        if DEBUG:
            print(f"  DEBUG solscan_direct_creator: {e}")
    return None


def _helius_rpc(method, params):
    """
    Стандартный Solana JSON-RPC через Helius (не Enhanced API).
    getSignaturesForAddress возвращает 1000 подписей за запрос — в 10x быстрее.
    """
    url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}"
    try:
        r = requests.post(
            url,
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            timeout=20,
        )
        return r.json().get("result")
    except Exception as e:
        if DEBUG:
            print(f"  DEBUG RPC {method}: {e}")
    return None


def helius_get_top_holders(ca):
    """
    Получаем топ-20 держателей токена через Helius RPC.
    Использует getTokenLargestAccounts → getMultipleAccounts (2 запроса всего).
    Fallback когда GMGN не возвращает holders (Meteora, малые токены).
    Возвращает список dict с полями: wallet, uiAmount, hold_pct.
    """
    if not HELIUS_KEY:
        return []
    try:
        # Шаг 1: топ-20 токен-аккаунтов по балансу
        result = _helius_rpc("getTokenLargestAccounts", [ca, {"commitment": "finalized"}])
        if not isinstance(result, dict):
            return []
        accounts = result.get("value") or []
        if not accounts:
            return []

        # Подсчёт общего supply для расчёта % холдинга
        total_ui = sum(float(a.get("uiAmount") or 0) for a in accounts) or 1

        # Шаг 2: узнаём владельца каждого токен-аккаунта батчем
        acct_addrs = [a.get("address") for a in accounts if a.get("address")]
        multi = _helius_rpc("getMultipleAccounts", [acct_addrs, {"encoding": "jsonParsed"}])
        if not isinstance(multi, dict):
            return []
        values = multi.get("value") or []

        holders = []
        for i, acct_info in enumerate(values):
            if not acct_info or i >= len(accounts):
                continue
            info = (
                (acct_info.get("data") or {})
                .get("parsed", {})
                .get("info", {})
            )
            owner   = info.get("owner") or ""
            ui_amt  = float(accounts[i].get("uiAmount") or 0)
            if owner and owner not in SYSTEM_PROGRAMS and len(owner) >= 30:
                frac = ui_amt / total_ui  # доля (0..1), как GMGN amount_percentage
                holders.append({
                    "wallet":             owner,
                    "address":            owner,
                    "account_address":    owner,
                    "uiAmount":           ui_amt,
                    "hold_pct":           frac * 100,
                    "amount_percentage":  frac,   # совместимость с GMGN-форматом
                    "_source":            "helius_rpc",
                })

        if DEBUG:
            print(f"  DEBUG helius_get_top_holders: {len(holders)} holders")
        return holders

    except Exception as e:
        if DEBUG:
            print(f"  DEBUG helius_get_top_holders: {e}")
        return []


def helius_check_dev_holding(deployer: str, ca: str) -> dict:
    """
    Проверяет держит ли деплоер ещё токены через Helius RPC.
    Возвращает dict:
      {
        "holds": True/False/None,  # True=держит, False=продал, None=нет данных
        "pct":   float,            # % от supply (0.0 если продал)
        "raw":   int,              # raw баланс
      }
    Использует getTokenAccountsByOwner — 1 быстрый RPC запрос.
    """
    if not deployer or not ca or not HELIUS_KEY:
        return {"holds": None, "pct": 0.0, "raw": 0}
    try:
        result = _helius_rpc(
            "getTokenAccountsByOwner",
            [
                deployer,
                {"mint": ca},
                {"encoding": "jsonParsed", "commitment": "finalized"},
            ],
        )
        if not isinstance(result, dict):
            return {"holds": None, "pct": 0.0, "raw": 0}
        accounts = result.get("value") or []
        total_raw = 0
        decimals   = 6
        for acc in accounts:
            parsed = (acc.get("account", {})
                        .get("data", {})
                        .get("parsed", {})
                        .get("info", {}))
            raw = int(parsed.get("tokenAmount", {}).get("amount") or 0)
            decimals = int(parsed.get("tokenAmount", {}).get("decimals") or 6)
            total_raw += raw

        holds = total_raw > 0
        # Чтобы вычислить %, нужен supply — пропускаем (просто показываем держит/не держит)
        return {"holds": holds, "pct": 0.0, "raw": total_raw}
    except Exception as e:
        if DEBUG:
            print(f"  DEBUG check_dev_holding: {e}")
        return {"holds": None, "pct": 0.0, "raw": 0}


def _helius_sigs_in_window(addr, launch_f, win_end):
    """
    Быстро находим подписи транзакций в окне [launch_f, win_end].
    RPC: 1000 подписей/запрос → для 10 000 транзакций нужно 10 запросов.
    """
    sigs = []
    before = None

    for page in range(30):   # макс 30 000 подписей
        params = [addr, {"limit": 1000, "commitment": "finalized"}]
        if before:
            params[1]["before"] = before

        result = _helius_rpc("getSignaturesForAddress", params)
        if not isinstance(result, list) or not result:
            break

        stop = False
        for info in result:
            bt = info.get("blockTime") or 0
            if bt > win_end:
                continue           # новее окна
            if bt < launch_f:
                stop = True        # ушли за запуск
                break
            if not info.get("err"):  # только успешные транзакции
                sigs.append(info["signature"])

        if DEBUG:
            oldest_bt = result[-1].get("blockTime", 0) if result else 0
            print(f"  DEBUG sigs page {page}: {len(result)} signatures, "
                  f"oldest={oldest_bt}, in_window={len(sigs)}, stop={stop}")

        if stop or len(result) < 1000:
            break
        before = result[-1]["signature"]

    return sigs


def _helius_parse_txs(signatures, ca):
    """
    Парсим пачку транзакций через Helius Enhanced POST /v0/transactions/.
    Возвращает список (wallet, ts, token_received, sol_spent, tx_type, sig, distributor).
    """
    if not signatures:
        return []

    WSOL = "So11111111111111111111111111111111111111112"
    results = []

    # Helius batch: макс 100 за раз
    for i in range(0, len(signatures), 100):
        batch = signatures[i:i + 100]
        try:
            r = requests.post(
                f"https://api.helius.xyz/v0/transactions/?api-key={HELIUS_KEY}",
                json={"transactions": batch},
                timeout=30,
            )
            if r.status_code != 200:
                if DEBUG:
                    print(f"  DEBUG parse_txs batch {i//100}: HTTP {r.status_code}")
                continue
            txs = r.json()
            if not isinstance(txs, list):
                continue
        except Exception as e:
            if DEBUG:
                print(f"  DEBUG parse_txs: {e}")
            continue

        for tx in txs:
            tx_type   = tx.get("type") or "UNKNOWN"
            sig       = tx.get("signature") or ""
            ts        = int(tx.get("timestamp") or 0)
            fee_payer = tx.get("feePayer") or ""

            token_transfers  = tx.get("tokenTransfers")  or []
            native_transfers = tx.get("nativeTransfers") or []

            buyer          = ""
            token_received = 0.0
            sol_spent      = 0.0
            distributor    = ""

            if tx_type == "SWAP":
                buyer = fee_payer
                for tr in token_transfers:
                    if tr.get("mint") == ca and tr.get("toUserAccount") == buyer:
                        try: token_received += float(tr.get("tokenAmount") or 0)
                        except: pass
                    if tr.get("mint") == WSOL and tr.get("fromUserAccount") == buyer:
                        try: sol_spent += float(tr.get("tokenAmount") or 0)
                        except: pass
                if sol_spent == 0:
                    for nt in native_transfers:
                        if nt.get("fromUserAccount") == buyer:
                            try: sol_spent += float(nt.get("amount") or 0) / 1e9
                            except: pass
            else:
                # TRANSFER: ищем кто ПОЛУЧИЛ наш токен
                for tr in token_transfers:
                    if tr.get("mint") != ca:
                        continue
                    to_w   = tr.get("toUserAccount")   or ""
                    from_w = tr.get("fromUserAccount") or ""
                    if to_w and to_w not in SYSTEM_PROGRAMS:
                        try: token_received = float(tr.get("tokenAmount") or 0)
                        except: token_received = 0
                        buyer       = to_w
                        distributor = from_w if from_w not in SYSTEM_PROGRAMS else ""
                        for nt in native_transfers:
                            if nt.get("toUserAccount") == buyer:
                                try: sol_spent += float(nt.get("amount") or 0) / 1e9
                                except: pass
                        break

            if not buyer or buyer in SYSTEM_PROGRAMS:
                continue
            if token_received == 0 and sol_spent == 0:
                continue

            results.append({
                "wallet":         buyer,
                "ts":             ts,
                "token_received": token_received,
                "sol_spent":      sol_spent,
                "tx_type":        tx_type,
                "signature":      sig,
                "distributor":    distributor,
            })

    return results


def helius_early_buyers(ca, launch_ts, pair_addr=None, window_min=30):
    """
    Ранние покупатели через Helius.
    Требует HELIUS_KEY в .env (бесплатно: helius.dev → Sign up → API Keys).

    Алгоритм (2-шаговый, быстрый):
      Шаг 1 — RPC getSignaturesForAddress (1000/запрос) на TOKEN CA:
        Пагинируем назад во времени, собираем подписи транзакций
        которые попали в окно [launch_ts, launch_ts + window_min*60].
        Для 10 000 транзакций нужно ~10 запросов (vs 100 в старом подходе).

      Шаг 2 — Enhanced POST /v0/transactions/ батчами по 100:
        Парсим только нужные транзакции, не тратя время на остальные.
    """
    if not HELIUS_KEY:
        return []
    if not launch_ts:
        return []

    launch_f = float(launch_ts)
    win_end  = launch_f + window_min * 60

    if DEBUG:
        print(f"\n── DEBUG helius: token CA={ca[:20]}… window={window_min}min ──")

    # Шаг 1: быстро найти подписи в окне через RPC
    sigs = _helius_sigs_in_window(ca, launch_f, win_end)
    if not sigs:
        if DEBUG:
            print("  helius: 0 сигнатур в окне — токен слишком старый или нет активности")
        return []

    if DEBUG:
        print(f"  helius: найдено {len(sigs)} сигнатур в окне, парсим…")

    # Шаг 2: парсим только нужные транзакции
    raw = _helius_parse_txs(sigs[:500], ca)  # берём макс 500

    # Дедупликация по wallet (лучшая запись = наибольшее кол-во токена)
    buyers = {}
    for entry in raw:
        w   = entry["wallet"]
        ts  = entry["ts"]
        delta     = ts - launch_f
        min_after = f"+{int(delta // 60)}m{int(delta % 60):02d}s"
        entry["min_after"] = min_after
        if w not in buyers or entry["token_received"] > buyers[w].get("token_received", 0):
            buyers[w] = entry

    result = sorted(buyers.values(), key=lambda x: x["ts"])
    if DEBUG:
        print(f"  helius итого: {len(result)} уникальных кошельков\n──────────────────")
    return result


def helius_dist_recipients(ca, distributor, launch_ts, window_min=180):
    """
    Все кошельки которым дистрибьютор отправлял токен CA.
    Используем 2-шаговый подход: RPC сигнатуры → Enhanced парсинг.
    window_min=180: смотрим 3 часа (дистрибуция могла растянуться).
    """
    if not HELIUS_KEY or not distributor:
        return []

    launch_f = float(launch_ts)
    win_end  = launch_f + window_min * 60

    # Шаг 1: подписи транзакций дистрибьютора в окне
    sigs = _helius_sigs_in_window(distributor, launch_f, win_end)
    if not sigs:
        if DEBUG:
            print(f"  DEBUG dist_recipients: нет сигнатур для {distributor[:20]}")
        return []

    if DEBUG:
        print(f"  DEBUG dist_recipients: {len(sigs)} сигнатур у дистрибьютора")

    # Шаг 2: парсим и ищем переводы нашего токена ОТ дистрибьютора
    raw = _helius_parse_txs(sigs[:500], ca)

    recipients = {}
    for entry in raw:
        # Нас интересуют только TRANSFER от дистрибьютора
        if entry.get("distributor") != distributor:
            continue
        w = entry["wallet"]
        if w == distributor:
            continue
        tok = entry.get("token_received", 0)
        if w not in recipients or tok > recipients[w].get("token_received", 0):
            recipients[w] = entry

    if DEBUG:
        print(f"  DEBUG dist_recipients: {len(recipients)} уникальных получателей")

    return sorted(recipients.values(), key=lambda x: x["ts"])


def helius_enrich(early_buyers, console=None):
    """
    Обогащаем ранних покупателей данными GMGN (WR, PnL, кол-во сделок).
    Запускаем параллельно, не больше топ-25.
    """
    top = early_buyers[:25]
    if not top:
        return early_buyers

    if console and RICH:
        console.print(f"  [dim]Проверяю {len(top)} кошельков в GMGN…[/dim]")

    def _enrich_one(entry):
        stats = g_stats(entry["wallet"]) or {}
        wr    = (stats.get("pnl_stat") or {}).get("winrate") or stats.get("winrate") or stats.get("win_rate")
        pnl   = _scalar(stats.get("realized_profit") or
                        stats.get("total_profit_usd") or 0)
        buys  = int(stats.get("buy_count")  or stats.get("buy")  or 0)
        sells = int(stats.get("sell_count") or stats.get("sell") or 0)
        try:
            wr_f = float(wr) * (100 if float(wr) <= 1 else 1) if wr else None
        except:
            wr_f = None
        return {
            **entry,
            "winrate":  wr_f,
            "pnl":      pnl,
            "total_tx": buys + sells,
        }

    enriched = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(_enrich_one, e): e for e in top}
        for fut in as_completed(futs):
            try:
                enriched.append(fut.result())
            except:
                enriched.append(futs[fut])

    return sorted(enriched, key=lambda x: x["ts"])


def _helius_parse_sol_received(signatures, target_wallet):
    """
    Парсим транзакции и ищем входящие SOL переводы на target_wallet.
    Возвращает список {funder, amount_sol, ts, sig}.
    Используется для построения цепочки финансирования деплоера.
    """
    if not signatures:
        return []

    results = []
    for i in range(0, len(signatures), 100):
        batch = signatures[i:i + 100]
        try:
            r = requests.post(
                f"https://api.helius.xyz/v0/transactions/?api-key={HELIUS_KEY}",
                json={"transactions": batch},
                timeout=30,
            )
            if r.status_code != 200:
                if DEBUG:
                    print(f"  DEBUG parse_sol_received batch {i//100}: HTTP {r.status_code}")
                continue
            txs = r.json()
            if not isinstance(txs, list):
                continue
        except Exception as e:
            if DEBUG:
                print(f"  DEBUG parse_sol_received: {e}")
            continue

        for tx in txs:
            ts  = int(tx.get("timestamp") or 0)
            sig = tx.get("signature") or ""

            # ── Метод 1: nativeTransfers (стандартный SOL-перевод) ───────────
            found = False
            for nt in (tx.get("nativeTransfers") or []):
                from_w = nt.get("fromUserAccount") or ""
                to_w   = nt.get("toUserAccount")   or ""
                try:
                    amount = float(nt.get("amount") or 0) / 1e9
                except Exception:
                    amount = 0
                if (to_w == target_wallet
                        and from_w
                        and from_w not in SYSTEM_PROGRAMS
                        and amount >= 0.01):      # снижен порог: было 0.05
                    results.append({
                        "funder":     from_w,
                        "amount_sol": amount,
                        "ts":         ts,
                        "sig":        sig,
                    })
                    found = True

            # ── Метод 2: accountData balance changes ─────────────────────────
            # Helius заполняет nativeTransfers НЕ всегда (createAccount,
            # программные переводы, Jito bundles). Но accountData.nativeBalanceChange
            # есть ВСЕГДА — это raw изменение баланса каждого аккаунта.
            if not found:
                acct_data = tx.get("accountData") or []
                # Ищем, вырос ли баланс target_wallet
                target_change = 0.0
                for ad in acct_data:
                    if ad.get("account") == target_wallet:
                        try:
                            target_change = float(ad.get("nativeBalanceChange") or 0) / 1e9
                        except Exception:
                            pass
                        break

                if target_change >= 0.01:   # target получил SOL
                    # Ищем отправителя: наибольший отрицательный баланс
                    # среди не-системных аккаунтов (исключаем сам target)
                    best_sender, best_neg = "", 0.0
                    for ad in acct_data:
                        acc = ad.get("account") or ""
                        if acc == target_wallet or acc in SYSTEM_PROGRAMS:
                            continue
                        try:
                            chg = float(ad.get("nativeBalanceChange") or 0) / 1e9
                        except Exception:
                            chg = 0.0
                        if chg < -0.009 and abs(chg) > best_neg:
                            best_sender = acc
                            best_neg    = abs(chg)
                    if best_sender:
                        results.append({
                            "funder":     best_sender,
                            "amount_sol": target_change,
                            "ts":         ts,
                            "sig":        sig,
                        })
                        if DEBUG:
                            print(f"  DEBUG accountData hit: {best_sender[:20]}… "
                                  f"→ {target_wallet[:12]} +{target_change:.3f} SOL")
    return results


def helius_funding_chain(deployer, launch_ts, window_hours=48):
    """
    Кто пополнял кошелёк деплоера SOLом в window_hours часов ДО запуска токена?
    Это позволяет найти «мастер-кошелёк», который финансирует деплоеров.

    Алгоритм:
      1. RPC getSignaturesForAddress(deployer, window=[launch-48h, launch])
      2. Enhanced parse → ищем nativeTransfers WHERE toUserAccount == deployer
      3. Суммируем SOL по источнику, сортируем по сумме
    """
    if not HELIUS_KEY:
        return []

    launch_f  = float(launch_ts)
    win_start = launch_f - window_hours * 3600

    if DEBUG:
        print(f"\n── DEBUG funding_chain: {deployer[:20]}… window={window_hours}h ──")

    sigs = _helius_sigs_in_window(deployer, win_start, launch_f)
    if not sigs:
        if DEBUG:
            print("  funding_chain: нет транзакций в окне")
        return []

    if DEBUG:
        print(f"  funding_chain: {len(sigs)} сигнатур, парсю…")

    raw = _helius_parse_sol_received(sigs[:200], deployer)

    # Дедупликация: суммируем SOL от каждого источника
    by_funder = defaultdict(lambda: {"funder": "", "amount_sol": 0.0, "ts": 0, "sig": ""})
    for entry in raw:
        f = entry["funder"]
        by_funder[f]["funder"]      = f
        by_funder[f]["amount_sol"] += entry["amount_sol"]
        if entry["ts"] > by_funder[f]["ts"]:
            by_funder[f]["ts"]  = entry["ts"]
            by_funder[f]["sig"] = entry["sig"]

    # ── Solscan transfers — дополнительный источник ───────────────────────
    # Solscan может видеть SOL-переводы которые Helius Enhanced не распарсил
    if SOLSCAN_OK:
        try:
            sol_inc = solscan_wallet_transfers(deployer, page=1, page_size=200, flow="in")
            for tx in (sol_inc or []):
                sender = tx.get("from_address") or tx.get("from") or ""
                if not sender or sender in SYSTEM_PROGRAMS or len(sender) < 30:
                    continue
                raw_ts = int(tx.get("block_time") or tx.get("blockTime") or 0)
                # Фильтрация по времени: только транзакции в нужном окне
                if raw_ts and (raw_ts < win_start or raw_ts > launch_f):
                    continue
                raw_amt = tx.get("amount") or 0
                try:
                    amt = float(raw_amt)
                    if amt > 1_000_000:
                        amt /= 1e9
                    elif amt > 1000 and amt / 1e9 >= 0.01:
                        amt /= 1e9
                except Exception:
                    continue
                if amt < 0.005:
                    continue
                sig = tx.get("trans_id") or tx.get("signature") or ""
                by_funder[sender]["funder"]       = sender
                by_funder[sender]["amount_sol"]  += amt
                if raw_ts > by_funder[sender]["ts"]:
                    by_funder[sender]["ts"]  = raw_ts
                    by_funder[sender]["sig"] = sig
            if DEBUG:
                print(f"  funding_chain: Solscan добавил данные, итого {len(by_funder)} источников")
        except Exception as _se:
            if DEBUG:
                print(f"  funding_chain: Solscan ошибка: {_se}")

    result = sorted(by_funder.values(), key=lambda x: -x["amount_sol"])
    if DEBUG:
        print(f"  funding_chain итого: {len(result)} уникальных источников\n──────────────────")
    return result


def helius_all_incoming_sol(wallet, max_sigs=1000):
    """
    Полный скан: все входящие SOL-переводы на кошелёк без ограничения по времени.
    Используется как фолбэк когда время-ограниченный поиск не даёт результатов.
    Возвращает список {funder, amount_sol, ts, sig} отсортированный по сумме.

    Стратегия:
      1. Helius RPC → getSignaturesForAddress (пагинация до max_sigs)
      2. Helius Enhanced /v0/transactions → парсим nativeTransfers + accountData
      3. Если Helius пуст — public RPC fallback (jsonParsed getTransaction)
    """
    by_funder = defaultdict(lambda: {"funder": "", "amount_sol": 0.0, "ts": 0, "sig": ""})

    def _merge(entries):
        for entry in entries:
            f = entry["funder"]
            by_funder[f]["funder"]      = f
            by_funder[f]["amount_sol"] += entry["amount_sol"]
            if entry["ts"] > by_funder[f]["ts"]:
                by_funder[f]["ts"]  = entry["ts"]
                by_funder[f]["sig"] = entry["sig"]

    # ── Шаг 1: собираем подписи через Helius RPC (пагинация) ─────────────
    all_sigs = []
    if HELIUS_KEY:
        before = None
        for _page in range(max_sigs // 1000 + 1):
            params = [wallet, {"limit": 1000, "commitment": "finalized"}]
            if before:
                params[1]["before"] = before
            result = _helius_rpc("getSignaturesForAddress", params)
            if not isinstance(result, list) or not result:
                break
            page_sigs = [r["signature"] for r in result if not r.get("err")]
            all_sigs.extend(page_sigs)
            if len(result) < 1000 or len(all_sigs) >= max_sigs:
                break
            before = result[-1]["signature"]

    # ── Шаг 2: если Helius RPC пуст — пробуем public RPC ─────────────────
    if not all_sigs:
        for rpc_url in ("https://api.mainnet-beta.solana.com",
                        "https://rpc.ankr.com/solana"):
            try:
                resp = requests.post(
                    rpc_url,
                    json={"jsonrpc": "2.0", "id": 1,
                          "method": "getSignaturesForAddress",
                          "params": [wallet, {"limit": 1000, "commitment": "finalized"}]},
                    timeout=20,
                )
                res = resp.json().get("result")
                if isinstance(res, list) and res:
                    all_sigs = [r["signature"] for r in res if not r.get("err")]
                    if DEBUG:
                        print(f"  DEBUG all_incoming_sol: public RPC {rpc_url[:30]} "
                              f"→ {len(all_sigs)} сигнатур")
                    break
            except Exception:
                pass

    if not all_sigs:
        return []

    # ── Шаг 3: парсим через Helius Enhanced (основной метод) ─────────────
    raw = _helius_parse_sol_received(all_sigs[:500], wallet)
    _merge(raw)

    # ── Шаг 4: если Enhanced пуст — парсим через public RPC jsonParsed ───
    if not by_funder:
        if DEBUG:
            print(f"  DEBUG all_incoming_sol: Enhanced пуст → public RPC jsonParsed")
        for rpc_url in ("https://api.mainnet-beta.solana.com",
                        "https://rpc.ankr.com/solana"):
            found_any = False
            for sig in all_sigs[:100]:   # ограничиваем — public RPC медленный
                try:
                    time.sleep(0.25)
                    resp = requests.post(
                        rpc_url,
                        json={"jsonrpc": "2.0", "id": 1,
                              "method": "getTransaction",
                              "params": [sig, {"encoding": "jsonParsed",
                                               "maxSupportedTransactionVersion": 0,
                                               "commitment": "finalized"}]},
                        timeout=15,
                    )
                    tx = resp.json().get("result")
                    if not tx:
                        continue
                    ts = tx.get("blockTime") or 0
                    meta = tx.get("meta") or {}
                    pre  = meta.get("preBalances")  or []
                    post = meta.get("postBalances") or []
                    keys = (tx.get("transaction", {})
                              .get("message", {})
                              .get("accountKeys") or [])
                    for i, k in enumerate(keys):
                        acc = (k.get("pubkey") if isinstance(k, dict) else k) or ""
                        if acc != wallet or i >= len(pre) or i >= len(post):
                            continue
                        delta = (post[i] - pre[i]) / 1e9
                        if delta >= 0.01:
                            # найти отправителя: наибольшее отрицательное изменение
                            sender, max_neg = "", 0.0
                            for j, k2 in enumerate(keys):
                                a2 = (k2.get("pubkey") if isinstance(k2, dict) else k2) or ""
                                if a2 == wallet or a2 in SYSTEM_PROGRAMS or j >= len(pre):
                                    continue
                                neg = (pre[j] - post[j]) / 1e9 if j < len(post) else 0
                                if neg > max_neg:
                                    sender, max_neg = a2, neg
                            if sender:
                                by_funder[sender]["funder"]      = sender
                                by_funder[sender]["amount_sol"] += delta
                                if ts > by_funder[sender]["ts"]:
                                    by_funder[sender]["ts"]  = ts
                                    by_funder[sender]["sig"] = sig
                                found_any = True
                        break
                except Exception:
                    pass
            if found_any:
                break

    # ── Шаг 5: Solscan transfers — дополнительный источник (ВСЕГДА) ─────
    # Запускаем даже если Helius что-то нашёл — Solscan может видеть
    # переводы которые Helius Enhanced пропустил (разные форматы tx).
    if SOLSCAN_OK:
        try:
            sol_inc = solscan_wallet_transfers(wallet, page=1, page_size=200, flow="in")
            _sol_added = 0
            for tx in (sol_inc or []):
                sender = tx.get("from_address") or tx.get("from") or ""
                if not sender or sender in SYSTEM_PROGRAMS or len(sender) < 30:
                    continue
                # Solscan: amount в lamports для native SOL переводов
                raw_amt = tx.get("amount") or 0
                try:
                    amt = float(raw_amt)
                    # Если очень большое → это lamports, конвертируем
                    if amt > 1_000_000:
                        amt /= 1e9
                    elif amt > 1000:
                        # Неоднозначно: может быть 1000 lamports (=0.000001 SOL) или 1000 SOL
                        # Для кошельков финансирования обычно от 0.1 до 100 SOL
                        # Пробуем как lamports если < 0.01 SOL иначе — уже SOL
                        if amt / 1e9 >= 0.01:
                            amt /= 1e9
                        # else: amt уже в SOL (напр. Solscan вернул 1500 lamports = 0.0000015 SOL → < 0.01 → skip)
                except Exception:
                    continue
                if amt < 0.005:
                    continue
                ts  = int(tx.get("block_time") or tx.get("blockTime") or 0)
                sig = tx.get("trans_id") or tx.get("signature") or tx.get("tx_hash") or ""
                by_funder[sender]["funder"]       = sender
                by_funder[sender]["amount_sol"]  += amt
                if ts > by_funder[sender]["ts"]:
                    by_funder[sender]["ts"]  = ts
                    by_funder[sender]["sig"] = sig
                _sol_added += 1
            if DEBUG and _sol_added:
                print(f"  DEBUG all_incoming_sol: Solscan добавил {_sol_added} переводов")
        except Exception as _se:
            if DEBUG:
                print(f"  DEBUG all_incoming_sol: Solscan ошибка: {_se}")

    return sorted(by_funder.values(), key=lambda x: -x["amount_sol"])


def _helius_wallet_tx_count(wallet, limit=30):
    """
    Быстро смотрим сколько транзакций у кошелька через RPC.
    limit=30: если вернул 30 — транзакций >= 30 (возможно много больше).
    Используем для детектирования прослоек: мало tx = одноразовый кошелёк.
    """
    params = [wallet, {"limit": limit, "commitment": "finalized"}]
    result = _helius_rpc("getSignaturesForAddress", params)
    if isinstance(result, list):
        return len(result)
    return 0


def helius_trace_funding_deep(deployer, launch_ts, max_depth=4):
    """
    Рекурсивная трассировка: деплоер → прослойки → мастер-кошелёк.

    Ключевая идея по ширине окна поиска:
      depth=0 (деплоер):       72ч  — финансируется непосредственно перед запуском
      depth=1+ (прослойки):   720ч (30 дней) — прослойки пополняются заранее,
                                                иногда за недели до запуска

    Алгоритм:
      1. Ищем входящие SOL в нужном окне
      2. Классифицируем каждого источника: прослойка vs мастер
      3. Прослойка = нет торговли + нет деплоев → рекурсируем с 30-дневным окном
      4. max_depth + visited set = защита от бесконечной рекурсии / циклов
    """
    if not HELIUS_KEY:
        return []

    visited   = set()
    all_nodes = []

    def _trace_level(target, target_ts, depth):
        if depth >= max_depth or target in visited:
            return
        visited.add(target)

        # depth=0 (деплоер): 168ч (7 дней) — может быть пополнен заблаговременно
        # depth>0 (прослойки): 720ч (30 дней)
        window_h = 168 if depth == 0 else 720

        if DEBUG:
            print(f"  DEBUG trace depth={depth}: {target[:20]}…  "
                  f"ts={target_ts}  window={window_h}h")

        # ── Источник 1: Helius (входящие SOL в временном окне) ───────────────
        funders = helius_funding_chain(target, target_ts, window_hours=window_h)

        # ── Источник 2: GMGN fund_from_address ───────────────────────────────
        # GMGN явно хранит "кто первым создал/пополнил этот кошелёк".
        # Это надёжнее парсинга транзакций и не зависит от временного окна.
        # Встречается в stats["common"]["fund_from_address"].
        try:
            tgt_stats  = g_stats(target) or {}
            tgt_common = tgt_stats.get("common") or {}
            gmgn_ff    = tgt_common.get("fund_from_address") or ""
            gmgn_ff_ts = int(tgt_common.get("fund_from_ts")  or 0)
            gmgn_ff_tx = tgt_common.get("fund_tx_hash")      or ""
            try:
                gmgn_ff_sol = float(tgt_common.get("fund_amount") or 0)
            except Exception:
                gmgn_ff_sol = 0.0
            if gmgn_ff and gmgn_ff not in SYSTEM_PROGRAMS:
                existing = {f["funder"] for f in funders}
                if gmgn_ff not in existing:
                    tag = " (GMGN: первое финансирование)"
                    print(f"  ↳ GMGN fund_from для {target}: "
                          f"{gmgn_ff} {gmgn_ff_sol:.3f} SOL{tag}")
                    funders.append({
                        "funder":     gmgn_ff,
                        "amount_sol": gmgn_ff_sol,
                        "ts":         gmgn_ff_ts,
                        "sig":        gmgn_ff_tx,
                        "_gmgn":      True,
                    })
        except Exception:
            pass

        # ── Фоллбэк: полная история Helius (если оба источника пусты) ─────────
        # Применяем для ЛЮБОГО depth — деплоер тоже мог быть пополнен давно
        if not funders:
            if DEBUG:
                print(f"  DEBUG trace depth={depth}: нет источников в окне {window_h}h")
            print(f"  ↳ Helius+GMGN пусты (окно {window_h}h) — "
                  f"сканирую ВСЮ историю {target[:20]}…")
            funders = helius_all_incoming_sol(target, max_sigs=1000)
            if funders:
                print(f"  ↳ Полная история: найдено {len(funders)} источников SOL")
            else:
                print(f"  ↳ Нет входящих SOL вообще — биржевой вывод или история недоступна")
                return

        # Параллельно классифицируем все источники этого уровня
        def _classify(finfo):
            w = finfo["funder"]

            # Известные кошельки / биржи — не прослойки, стоп
            if w in KNOWN_WALLETS:
                return {
                    **finfo,
                    "depth":      depth,
                    "funded_target": target,
                    "tx_count":   9999,
                    "is_proxy":   False,
                    "known":      KNOWN_WALLETS[w],
                    "wr": 0, "pnl": 0, "gmgn_txns": 0, "f_created": [],
                }

            # RPC: кол-во транзакций кошелька
            tx_cnt = _helius_wallet_tx_count(w, limit=30)

            # GMGN: торговая история и созданные токены
            stats   = g_stats(w)   or {}
            created = g_created(w) or []
            wr    = (stats.get("pnl_stat") or {}).get("winrate") or stats.get("winrate") or stats.get("win_rate")
            pnl   = _scalar(stats.get("realized_profit") or
                            stats.get("total_profit_usd") or 0)
            buys  = int(stats.get("buy_count")  or stats.get("buy")  or 0)
            sells = int(stats.get("sell_count") or stats.get("sell") or 0)
            gmgn_txns = buys + sells
            try:
                wr_f = float(wr) * (100 if float(wr) <= 1 else 1) if wr else 0
            except Exception:
                wr_f = 0

            # ПРОСЛОЙКА — классификация:
            #   no_activity = нет торговли И нет деплоев → пустой кошелёк
            #   rpc_unreliable = RPC вернул 0 но GMGN показывает активность
            #      (tx_cnt=0 когда RPC недоступен → не доверяем)
            no_activity = gmgn_txns < 3 and len(created) < 2
            rpc_unreliable = (tx_cnt == 0) and (gmgn_txns > 0 or len(created) > 0)
            if rpc_unreliable:
                # RPC явно врёт — есть GMGN активность → классифицируем только по GMGN
                is_proxy = no_activity
            else:
                # tx_cnt надёжен: < 15 = одноразовый; или нет активности при < 100 tx
                is_proxy = (tx_cnt < 15) or (no_activity and tx_cnt < 100)

            return {
                **finfo,
                "depth":         depth,
                "funded_target": target,
                "tx_count":      tx_cnt,
                "is_proxy":      is_proxy,
                "known":         "",
                "wr":            wr_f,
                "pnl":           pnl,
                "gmgn_txns":     gmgn_txns,
                "f_created":     created,
            }

        level_nodes = []
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = {ex.submit(_classify, f): f for f in funders[:5]}
            for fut in as_completed(futs):
                try:
                    level_nodes.append(fut.result())
                except Exception:
                    pass

        level_nodes.sort(key=lambda x: -x.get("amount_sol", 0))
        all_nodes.extend(level_nodes)

        # Рекурсия для ВСЕХ кошельков кроме известных CEX/бирж
        # is_proxy/is_master — это только метка для отображения, не стоп-сигнал
        # Останавливаемся только на: CEX, уже посещённых, max_depth
        for node in level_nodes:
            if not node.get("known") and node["funder"] not in visited:
                next_ts = node["ts"] if node["ts"] else target_ts
                _trace_level(node["funder"], next_ts, depth + 1)

    _trace_level(deployer, launch_ts, depth=0)
    return all_nodes


def build_watch_list(early_buyers):
    """
    Качественный Watch List из ранних покупателей.
    Критерии «настоящего независимого трейдера»:
      - tx_type == SWAP  (покупал через DEX, не бандл-получатель)
      - WR > 50%
      - PnL 30d > $3 000
      - total_tx >= 20  (опытный, есть история)
    Возвращает список, отсортированный по PnL desc.
    """
    watch = []
    for e in early_buyers:
        if e.get("tx_type") != "SWAP":
            continue
        wr   = e.get("winrate") or 0
        pnl  = e.get("pnl")    or 0
        txns = e.get("total_tx") or 0
        if wr > 50 and pnl > 3000 and txns >= 20:
            watch.append(e)
    return sorted(watch, key=lambda x: -x.get("pnl", 0))


def build_top_holder_watch_list(ca, console=None):
    """
    Watch List из ТЕКУЩИХ топ холдеров токена (не ограничен первыми 30 мин).

    Возвращает dict:
      "watch"        — список качественных трейдеров (WR>50% + PnL>$3K + ≥20tx)
      "dev_insiders" — холдеры из дерева monitor.db (связаны с дев-кошельком)
      "distributors" — dict {sender_addr: [list of recipient wallets]}
                       дистрибьюторы, разославшие токены 3+ холдерам
      "dist_profiles"— dict {sender_addr: {wr, pnl, txns, created, dev_info}}
                       профили дистрибьюторов
      "all_enriched" — все обогащённые кандидаты (топ-50)
    """
    empty = {"watch": [], "dev_insiders": [], "distributors": {},
             "big_dist": {}, "dist_profiles": {}, "all_enriched": [],
             "data_source": "none"}

    if console:
        console.print("  [dim]Запрашиваю топ холдеров GMGN…[/dim]")

    holders = gmgn_vas_holders(ca, limit=200)
    _data_source = "holders"

    if not holders:
        if console:
            console.print(
                "  [dim]Текущие холдеры недоступны → пробую top traders "
                "(трейдеры которые уже вышли)…[/dim]"
            )
        holders = gmgn_top_traders(ca, limit=100)
        _data_source = "traders"

    if not holders:
        if console:
            console.print("  [dim]Пробую OpenAPI /v1/market/token_top_holders…[/dim]")
        holders = g_holders(ca, n=100)
        _data_source = "top_holders_api"

    if not holders and HELIUS_KEY:
        if console:
            console.print("  [dim]GMGN пуст → пробую Helius getTokenLargestAccounts…[/dim]")
        holders = helius_get_top_holders(ca)
        _data_source = "helius_rpc"

    if not holders:
        if console:
            console.print("  [dim]GMGN не вернул данных ни по холдерам, ни по трейдерам.[/dim]")
        return empty

    # ── Шаг 1: загружаем дерево дева ─────────────────────────────────────
    monitor_tree = load_monitor_tree()

    # ── Шаг 2: разбираем холдеров, строим карту дистрибьюторов ──────────
    candidates    = []
    distributor_map = defaultdict(list)   # sender → [recipient, …]
    skipped_bundle = skipped_cex = 0

    for h in holders:
        addr      = h.get("account_address") or h.get("address") or ""
        if not addr:
            continue

        tags      = h.get("maker_token_tags") or []
        buy_count = int(h.get("buy_tx_count_cur") or 0)
        ti_info   = h.get("token_transfer_in") or {}
        sender    = ti_info.get("address") or ""
        is_ti     = bool(h.get("transfer_in"))
        pct       = float(h.get("amount_percentage") or 0) * 100

        # Бандлер → пропускаем из качественного анализа, но фиксируем
        if "bundler" in tags:
            skipped_bundle += 1
            if is_ti and sender:
                distributor_map[sender].append(addr)
            continue

        # CEX → пропускаем
        if addr in KNOWN_WALLETS:
            skipped_cex += 1
            continue

        # Фиксируем дистрибьютора для ВСЕХ transfer_in (даже если не в watch list)
        if is_ti and sender:
            distributor_map[sender].append(addr)

        # Связь с дев-кошельком
        dev_info = monitor_tree.get(addr)

        candidates.append({
            "wallet":        addr,
            "hold_pct":      pct,
            "buy_count":     buy_count,
            "transfer_in":   is_ti,
            "sender":        sender,
            "token_profit":  float(h.get("profit")    or 0),
            "usd_value":     float(h.get("usd_value") or 0),
            "tags":          tags,
            "wallet_tag":    h.get("wallet_tag_v2") or "",
            "is_new":        bool(h.get("is_new")),
            "start_holding": h.get("start_holding_at"),
            # dev tree
            "dev_depth":     dev_info["depth"]        if dev_info else None,
            "dev_path":      dev_info["path"]          if dev_info else [],
            "dev_master":    dev_info["master"]        if dev_info else "",
            "dev_label":     dev_info["master_label"]  if dev_info else "",
        })

    if console:
        console.print(
            f"  [dim]{len(holders)} холдеров | -бандл:{skipped_bundle}"
            f" -CEX:{skipped_cex}"
            f" → {len(candidates)} кандидатов → обогащаю stats…[/dim]"
        )

    if not candidates:
        return {**empty, "distributors": dict(distributor_map)}

    # ── Шаг 3: обогащение g_stats() параллельно для топ-50 ───────────────
    top50 = candidates[:50]

    def _enrich(item):
        stats = g_stats(item["wallet"]) or {}
        pnl_s = stats.get("pnl_stat") or {}
        buys  = int(stats.get("buy")  or stats.get("buy_count")  or 0)
        sells = int(stats.get("sell") or stats.get("sell_count") or 0)
        txns  = buys + sells
        wr_raw = pnl_s.get("winrate") or stats.get("winrate") or 0
        try:
            wr = float(wr_raw) * (100 if float(wr_raw) <= 1 else 1)
        except Exception:
            wr = 0.0
        pnl = _scalar(stats.get("realized_profit") or pnl_s.get("pnl") or 0)
        return {**item, "winrate": wr, "pnl": pnl, "total_tx": txns}

    enriched = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {ex.submit(_enrich, c2): c2 for c2 in top50}
        for fut in as_completed(futs):
            try:
                enriched.append(fut.result())
            except Exception:
                pass

    # ── Шаг 4: качественный фильтр ───────────────────────────────────────
    watch = sorted(
        [e for e in enriched
         if e.get("winrate", 0) > 50
         and e.get("pnl",     0) > 3000
         and e.get("total_tx",0) >= 20],
        key=lambda x: -x.get("pnl", 0),
    )

    # ── Шаг 5: дев-инсайдеры среди холдеров ──────────────────────────────
    dev_insiders = [e for e in enriched if e.get("dev_depth") is not None]

    # ── Шаг 6: значимые дистрибьюторы (≥3 получателей) → профилируем ────
    big_distributors = {s: r for s, r in distributor_map.items() if len(r) >= 3}

    def _profile_dist(sender):
        stats = g_stats(sender) or {}
        pnl_s = stats.get("pnl_stat") or {}
        buys  = int(stats.get("buy")  or stats.get("buy_count")  or 0)
        sells = int(stats.get("sell") or stats.get("sell_count") or 0)
        txns  = buys + sells
        wr_raw = pnl_s.get("winrate") or stats.get("winrate") or 0
        try:
            wr = float(wr_raw) * (100 if float(wr_raw) <= 1 else 1)
        except Exception:
            wr = 0.0
        pnl     = _scalar(stats.get("realized_profit") or pnl_s.get("pnl") or 0)
        created = g_created(sender) or []
        dev_i   = monitor_tree.get(sender)
        return {
            "wr": wr, "pnl": pnl, "txns": txns,
            "created": created,
            "dev_depth": dev_i["depth"]        if dev_i else None,
            "dev_path":  dev_i["path"]          if dev_i else [],
            "dev_label": dev_i["master_label"]  if dev_i else "",
        }

    dist_profiles = {}
    if big_distributors:
        with ThreadPoolExecutor(max_workers=6) as ex:
            futs2 = {ex.submit(_profile_dist, s): s for s in big_distributors}
            for fut in as_completed(futs2):
                s = futs2[fut]
                try:
                    dist_profiles[s] = fut.result()
                except Exception:
                    pass

    return {
        "watch":         watch,
        "dev_insiders":  dev_insiders,
        "distributors":  dict(distributor_map),
        "big_dist":      big_distributors,
        "dist_profiles": dist_profiles,
        "all_enriched":  enriched,
        "data_source":   _data_source,
    }


# ── Solscan helpers ───────────────────────────────────────────────────────────
def solscan_transfers(wallet, pages=5):
    """История SOL-переводов кошелька (старый helper, теперь через _solscan)."""
    if not SOLSCAN_OK:
        return []
    out = []
    for page in range(1, pages + 1):
        try:
            data = _solscan("/account/transfer", {
                "address": wallet, "page": page, "page_size": 100,
                "exclude_amount_zero": "true",
                "sort_by": "block_time", "sort_order": "desc",
            })
            rows = data if isinstance(data, list) else (data.get("data", []) if isinstance(data, dict) else [])
            out.extend(rows)
            if len(rows) < 100:
                break
        except Exception:
            break
    return out


def solscan_top(wallet, days=30):
    """Топ адреса с которыми кошелёк обменивался SOL."""
    if not SOLSCAN_OK:
        return []
    try:
        data = _solscan("/analytics/account/top-address-transfers",
                        {"address": wallet, "range": days})
        return data if isinstance(data, list) else []
    except Exception:
        return []


def solscan_early_buyers(ca, launch_ts=None, window_min=120, limit=200):
    """
    Ранние покупатели токена через Solscan — fallback когда Helius пуст.
    Берём первые limit трансферов токена CA (сортировка ASC = с самого начала).
    Фильтруем по окну [launch_ts, launch_ts + window_min*60] если известен.
    """
    if not SOLSCAN_OK:
        return []
    try:
        data = _solscan("/token/transfer", {
            "address": ca, "page": 1, "page_size": limit,
            "sort_by": "block_time", "sort_order": "asc",
        })
        rows = data if isinstance(data, list) else (data.get("data", []) if isinstance(data, dict) else [])
        if launch_ts and rows:
            launch_f = float(launch_ts)
            win_end  = launch_f + window_min * 60
            rows = [r for r in rows
                    if launch_f <= float(r.get("block_time") or r.get("blockTime") or 0) <= win_end]
        return rows
    except Exception as e:
        if DEBUG:
            print(f"  DEBUG solscan_early_buyers: {e}")
        return []

# ── Format helpers ────────────────────────────────────────────────────────────
def _scalar(v):
    """Число из поля, которое может быть dict/str/float."""
    if isinstance(v, dict):
        v = (v.get("usd") or v.get("value") or v.get("price")
             or next(iter(v.values()), 0))
    try:
        return float(v)
    except:
        return 0.0

def fusd(v):
    try:
        v = float(v)
    except:
        return "?"
    if v >= 1_000_000: return f"${v/1_000_000:.2f}M"
    if v >= 1_000:     return f"${v/1_000:.1f}K"
    return f"${v:.2f}"

def fage(ts):
    if not ts: return "?"
    try:
        h = (time.time() - float(ts)) / 3600
    except:
        return "?"
    if h < 1:  return f"{int(h*60)}m"
    if h < 48: return f"{h:.1f}h"
    return f"{h/24:.1f}d"

def fpct(v):
    try: return f"{float(v):.1f}%"
    except: return "?"

def pcol(v, bad=20, warn=10):
    try:
        f = float(v) * (100 if float(v) <= 1 else 1)
        return "red" if f > bad else ("yellow" if f > warn else "green")
    except:
        return "white"

# ── Извлекаем конкретное поле безопасности по списку возможных ключей ─────────
def _sec_pct(sec, keys):
    """Пробуем все возможные имена поля, возвращаем (float_value, key_found)."""
    for k in keys:
        v = sec.get(k)
        if v is not None:
            try:
                f = float(v)
                # Значения могут быть 0..1 или 0..100
                return (f * 100 if f <= 1.0 and f > 0 else f), k
            except:
                pass
    return None, None

# ── Verdict ───────────────────────────────────────────────────────────────────
def verdict(info, sec, stat=None):
    """sec = /v1/token/security, stat = дополнительный endpoint (bundle/phishing)."""
    score = 0
    reasons = []
    combined = {}
    combined.update(sec)
    if stat:
        combined.update(stat)

    # Bundle — в security нет, ищем в stat или pool
    bv, _ = _sec_pct(combined, ["bundled_percentage", "bundle_percentage",
                                  "bundled_pct", "bundle_pct", "bundles",
                                  "bundle_rate"])
    if bv is not None:
        if bv > 25:   score += 3; reasons.append(f"бандлы {bv:.0f}%")
        elif bv > 10: score += 1; reasons.append(f"бандлы {bv:.0f}%")

    sv, _ = _sec_pct(combined, ["sniper_percentage", "sniper_pct", "snipers",
                                  "snipe_percentage"])
    if sv is not None and sv > 20:
        score += 2; reasons.append(f"снайперы {sv:.0f}%")

    ph, _ = _sec_pct(combined, ["phishing_percentage", "phishing_pct",
                                  "phishing", "phishing_rate"])
    if ph is not None and ph > 15:
        score += 2; reasons.append(f"фишинг {ph:.0f}%")

    ins, _ = _sec_pct(combined, ["insider_percentage", "insider_pct",
                                   "insiders", "insider_rate"])
    if ins is not None and ins > 10:
        score += 1; reasons.append(f"инсайдеры {ins:.0f}%")

    # Mint / Freeze — GMGN использует renounced_mint / renounced_freeze_account
    # renounced=True означает ЗАКРЫТ (хорошо), False/null = открыт (плохо)
    mint_closed   = sec.get("renounced_mint")
    freeze_closed = sec.get("renounced_freeze_account")
    mint_open     = sec.get("mint_authority") or sec.get("mintable")
    freeze_open   = sec.get("freeze_authority") or sec.get("freezable")

    if mint_open or (mint_closed is False):
        score += 2; reasons.append("mint не закрыт")
    if freeze_open or (freeze_closed is False):
        score += 1; reasons.append("freeze не закрыт")

    # Honeypot
    if sec.get("is_honeypot") or int(sec.get("honeypot", 0) or 0) > 0:
        score += 3; reasons.append("honeypot!")

    # Tax
    try:
        tax = max(float(sec.get("buy_tax") or 0), float(sec.get("sell_tax") or 0))
        if tax > 10: score += 2; reasons.append(f"налог {tax:.0f}%")
        elif tax > 5: score += 1; reasons.append(f"налог {tax:.0f}%")
    except: pass

    if sec.get("dev_sold") or sec.get("creator_sold"):
        score += 2; reasons.append("дев продал")

    tv, _ = _sec_pct(combined, ["top_10_holder_rate", "top10_holder_rate",
                                  "top_10_holder_percent", "top10_percent"])
    if tv is not None:
        if tv > 80:   score += 2; reasons.append(f"топ-10={tv:.0f}%")
        elif tv > 60: score += 1; reasons.append(f"топ-10={tv:.0f}%")

    if score >= 4: return "🔴", " | ".join(reasons[:3]) if reasons else "высокий риск"
    if score >= 2: return "🟡", " | ".join(reasons[:3]) if reasons else "средний риск"
    return "🟢", "проходит базовые проверки"

# ── Project / GitHub checks ───────────────────────────────────────────────────
def check_github(github_url):
    """Проверить репо на признаки скама."""
    m = re.search(r'github\.com/([\w\-]+)/([\w\-]+)', github_url)
    if not m:
        return None
    owner, repo = m.group(1), m.group(2)
    try:
        hdr = {"Accept": "application/vnd.github.v3+json",
               "User-Agent": "Mozilla/5.0"}
        api = requests.get(
            f"https://api.github.com/repos/{owner}/{repo}", headers=hdr, timeout=8
        ).json()
        cmts = requests.get(
            f"https://api.github.com/repos/{owner}/{repo}/commits?per_page=10",
            headers=hdr, timeout=8,
        ).json()

        stars    = api.get("stargazers_count", 0)
        forks    = api.get("forks_count", 0)
        updated  = (api.get("updated_at") or "")[:10]
        n_commits = len(cmts) if isinstance(cmts, list) else 0

        signals = []
        if stars == 0 and forks == 0:
            signals.append("0 звёзд/форков")
        if n_commits <= 2:
            signals.append(f"мало коммитов ({n_commits})")
        if updated and updated < "2025-01-01":
            signals.append(f"не обновлялся с {updated}")
        if not api.get("description"):
            signals.append("нет описания репо")

        return {
            "url": f"https://github.com/{owner}/{repo}",
            "stars": stars, "forks": forks,
            "commits": n_commits, "updated": updated,
            "signals": signals,
        }
    except Exception:
        return None

def check_website(url):
    """Проверить сайт, найти GitHub и whitepaper."""
    result = {"url": url, "ok": False, "github": None, "has_whitepaper": False}
    if not url:
        return result
    try:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        r = requests.get(url, timeout=10, verify=False, allow_redirects=True, headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,*/*",
            "Accept-Language": "en-US,en;q=0.9",
        })
        if r.status_code == 200:
            result["ok"] = True
            body = r.text
            m = re.search(r'github\.com/([\w\-]+)/([\w\-]+)', body)
            if m:
                result["github"] = f"https://github.com/{m.group(1)}/{m.group(2)}"
            result["has_whitepaper"] = bool(
                re.search(r'whitepaper|white\s+paper|litepaper', body, re.I)
            )
    except Exception:
        pass
    return result

# ── Insider detection ─────────────────────────────────────────────────────────
# Адреса Solana system programs / DEX programs (не настоящие кошельки)
SYSTEM_PROGRAMS = {
    "11111111111111111111111111111111",
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJe1bY8",
    "So11111111111111111111111111111111111111112",
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",  # Raydium AMM
    "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP",  # Orca
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",   # Whirlpool
    "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB",   # Jupiter
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",  # Jupiter v6
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",  # Raydium CAMM
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",   # Meteora DLMM
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EkSX2zN8",  # Meteora
}

def _is_real_wallet(addr):
    """Проверяем, что адрес — настоящий кошелёк, а не программа."""
    if not addr or len(addr) < 30:
        return False
    return addr not in SYSTEM_PROGRAMS

def find_insiders(ca, launch_ts, console=None):
    """
    Потенциальные инсайдеры — ищем два паттерна:

    Паттерн A — "ранний покупатель":
      - первая транзакция с токеном в окне 0–30 мин после запуска
      - WR > 50%, не новый кошелёк (>= 10 сделок)

    Паттерн B — "получил токены + докупил" (сильный сигнал):
      - получил токены переводом от другого кошелька (не от DEX)
      - потом сам купил ещё на рынке
      Это классический insider pattern — аффилированные кошельки.
    """
    if not launch_ts:
        return []

    try:
        launch_f = float(launch_ts)
    except:
        return []

    win_end = launch_f + 1800  # 0–30 мин после запуска

    if console and RICH:
        t_launch = datetime.fromtimestamp(launch_f, tz=timezone.utc).strftime("%H:%M")
        t_end    = datetime.fromtimestamp(win_end,  tz=timezone.utc).strftime("%H:%M")
        console.print(f"  [dim]Окно анализа: {t_launch}–{t_end} UTC (30 мин)[/dim]")

    # ── Загружаем ранние трансферы токена из Solscan ──────────────────────
    if not SOLSCAN_OK:
        console and RICH and console.print("  [red]Solscan недоступен — пропускаем[/red]")
        return []

    solscan_raw = solscan_early_buyers(ca, 200)

    if not solscan_raw:
        # GMGN trades как fallback
        gmgn_tr = g_trades(ca, 300)
        for tx in gmgn_tr:
            ts = float(tx.get("timestamp") or tx.get("block_time") or 0)
            if launch_f <= ts <= win_end:
                solscan_raw.append({
                    "block_time":   ts,
                    "from_address": tx.get("wallet_address") or tx.get("from_address"),
                    "to_address":   tx.get("to_address", ""),
                    "cost_usd":     _scalar(tx.get("cost_usd") or 0),
                })

    if not solscan_raw:
        return []

    # ── Парсим трансферы ──────────────────────────────────────────────────
    # received[wallet] = [{"from": ..., "ts": ...}]  ← кто переводил этому кошу токены
    # buyers[wallet]   = {"ts": ..., "cost_usd": ..., "min_after": ...}  ← кто покупал с рынка
    received = defaultdict(list)
    buyers   = {}

    for tx in solscan_raw:
        ts = float(tx.get("block_time", 0) or 0)
        if not (launch_f <= ts <= win_end):
            continue
        fa = str(tx.get("from_address") or tx.get("sender") or "").strip()
        ta = str(tx.get("to_address")   or tx.get("receiver") or "").strip()
        delta_sec = ts - launch_f
        min_after = f"+{int(delta_sec//60)}m{int(delta_sec%60)}s"

        # Паттерн B: кошелёк ПОЛУЧИЛ токены от другого (не от программы)
        if _is_real_wallet(ta) and _is_real_wallet(fa):
            received[ta].append({"from": fa, "ts": ts, "min_after": min_after})

        # Паттерн A: кошелёк КУПИЛ токены с рынка (from = программа/DEX или системный)
        if _is_real_wallet(ta) and not _is_real_wallet(fa):
            cost = _scalar(tx.get("cost_usd") or tx.get("amount_usd") or 0)
            delta_sec = ts - launch_f
            if ta not in buyers or cost > buyers[ta]["cost_usd"]:
                buyers[ta] = {
                    "wallet":    ta,
                    "cost_usd":  cost,
                    "ts":        ts,
                    "min_after": min_after,
                    "pattern":   "A",  # ранний покупатель
                }

    # Кошельки из паттерна B (получил трансфер), которые потом докупали
    combo_wallets = set(received.keys()) & set(buyers.keys())
    for w in combo_wallets:
        buyers[w]["pattern"] = "B"  # апгрейд — получил И купил
        buyers[w]["received_from"] = received[w][0]["from"]  # кто прислал

    # Добавляем "только получили" (без подтверждённой покупки) как отдельных кандидатов
    only_received = set(received.keys()) - set(buyers.keys())
    for w in only_received:
        r = received[w][0]
        buyers[w] = {
            "wallet":        w,
            "cost_usd":      0,
            "ts":            r["ts"],
            "min_after":     r["min_after"],
            "pattern":       "B?",  # получил, покупку пока не видим
            "received_from": r["from"],
        }

    candidates = list(buyers.values())

    # Фильтр $500 если есть USD данные
    has_usd = any(v["cost_usd"] >= 500 for v in candidates)
    if has_usd:
        candidates = [v for v in candidates if v["cost_usd"] >= 500 or v["pattern"] in ("B", "B?")]

    candidates = candidates[:30]

    if console and RICH:
        console.print(f"  [dim]Проверяю {len(candidates)} кандидатов через GMGN…[/dim]")

    # ── Проверяем качество через GMGN stats ───────────────────────────────
    def check_one(cand):
        stats = g_stats(cand["wallet"]) or {}
        wr    = (stats.get("pnl_stat") or {}).get("winrate") or stats.get("winrate") or stats.get("win_rate")
        pnl   = _scalar(stats.get("realized_profit") or stats.get("total_profit_usd") or 0)
        buys  = int(stats.get("buy_count")  or stats.get("total_buy")  or stats.get("buy")  or 0)
        sells = int(stats.get("sell_count") or stats.get("total_sell") or stats.get("sell") or 0)
        total_tx = buys + sells

        try:
            wr_f = float(wr) * (100 if float(wr) <= 1 else 1) if wr else 0
        except:
            wr_f = 0

        # Паттерн B и B? проходят даже с пустой статистикой (новый кош — это тоже сигнал)
        pattern = cand.get("pattern", "A")
        if pattern in ("B", "B?"):
            return {**cand, "winrate": wr_f, "pnl": pnl, "total_tx": total_tx,
                    "new_wallet": total_tx < 10}

        # Паттерн A: требуем WR > 50% и опыт
        if wr_f > 50 and total_tx >= 10:
            return {**cand, "winrate": wr_f, "pnl": pnl, "total_tx": total_tx,
                    "new_wallet": False}
        return None

    insiders = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(check_one, c): c for c in candidates}
        for fut in as_completed(futs):
            try:
                r = fut.result()
                if r:
                    insiders.append(r)
            except:
                pass

    # Сортируем: B паттерн первый, потом по WR
    def _sort_key(x):
        p = x.get("pattern", "A")
        p_score = 0 if p == "B" else (1 if p == "B?" else 2)
        return (p_score, -x.get("winrate", 0))

    return sorted(insiders, key=_sort_key)

# ── Stage 1 ───────────────────────────────────────────────────────────────────
def stage1(ca):
    print(f"\n🔍 Анализирую {ca[:8]}…{ca[-4:]}")

    # Показываем статус пула ключей
    n_keys = _gmgn_pool.count
    if n_keys > 1:
        print(f"  🔑 GMGN ключей в пуле: {n_keys}")
    elif DEBUG:
        print(f"  🔑 GMGN: 1 ключ (добавь GMGN_API_KEYS=k1,k2,k3 в .env для ротации)")

    # Синхронизируем время ДО параллельных запросов
    get_server_time()
    offset_info = f"(offset={_time_offset:+d}s)" if _time_offset else ""
    if _time_offset:
        print(f"  ⏱ Время синхронизировано {offset_info}")

    t0 = time.time()

    with ThreadPoolExecutor(max_workers=9) as ex:
        fi   = ex.submit(g_info,               ca)
        fs   = ex.submit(g_sec,                ca)
        fp   = ex.submit(g_pool,               ca)
        fh   = ex.submit(g_holders,            ca, 10)
        fdx  = ex.submit(dexscreener_get,      ca)
        fst  = ex.submit(g_stat,               ca)
        fvas = ex.submit(gmgn_vas_holders,     ca)   # фронтенд API: bundle/phishing/insider
        fcr  = ex.submit(helius_get_creator,   ca) if HELIUS_KEY else None
        fscr = ex.submit(solscan_token_creator, ca) if SOLSCAN_OK else None  # Solscan creator

    info        = fi.result()
    sec         = fs.result()
    pool        = fp.result()
    holders     = fh.result()
    dex         = fdx.result()
    stat        = fst.result()
    vas_holders = fvas.result()
    vas         = analyze_vas_holders(vas_holders)  # bundle%, transfer_in%, pattern_b, CEX
    elapsed = time.time() - t0

    # ── Деплоер токена — определяем один раз, используем везде ───────────

    # Собираем все известные адреса пулов/пар — их НИКОГДА не считаем деплоером
    _known_pool_addrs = set()
    for _pa_key in ("pairAddress", "pair_address", "poolAddress", "pool_address", "address"):
        _pa = (dex or {}).get(_pa_key) or ""
        if _pa and 30 <= len(_pa) <= 50:
            _known_pool_addrs.add(_pa)

    def _addr(v):
        """Возвращает строку-адрес только если это Solana-like строка (32-44 символа)
        и НЕ является известным адресом пула/пары."""
        if isinstance(v, str) and 30 <= len(v) <= 50 and v not in _known_pool_addrs:
            return v
        return None

    # ── Приоритет 0: явные поля GMGN sec/info ────────────────────────────
    token_creator = (
        _addr(sec.get("creator"))            or _addr(sec.get("dev_address"))    or
        _addr(sec.get("owner"))              or _addr(sec.get("creator_address")) or
        _addr(sec.get("dev"))                or _addr(sec.get("deployer"))        or
        _addr(sec.get("dev_wallet"))         or _addr(sec.get("deployer_address")) or
        _addr(sec.get("mint_authority"))     or _addr(sec.get("update_authority")) or
        _addr(info.get("creator"))           or _addr(info.get("dev"))            or
        _addr(info.get("deployer"))          or _addr(info.get("owner"))          or
        _addr(fcr.result()  if fcr  else None) or
        _addr(fscr.result() if fscr else None) or ""   # ← Solscan creator (параллельно)
    )

    # ── Приоритет 0.5: полный скан ВСЕХ строковых полей sec/info ─────────
    # GMGN может называть поле иначе (dev_token, wallet_address, и т.д.)
    if not token_creator:
        def _deep_scan_addr(d, skip_keys=None, depth=0):
            """Рекурсивно ищем первую строку похожую на Solana-адрес."""
            if depth > 3 or not isinstance(d, dict):
                return None
            skip = skip_keys or set()
            for k, v in d.items():
                if k in skip:
                    continue
                if isinstance(v, str):
                    a = _addr(v)
                    if a and a not in SYSTEM_PROGRAMS:
                        return a
                elif isinstance(v, dict):
                    r = _deep_scan_addr(v, skip, depth + 1)
                    if r:
                        return r
            return None
        # Сканируем sec, пропуская поля которые точно не creator
        _skip = {
            # snake_case
            "address", "ca", "token", "mint", "pool", "pair", "lp",
            "program", "router", "dex", "amm", "liquidity_pool",
            "pool_address", "pair_address", "token_address",
            # camelCase (DexScreener / GMGN поля)
            "pairAddress", "poolAddress", "tokenAddress", "baseToken", "quoteToken",
            "pairCreatedAt", "chainId", "dexId", "labels", "url",
            # числовые/ценовые поля (строки но не адреса)
            "priceUsd", "priceNative", "volume", "liquidity", "fdv", "marketCap",
            "txns", "priceChange",
        }
        _scanned = _deep_scan_addr(sec, _skip) or _deep_scan_addr(info, _skip)
        if _scanned:
            token_creator = _scanned
            if DEBUG:
                print(f"  DEBUG: creator найден скан-полей: {token_creator[:20]}…")

    # ── Fallback 1: VAS main_distributor (тот кто раздал больше всего токенов) ──
    # Для Meteora: деплоер получает токены на свой кошелёк и раздаёт их — это main_distributor
    if not token_creator:
        _md = vas.get("main_distributor") or ""
        if _md and _addr(_md) and _md not in SYSTEM_PROGRAMS:
            token_creator = _md
            if DEBUG:
                print(f"  DEBUG: creator найден через VAS main_distributor: {token_creator[:20]}…")

    # ── Fallback 2: Rugcheck API (публичный, без ключа, надёжный) ────────
    # Самый простой источник — Rugcheck сам парсит on-chain данные
    if not token_creator:
        _rc = rugcheck_creator(ca)
        if _rc:
            token_creator = _rc
            if DEBUG:
                print(f"  DEBUG: creator найден через Rugcheck: {token_creator[:20]}…")

    # ── Fallback 3: mintAuthority через getAccountInfo (1 быстрый RPC) ───
    if not token_creator and HELIUS_KEY:
        _ma = helius_get_mint_authority(ca) or ""
        if _ma and _ma not in _known_pool_addrs:
            token_creator = _ma

    # ── Fallback 4: feePayer минта через Helius RPC ───────────────────────
    if not token_creator and HELIUS_KEY:
        _tx_creator = helius_get_creator_from_tx(ca)
        if _tx_creator and _tx_creator not in _known_pool_addrs:
            token_creator = _tx_creator
            if DEBUG:
                print(f"  DEBUG: creator найден через Helius RPC tx: {token_creator[:20]}…")

    # ── Fallback 5: feePayer минта через ПУБЛИЧНЫЙ Solana RPC ─────────────
    # Helius бесплатный тир не индексирует все аккаунты — public RPC шире
    if not token_creator:
        if DEBUG:
            print("  DEBUG: пробую публичный Solana RPC для минта…")
        _pub = public_rpc_creator_from_addr(ca, "mint")
        if _pub and _pub not in _known_pool_addrs:
            token_creator = _pub
            if DEBUG:
                print(f"  DEBUG: creator найден через public RPC (mint): {token_creator[:20]}…")

    # ── Fallback 6: feePayer пула через Helius RPC (Meteora DAMM v2) ──────
    if not token_creator and HELIUS_KEY:
        _pair_addr = (dex.get("pairAddress") or dex.get("pair_address") or
                      dex.get("poolAddress") or dex.get("address") or "")
        if _pair_addr:
            if DEBUG:
                print(f"  DEBUG: Meteora → пробуем Helius RPC на пул {_pair_addr[:20]}…")
            _pool_creator = helius_creator_from_pool(_pair_addr)
            if _pool_creator and _pool_creator not in _known_pool_addrs:
                token_creator = _pool_creator
                if DEBUG:
                    print(f"  DEBUG: creator найден через Helius pool: {token_creator[:20]}…")

    # ── Fallback 7: feePayer пула через ПУБЛИЧНЫЙ RPC (Meteora) ───────────
    if not token_creator:
        _pair_addr = (dex.get("pairAddress") or dex.get("pair_address") or
                      dex.get("poolAddress") or dex.get("address") or "")
        if _pair_addr:
            if DEBUG:
                print(f"  DEBUG: Meteora → пробуем public RPC на пул {_pair_addr[:20]}…")
            _pub_pool = public_rpc_creator_from_addr(_pair_addr, "pool")
            if _pub_pool and _pub_pool not in _known_pool_addrs:
                token_creator = _pub_pool
                if DEBUG:
                    print(f"  DEBUG: creator найден через public RPC (pool): {token_creator[:20]}…")

    # ── Fallback 8: Helius Enhanced Transactions API ──────────────────────
    if not token_creator and HELIUS_KEY:
        if DEBUG:
            print("  DEBUG: пробую Helius Enhanced API…")
        _enh = helius_enhanced_creator(ca)
        if _enh and _enh not in _known_pool_addrs:
            token_creator = _enh
            if DEBUG:
                print(f"  DEBUG: creator найден через Enhanced API: {token_creator[:20]}…")

    # ── Fallback 9: Solscan прямой HTTP (с sol-aut заголовком) ───────────
    if not token_creator:
        _sol_direct = solscan_direct_creator(ca)
        if _sol_direct and _sol_direct not in _known_pool_addrs:
            token_creator = _sol_direct
            if DEBUG:
                print(f"  DEBUG: creator найден через Solscan direct: {token_creator[:20]}…")

    # ── Возраст токена (нужен для поиска фандеров) ───────────────────────
    _early_age = (info.get("pool_creation_timestamp") or info.get("created_at") or
                  info.get("open_timestamp"))
    if not _early_age and dex.get("pairCreatedAt"):
        _early_age = float(dex["pairCreatedAt"]) / 1000

    # ── Сверяем деплоера с монитором ─────────────────────────────────────
    _mon_tree    = load_monitor_tree()
    _creator_mon = _mon_tree.get(token_creator) if token_creator else None
    _creator_via = None   # промежуточный кошелёк через который нашли связь

    # Приоритет 0: monitor уже видел этот токен → мастер известен сразу
    if not _creator_mon:
        _mon_tok = load_monitor_token(ca)
        if _mon_tok:
            _master_addr = _mon_tok["master"]
            _creator_mon = {
                "depth":        0,
                "path":         [_master_addr],
                "master":       _master_addr,
                "master_label": _mon_tok["master_label"],
            }
            _creator_via = {
                "addr":          _mon_tok["deployer"],
                "_from_monitor": True,
                "detected_at":   _mon_tok["detected_at"],
            }

    # Если деплоер сам не в дереве — смотрим на 1-2 уровня его фандеров
    if not _creator_mon and token_creator and _mon_tree and HELIUS_KEY and _early_age:
        try:
            # Окно 30 дней — токены могли деплоиться задолго до запуска monitor
            _funders1 = (
                helius_funding_chain(token_creator, _early_age, window_hours=48)
                or helius_all_incoming_sol(token_creator, max_sigs=100)
            )
            for _f1 in _funders1[:8]:
                _fa = _f1.get("funder", "")
                if _fa and _fa in _mon_tree:
                    _creator_mon  = _mon_tree[_fa]
                    _creator_via  = {"addr": _fa, "sol": _f1["amount_sol"],
                                     "level": 1}
                    break
            # Если не нашли на уровне 1 — смотрим уровень 2
            if not _creator_mon:
                for _f1 in _funders1[:3]:
                    _fa = _f1.get("funder", "")
                    if not _fa:
                        continue
                    _funders2 = helius_all_incoming_sol(_fa, max_sigs=50)
                    for _f2 in _funders2[:5]:
                        _fb = _f2.get("funder", "")
                        if _fb and _fb in _mon_tree:
                            _creator_mon = _mon_tree[_fb]
                            _creator_via = {"addr": _fb, "sol": _f2["amount_sol"],
                                            "via": _fa, "level": 2}
                            break
                    if _creator_mon:
                        break
        except Exception:
            pass
    ve, vr  = verdict(info, sec, stat)

    # ── Non-rich fallback ─────────────────────────────────────────────────
    if not RICH:
        mc0 = _scalar(info.get("market_cap") or info.get("mc") or 0)
        print(f"{ve} {vr} ({elapsed:.1f}s)")
        print(f"MC:{fusd(mc0)} Vol:{fusd(info.get('volume_24h'))}")
        print(f"Бандлы:{sec.get('bundled_percentage','?')} Снайперы:{sec.get('sniper_percentage','?')}")
        return {"ca": ca, "info": info, "sec": sec}

    c = Console()

    # ── Заголовок ─────────────────────────────────────────────────────────
    c.print(Panel(
        f"[bold]{ve}  {vr}[/bold]",
        title=f"[cyan]{info.get('symbol','?')} · {ca[:10]}…[/cyan]",
        subtitle=f"[dim]{elapsed:.1f}s[/dim]",
        border_style="cyan",
    ))

    # ── 🚨 Алерт: деплоер — отслеживаемый дев ────────────────────────────
    if _creator_mon:
        _cm_depth = _creator_mon.get("depth", 0)
        _cm_label = _creator_mon.get("master_label") or _creator_mon.get("master", "")[:16]
        _cm_path  = _creator_mon.get("path") or []
        _cm_chain = " → ".join(
            (f"{a[:6]}…{a[-4:]}" if len(a) > 12 else a) for a in _cm_path
        )

        # Строим строку цепочки с учётом промежуточных кошельков
        if _creator_via:
            if _creator_via.get("_from_monitor"):
                # Связь найдена через таблицу tokens в monitor.db
                _dep = _creator_via.get("addr", token_creator)
                _det = _creator_via.get("detected_at", "")
                _det_str = (datetime.fromtimestamp(float(_det), tz=timezone.utc)
                            .strftime("%Y-%m-%d %H:%M") if _det else "")
                _full_chain = (
                    f"[bold]{_cm_label}[/bold] [dim]({_creator_mon.get('master','')[:8]}…)[/dim]"
                    f" → [red]{_dep[:8]}…{_dep[-4:]}[/red] (деплоер)"
                    + (f"  [dim]задетектирован: {_det_str}[/dim]" if _det_str else "")
                )
                _alert_title = "monitor.db ✓"
            else:
                _via_level = _creator_via.get("level", 1)
                _via_addr  = _creator_via.get("addr", "")
                _via_mid   = _creator_via.get("via", "")
                _via_sol   = _creator_via.get("sol", 0)
                if _via_level == 1:
                    _full_chain = (
                        f"[bold]{_cm_label}[/bold] [dim]({_creator_mon.get('master','')[:8]}…)[/dim]"
                        f" → [yellow]{_via_addr[:8]}…{_via_addr[-4:]}[/yellow] ({_via_sol:.2f} SOL)"
                        f" → [red]{token_creator[:8]}…{token_creator[-4:]}[/red] (деплоер)"
                    )
                else:
                    _full_chain = (
                        f"[bold]{_cm_label}[/bold] → [dim]{_via_addr[:8]}…[/dim]"
                        f" → [yellow]{_via_mid[:8]}…{_via_mid[-4:]}[/yellow]"
                        f" → [red]{token_creator[:8]}…{token_creator[-4:]}[/red] (деплоер)"
                    )
                _alert_title = "через фандера"
        else:
            _full_chain = _cm_chain + f" → [red]{token_creator[:8]}…{token_creator[-4:]}[/red]"
            _alert_title = "прямая связь"

        if _cm_depth == 0 and not _creator_via:
            _msg = f"[bold red]🚨 ТОКЕН ЗАДЕПЛОЕН МАСТЕР-КОШЕЛЬКОМ НАПРЯМУЮ![/bold red]"
        else:
            _msg = f"[bold red]🚨 ДЕПЛОЕР СВЯЗАН С ОТСЛЕЖИВАЕМЫМ ДЕВОМ ({_alert_title})![/bold red]"

        c.print(Panel(
            f"{_msg}\n"
            f"[yellow]Мастер: [bold]{_cm_label}[/bold][/yellow]\n"
            f"[dim]Цепочка: {_full_chain}[/dim]",
            border_style="red",
            title="[bold red]⚡ MONITOR ALERT[/bold red]",
        ))
    elif token_creator:
        # Деплоер известен но не в monitor — показываем тихо
        c.print(f"  [dim]👨‍💻 Деплоер: {token_creator}[/dim]")

    # ── Базовые метрики (GMGN + DexScreener fallback) ────────────────────
    # DexScreener — надёжный источник для MC, Volume, Age, DEX
    dx_liq  = _scalar((dex.get("liquidity") or {}).get("usd") or 0)
    dx_mc   = _scalar(dex.get("fdv") or dex.get("marketCap") or 0)
    dx_vol  = _scalar((dex.get("volume") or {}).get("h24") or 0)
    dx_age  = dex.get("pairCreatedAt")  # миллисекунды
    dx_dex  = dex.get("dexId") or ""
    dx_price= _scalar(dex.get("priceUsd") or 0)
    dx_ch24 = dex.get("priceChange", {}).get("h24")  # % изменение

    mc    = _scalar(info.get("market_cap") or info.get("mc") or 0) or dx_mc
    price = _scalar(info.get("price") or info.get("price_usd") or 0) or dx_price
    liq   = _scalar(pool.get("liquidity") or pool.get("liquidity_usd") or
                    info.get("liquidity") or 0) or dx_liq
    vol   = _scalar(info.get("volume_24h") or info.get("vol_24h") or 0) or dx_vol
    dex_name = (pool.get("dex_id") or pool.get("dex") or info.get("dex") or dx_dex or "?")

    # Возраст: GMGN секунды или DexScreener миллисекунды
    age = (info.get("pool_creation_timestamp") or info.get("created_at") or
           info.get("open_timestamp"))
    if not age and dx_age:
        age = float(dx_age) / 1000  # ms → s

    hcnt  = info.get("holder_count") or info.get("holders") or sec.get("holder_count") or "?"
    mcc   = "green" if 10_000 <= mc <= 500_000 else "yellow"

    t = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
    t.add_column("", style="dim", width=22)
    t.add_column("")

    t.add_row("💰 Market Cap",  f"[{mcc}]{fusd(mc)}[/{mcc}]" if mc else "[dim]?[/dim]")
    t.add_row("💵 Цена",        f"${price:.8f}" if price else "?")
    t.add_row("📊 Объём 24h",   fusd(vol) if vol else "?")
    if dx_ch24 is not None:
        try:
            ch = float(dx_ch24)
            ch_col = "green" if ch >= 0 else "red"
            t.add_row("📈 24h изм.",  f"[{ch_col}]{ch:+.1f}%[/{ch_col}]")
        except: pass
    t.add_row("💧 Ликвидность", fusd(liq) if liq else "?")
    t.add_row("⏰ Возраст",      fage(age))
    t.add_row("👥 Холдеров",    str(hcnt))
    t.add_row("🏪 DEX",         dex_name)
    t.add_row("", "")

    # Объединяем sec + stat для поиска всех полей
    combined = {}
    combined.update(sec)
    if stat:
        combined.update(stat)

    def _pct_row(label, val, bad=20, warn=10):
        if val is None:
            t.add_row(label, "[dim]нет данных[/dim]")
        else:
            col = "red" if val > bad else ("yellow" if val > warn else "green")
            t.add_row(label, f"[{col}]{val:.1f}%[/{col}]")

    # Bundle / Sniper / Phishing / Insider
    # Приоритет: VAS API (фронтенд, реальные данные) → combined (security + stat)
    bundle_api, _ = _sec_pct(combined, ["bundled_percentage", "bundle_percentage",
                                         "bundled_pct", "bundle_pct", "bundles", "bundle_rate"])
    sniper_v,   _ = _sec_pct(combined, ["sniper_percentage", "sniper_pct",
                                         "snipers", "snipe_percentage"])
    phish_api,  _ = _sec_pct(combined, ["phishing_percentage", "phishing_pct",
                                         "phishing", "phishing_rate"])
    ins_v,      _ = _sec_pct(combined, ["insider_percentage", "insider_pct",
                                         "insiders", "insider_rate"])

    # VAS данные перезаписывают (они точнее — рассчитаны напрямую по holder list)
    bundle_v = vas.get("bundle_pct") if vas else bundle_api
    if bundle_v is None:
        bundle_v = bundle_api
    phish_v  = vas.get("transfer_in_pct") if vas else phish_api
    if phish_v is None:
        phish_v  = phish_api

    _pct_row("📦 Бандлы",    bundle_v, bad=25, warn=10)
    _pct_row("🎯 Снайперы",  sniper_v, bad=20, warn=10)
    _pct_row("🎣 Фишинг",    phish_v,  bad=20, warn=10)
    _pct_row("🕵 Инсайдеры", ins_v,    bad=15, warn=5)

    # Top-10 (confirmed field: top_10_holder_rate = "0.2179")
    top10_v, _ = _sec_pct(combined, ["top_10_holder_rate", "top10_holder_rate",
                                      "top_10_holder_percent", "top10_percent"])
    if top10_v is not None:
        col = "red" if top10_v > 80 else ("yellow" if top10_v > 60 else "green")
        t.add_row("🔝 Топ-10 %", f"[{col}]{top10_v:.1f}%[/{col}]")

    # Honeypot
    is_hp = sec.get("is_honeypot") or int(sec.get("honeypot", 0) or 0) > 0
    if is_hp is not None:
        t.add_row("🍯 Honeypot", "[red]⚠ ДА[/red]" if is_hp else "[green]нет ✓[/green]")

    # Tax
    try:
        buy_tax  = float(sec.get("buy_tax")  or 0)
        sell_tax = float(sec.get("sell_tax") or 0)
        if buy_tax > 0 or sell_tax > 0:
            tax_col = "red" if max(buy_tax, sell_tax) > 5 else "yellow"
            t.add_row("💸 Налог",
                      f"[{tax_col}]buy {buy_tax:.1f}% / sell {sell_tax:.1f}%[/{tax_col}]")
    except: pass

    # Burn
    burn = _scalar(sec.get("burn_ratio") or 0)
    if burn > 0:
        t.add_row("🔥 Burn",  f"[green]{burn*100:.1f}%[/green]")

    # Mint / Freeze — используем renounced_* (подтверждено из debug)
    mint_closed   = sec.get("renounced_mint")            # True = закрыт ✓
    freeze_closed = sec.get("renounced_freeze_account")  # True = закрыт ✓
    # Fallback на старые имена
    mint_open     = sec.get("mint_authority") or sec.get("mintable")
    freeze_open   = sec.get("freeze_authority") or sec.get("freezable")

    if mint_closed is not None:
        t.add_row("🔑 Mint", "[green]закрыт ✓[/green]" if mint_closed else "[red]⚠ открыт[/red]")
    elif mint_open is not None:
        t.add_row("🔑 Mint", "[red]⚠ открыт[/red]" if mint_open else "[green]закрыт ✓[/green]")

    if freeze_closed is not None:
        t.add_row("❄️  Freeze", "[green]нет ✓[/green]" if freeze_closed else "[red]⚠ есть[/red]")
    elif freeze_open is not None:
        t.add_row("❄️  Freeze", "[red]⚠ есть[/red]" if freeze_open else "[green]нет ✓[/green]")

    dsold = sec.get("dev_sold") or sec.get("creator_sold")
    # Если GMGN не дал ответа — проверяем on-chain через Helius
    if dsold is None and token_creator and HELIUS_KEY:
        _dev_hold = helius_check_dev_holding(token_creator, ca)
        if _dev_hold["holds"] is True:
            dsold = False   # держит → не продал
        elif _dev_hold["holds"] is False:
            dsold = True    # нет баланса → продал
        # holds=None → данных нет, оставляем dsold=None
    t.add_row("👨‍💻 Дев продал", "[red]❌ да[/red]" if dsold else (
        "[green]нет ✓[/green]" if dsold is False else "[dim]?[/dim]"
    ))

    # LP Lock из lock_summary
    # GMGN OpenAPI часто возвращает is_locked=false даже когда LP залочена
    # → показываем "заблокирована" только если ЯВНО true, иначе советуем проверить вручную
    lock_sum  = sec.get("lock_summary") or {}
    is_locked = lock_sum.get("is_locked")
    lplk_v, _ = _sec_pct(combined, ["lp_locked_pct", "lp_lock_ratio", "lp_lock_percent"])
    burn_v    = _scalar(sec.get("burn_ratio") or 0)  # burn = LP сожжена = locked навсегда
    if is_locked is True:
        t.add_row("🔐 LP Lock", "[green]заблокирована ✓[/green]")
    elif burn_v >= 0.95:
        t.add_row("🔐 LP Lock", "[green]сожжена ✓ (навсегда)[/green]")
    elif lplk_v and lplk_v >= 80:
        col = "green"
        t.add_row("🔐 LP Lock", f"[{col}]{lplk_v:.0f}%[/{col}]")
    elif is_locked is False:
        # API вернул false — но это часто неточно. Рекомендуем проверить вручную.
        t.add_row("🔐 LP Lock", "[yellow]API: нет — проверь Rugcheck ↓[/yellow]")
    else:
        t.add_row("🔐 LP Lock", "[dim]нет данных → Rugcheck ↓[/dim]")

    c.print(t)

    # ── Топ холдеры ───────────────────────────────────────────────────────
    if holders:
        c.print("\n[bold]Топ холдеры:[/bold]")
        ht = Table(box=box.SIMPLE, padding=(0, 1))
        ht.add_column("#", width=3, style="dim")
        ht.add_column("Адрес", width=24)
        ht.add_column("%", justify="right", width=8)
        ht.add_column("Тип", width=14)
        for i, h in enumerate(holders[:10], 1):
            addr = h.get("address") or h.get("wallet") or "?"
            pct  = (h.get("amount_percentage") or h.get("percent") or
                    h.get("percentage") or h.get("ratio") or 0)
            try:
                pf = float(pct) * (100 if float(pct) <= 1 else 1)
            except:
                pf = 0
            tag = (h.get("wallet_tag_v2") or h.get("tag") or
                   h.get("label") or h.get("type") or "")
            col = "red" if pf > 10 else ("yellow" if pf > 5 else "white")
            ht.add_row(
                str(i),
                f"[dim]{str(addr)[:22]}…[/dim]",
                f"[{col}]{pf:.2f}%[/{col}]",
                f"[dim]{tag}[/dim]",
            )
        c.print(ht)

    # ── Инсайдер кластер (VAS API данные) ────────────────────────────────
    if vas:
        distributor = vas.get("main_distributor")
        pattern_b   = vas.get("pattern_b", [])
        cex_names   = vas.get("cex_names", [])
        ti_pct      = vas.get("transfer_in_pct", 0)
        dist_pct    = vas.get("dist_pct", 0)
        n_holders   = vas.get("n_holders", 0)

        if distributor or pattern_b:
            c.print("\n[bold yellow]🔗 Инсайдер кластер:[/bold yellow]")
            c.print(f"  [dim]Данные по {n_holders} холдерам из GMGN frontend API[/dim]")

            if distributor:
                c.print(
                    f"\n  [yellow]Главный дистрибьютор[/yellow] "
                    f"[dim](раздал ~{dist_pct:.1f}% supply, итого transfer-in={ti_pct:.1f}%):[/dim]"
                )
                short = distributor[:20] + "…"
                c.print(f"  [bold]{short}[/bold]")
                c.print(f"  [dim]https://gmgn.ai/sol/address/{distributor}[/dim]")
                c.print(f"  [dim]https://solscan.io/account/{distributor}[/dim]")

            if pattern_b:
                c.print(
                    f"\n  [bold]★ Паттерн B — получили токены + докупили "
                    f"({len(pattern_b)} кошельков):[/bold]"
                )
                bt = Table(box=box.SIMPLE, padding=(0, 1))
                bt.add_column("#",         width=3,  style="dim")
                bt.add_column("Кошелёк",   width=24)
                bt.add_column("Вошёл",     width=9,  justify="center")
                bt.add_column("Докупил$",  width=10, justify="right")
                bt.add_column("Txns",      width=5,  justify="right")
                bt.add_column("Позиция",   width=11, justify="right")
                bt.add_column("PnL",       width=10, justify="right")
                bt.add_column("CEX",       width=10)

                for i, wb in enumerate(pattern_b[:12], 1):
                    w_addr      = wb["wallet"]
                    start_ts    = wb.get("start_holding")
                    entry_str   = fage(start_ts) + " назад" if start_ts else "?"
                    cex_str     = wb.get("cex") or ""
                    new_tag     = " [red](NEW)[/red]" if wb.get("is_new") else ""
                    cost_col    = "green" if wb["bought_cost"] > 1000 else "white"
                    pnl_val     = wb.get("profit", 0)
                    pnl_col     = "green" if pnl_val > 0 else "red"
                    pnl_str     = fusd(pnl_val) if pnl_val else "?"

                    bt.add_row(
                        str(i),
                        f"[dim]{w_addr[:22]}…[/dim]{new_tag}",
                        f"[dim]{entry_str}[/dim]",
                        f"[{cost_col}]{fusd(wb['bought_cost'])}[/{cost_col}]",
                        str(wb["buy_count"]),
                        fusd(wb["usd_value"]),
                        f"[{pnl_col}]{pnl_str}[/{pnl_col}]",
                        f"[yellow]{cex_str}[/yellow]" if cex_str else "[dim]—[/dim]",
                    )
                c.print(bt)

                c.print("[dim]GMGN профили:[/dim]")
                for wb in pattern_b[:8]:
                    c.print(f"  [dim]★ https://gmgn.ai/sol/address/{wb['wallet']}[/dim]")

        # CEX funding summary
        if cex_names:
            from collections import Counter
            cex_cnt = Counter(cex_names).most_common(4)
            cex_summary = ", ".join(f"{nm} ×{cnt}" for nm, cnt in cex_cnt)
            c.print(f"\n  [dim]💳 Кошельки пополнялись с CEX: {cex_summary}[/dim]")

    # ── Проект: сайт и GitHub (GMGN + DexScreener) ───────────────────────
    # Собираем соцсети из DexScreener
    dx_info   = dex.get("info") or {}
    dx_sites  = [s.get("url","") for s in (dx_info.get("websites") or []) if s.get("url")]
    dx_socials = {s.get("type","").lower(): s.get("url","")
                  for s in (dx_info.get("socials") or []) if s.get("url")}

    website = (info.get("website") or info.get("homepage") or
               (dx_sites[0] if dx_sites else "")).strip()
    github  = (info.get("github") or "").strip()
    twitter = (info.get("twitter") or info.get("twitter_username") or
               dx_socials.get("twitter", "")).strip()
    tg      = (info.get("telegram") or dx_socials.get("telegram", "")).strip()
    desc    = (info.get("description") or info.get("overview") or "").strip()

    if desc or website or twitter or tg:
        c.print("\n[bold]📋 Проект:[/bold]")
        pt = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
        pt.add_column("", style="dim", width=14)
        pt.add_column("")
        if desc:
            pt.add_row("Описание", desc[:220] + ("…" if len(desc) > 220 else ""))
        if website:
            pt.add_row("Сайт",     website)
        if twitter:
            pt.add_row("Twitter",  f"[dim]{twitter}[/dim]")
        if tg:
            pt.add_row("Telegram", f"[dim]{tg}[/dim]")
        c.print(pt)

        if website:
            c.print("  [dim]Проверяю сайт…[/dim]")
            ws = check_website(website)
            if ws["ok"]:
                c.print("  ✅ Сайт доступен", style="green")
                if ws["has_whitepaper"]:
                    c.print("  📄 Whitepaper найден", style="green")
                if ws["github"] and not github:
                    github = ws["github"]
                    c.print(f"  🔗 GitHub найден: {github}", style="dim")
            else:
                c.print(f"  ❌ Сайт недоступен", style="red")

        if github:
            c.print("  [dim]Проверяю GitHub…[/dim]")
            gh = check_github(github)
            if gh:
                gh_col = "red" if gh["signals"] else "green"
                c.print(f"\n  [bold]GitHub:[/bold] [link={gh['url']}]{gh['url']}[/link]")
                c.print(f"  ⭐ Stars: {gh['stars']} | Forks: {gh['forks']} | Commits: {gh['commits']} | Updated: {gh['updated']}")
                if gh["signals"]:
                    c.print(f"  [{gh_col}]⚠ Скам-сигналы: {' | '.join(gh['signals'])}[/{gh_col}]")
                else:
                    c.print("  [green]✓ GitHub выглядит активным[/green]")
            else:
                c.print(f"  [dim]GitHub не доступен: {github}[/dim]")

    # ── Ранние покупатели (Helius) ────────────────────────────────────────
    # Адаптивное окно: токен < 6ч → 30 мин, < 3д → 60 мин, старше → 120 мин
    _token_age_h = (time.time() - float(age)) / 3600 if age else 999
    if _token_age_h < 6:
        _buy_window = 30
    elif _token_age_h < 72:
        _buy_window = 60
    else:
        _buy_window = 120

    c.print(
        f"\n[bold yellow]⏱ Ранние покупатели "
        f"(первые {_buy_window} мин после запуска):[/bold yellow]"
    )

    if not HELIUS_KEY:
        c.print(
            "  [dim]Helius не настроен. Добавь [bold]HELIUS_KEY=...[/bold] в .env[/dim]\n"
            "  [dim]→ helius.dev → Sign Up (бесплатно) → Dashboard → API Keys[/dim]"
        )
        early_buyers = []
    else:
        pair_addr = dex.get("pairAddress") or dex.get("pair_address") or None

        # Источник 1: Helius по CA токена
        c.print("  [dim]Запрашиваю Helius (по CA токена)…[/dim]")
        early_buyers = helius_early_buyers(ca, age, pair_addr=pair_addr, window_min=_buy_window)

        # Источник 2: Helius по адресу пула (Meteora/Raydium — транзакции там, не на CA)
        if not early_buyers and pair_addr and pair_addr != ca:
            c.print(f"  [dim]Helius пуст по CA → пробую по pairAddress ({pair_addr[:20]}…)[/dim]")
            early_buyers = helius_early_buyers(pair_addr, age, window_min=_buy_window)

        # Источник 3: Solscan fallback (хранит историю дольше Helius)
        if not early_buyers and SOLSCAN_OK:
            c.print("  [dim]Helius пуст → пробую Solscan /token/transfer…[/dim]")
            _sol_rows = solscan_early_buyers(ca, launch_ts=age, window_min=_buy_window)
            if _sol_rows:
                # Преобразуем в формат совместимый с helius_enrich
                early_buyers = []
                for r in _sol_rows:
                    _to = r.get("to_address") or r.get("destination") or ""
                    _ts = float(r.get("block_time") or r.get("blockTime") or 0)
                    _delta = _ts - float(age) if age and _ts else 0
                    _min_after = f"+{int(_delta//60)}m{int(_delta%60)}s" if _delta > 0 else "?"
                    if _to and 30 <= len(_to) <= 50:
                        early_buyers.append({
                            "wallet":    _to,
                            "ts":        _ts,
                            "min_after": _min_after,
                            "tx_type":   "TRANSFER",
                            "cost_usd":  0,
                        })
                c.print(f"  [dim]Solscan: {len(early_buyers)} трансферов в окне[/dim]")

        if early_buyers:
            c.print(f"  [dim]Найдено {len(early_buyers)} покупателей → обогащаю GMGN…[/dim]")
            early_buyers = helius_enrich(early_buyers, console=c)

    if early_buyers:
        # Разбиваем на покупатели (SWAP) и получатели (TRANSFER/бандл)
        swaps     = [e for e in early_buyers if e.get("tx_type") == "SWAP"]
        transfers = [e for e in early_buyers if e.get("tx_type") != "SWAP"]

        def _early_table(entries, label, sol_label):
            if not entries:
                return
            c.print(f"\n  [bold]{label} ({len(entries)}):[/bold]")
            et = Table(box=box.SIMPLE, padding=(0, 1))
            et.add_column("#",        width=3,   style="dim")
            et.add_column("Кошелёк (полный адрес)", width=46)
            et.add_column("Вход",     width=9,   justify="center")
            et.add_column(sol_label,  width=8,   justify="right")
            et.add_column("Токенов",  width=16,  justify="right")
            et.add_column("WR%",      width=7,   justify="right")
            et.add_column("PnL 30d",  width=12,  justify="right")
            et.add_column("Txns",     width=6,   justify="right")

            for i, e in enumerate(entries[:30], 1):
                wr   = e.get("winrate")
                pnl  = e.get("pnl", 0)
                txns = e.get("total_tx", 0)

                wr_col  = ("green"  if wr and wr > 60 else
                           "yellow" if wr and wr > 40 else "dim")
                pnl_col = "green" if pnl > 0 else ("red" if pnl < 0 else "dim")

                wr_str  = (f"[{wr_col}]{wr:.0f}%[/{wr_col}]"
                           if wr is not None else "[dim]?[/dim]")
                pnl_str = (f"[{pnl_col}]{fusd(pnl)}[/{pnl_col}]"
                           if pnl else "[dim]?[/dim]")
                tok_val = e.get("token_received", 0)
                tok_str = f"{tok_val:,.0f}" if tok_val else "[dim]?[/dim]"
                sol_str = f"{e['sol_spent']:.3f}" if e.get("sol_spent") else "[dim]—[/dim]"

                smart = wr and wr > 60 and txns and txns > 20
                addr  = e["wallet"]   # полный адрес (44 символа)
                addr_fmt = (f"[bold green]{addr}[/bold green]"
                            if smart else f"[dim]{addr}[/dim]")

                et.add_row(
                    str(i), addr_fmt,
                    f"[dim]{e.get('min_after','?')}[/dim]",
                    sol_str, tok_str, wr_str, pnl_str,
                    str(txns) if txns else "[dim]?[/dim]",
                )
            c.print(et)

        _early_table(swaps,     "🟢 Купили через DEX (SWAP)", "SOL потрачено")
        _early_table(transfers, "🔴 Получили переводом (TRANSFER = бандл/инсайдер)", "SOL получено")

        # ── Инсайдеры через monitor.db ────────────────────────────────────
        monitor_tree = load_monitor_tree()
        if monitor_tree:
            insider_matches = check_insider_connections(early_buyers, monitor_tree)
            if insider_matches:
                c.print(f"\n  [bold red]🕵 ИНСАЙДЕРЫ — связаны с девом ({len(insider_matches)}):[/bold red]")
                c.print(f"  [dim]Эти ранние покупатели найдены в дереве monitor.py[/dim]")
                ins_t = Table(box=box.SIMPLE, padding=(0, 1))
                ins_t.add_column("#",         width=3,  style="dim")
                ins_t.add_column("Кошелёк",   width=46)
                ins_t.add_column("Тип",       width=9,  justify="center")
                ins_t.add_column("Вход мин",  width=9,  justify="center")
                ins_t.add_column("Уровень",   width=8,  justify="center")
                ins_t.add_column("Цепочка к мастеру", width=40)
                for idx, e in enumerate(insider_matches, 1):
                    path   = e.get("_insider_path") or []
                    depth  = e.get("_insider_depth", "?")
                    label  = e.get("_insider_label") or e.get("_insider_master", "")[:12]
                    chain  = " → ".join(
                        (f"{a[:6]}…{a[-4:]}" if len(a) > 12 else a)
                        for a in path
                    )
                    tx_type = e.get("tx_type") or "?"
                    typ_col = "red" if tx_type != "SWAP" else "yellow"
                    ins_t.add_row(
                        str(idx),
                        f"[bold red]{e['wallet']}[/bold red]",
                        f"[{typ_col}]{tx_type}[/{typ_col}]",
                        f"[dim]{e.get('min_after','?')}[/dim]",
                        f"[red]lvl {depth}[/red]",
                        f"[dim]{chain}[/dim]",
                    )
                c.print(ins_t)
                c.print(f"  [dim]Мастер: {insider_matches[0].get('_insider_label') or insider_matches[0].get('_insider_master','')}[/dim]")
                for e in insider_matches[:5]:
                    c.print(f"  [red]⚠[/red] https://gmgn.ai/sol/address/{e['wallet']}")
            else:
                c.print(f"\n  [dim]🕵 Инсайдеры monitor.db: совпадений нет среди ранних покупателей[/dim]")
        # ──────────────────────────────────────────────────────────────────

        # Дистрибьюторы — кто рассылал токены
        distributors = {}
        for e in transfers:
            d = e.get("distributor") or ""
            if d and d not in SYSTEM_PROGRAMS:
                distributors[d] = distributors.get(d, 0) + 1
        if distributors:
            top_dist = sorted(distributors.items(), key=lambda x: -x[1])
            c.print(f"\n  [bold yellow]⚠ Дистрибьюторы (кто рассылал токены):[/bold yellow]")
            for addr, cnt in top_dist[:3]:
                c.print(f"  [yellow]{addr}[/yellow]  [dim](→ {cnt} кошел. в этом окне)[/dim]")
                c.print(f"  [dim]  https://gmgn.ai/sol/address/{addr}[/dim]")
                c.print(f"  [dim]  https://solscan.io/account/{addr}[/dim]")

            # Запрашиваем все получатели от главного дистрибьютора
            main_dist = top_dist[0][0]
            c.print(f"\n  [dim]Запрашиваю всех получателей от {main_dist[:20]}… через Helius…[/dim]")
            all_recipients = helius_dist_recipients(ca, main_dist, age)
            if all_recipients:
                c.print(
                    f"  [bold red]🔴 Бандл-кластер: {len(all_recipients)} кошельков[/bold red] "
                    f"[dim]получили токены от дистрибьютора[/dim]"
                )
                rt = Table(box=box.SIMPLE, padding=(0, 1))
                rt.add_column("#",        width=3,  style="dim")
                rt.add_column("Кошелёк (получатель)", width=46)
                rt.add_column("Токенов",  width=16, justify="right")
                rt.add_column("Вход",     width=9,  justify="center")
                for j, rec in enumerate(all_recipients[:30], 1):
                    tok_v = rec.get("token_received", 0)
                    tok_s = f"{tok_v:,.0f}" if tok_v else "?"
                    rt.add_row(
                        str(j),
                        f"[dim]{rec['wallet']}[/dim]",
                        tok_s,
                        f"[dim]{rec.get('min_after','?')}[/dim]",
                    )
                c.print(rt)
                if len(all_recipients) > 30:
                    c.print(f"  [dim]… и ещё {len(all_recipients)-30} кошельков[/dim]")
                c.print("\n  [dim]GMGN профили первых 10 получателей:[/dim]")
                for rec in all_recipients[:10]:
                    c.print(f"  [dim]  https://gmgn.ai/sol/address/{rec['wallet']}[/dim]")

        # ── Watch List — качественные независимые трейдеры ──────────────
        watch_list = build_watch_list(early_buyers)
        if watch_list:
            c.print(
                f"\n  [bold green]📋 WATCH LIST — Качественные трейдеры"
                f" ({len(watch_list)}):[/bold green]"
            )
            c.print("[dim]  Критерии: SWAP + WR>50% + PnL>$3K за 30д + ≥20 сделок[/dim]")
            wt = Table(box=box.SIMPLE, padding=(0, 1))
            wt.add_column("#",        width=3,  style="dim")
            wt.add_column("Кошелёк (полный адрес)", width=46)
            wt.add_column("WR%",      width=7,  justify="right")
            wt.add_column("PnL 30d",  width=12, justify="right")
            wt.add_column("Txns",     width=6,  justify="right")
            wt.add_column("Вход",     width=9,  justify="center")
            for idx, e in enumerate(watch_list, 1):
                wr_col  = "green"  if e["winrate"] > 60 else "yellow"
                pnl_col = "green"  if e["pnl"]     > 0  else "red"
                wt.add_row(
                    str(idx),
                    f"[bold green]{e['wallet']}[/bold green]",
                    f"[{wr_col}]{e['winrate']:.0f}%[/{wr_col}]",
                    f"[{pnl_col}]{fusd(e['pnl'])}[/{pnl_col}]",
                    str(e.get("total_tx", "?")),
                    f"[dim]{e.get('min_after','?')}[/dim]",
                )
            c.print(wt)
            c.print("  [bold]GMGN профили Watch List:[/bold]")
            for e in watch_list[:15]:
                c.print(f"  [bold green]★[/bold green] https://gmgn.ai/sol/address/{e['wallet']}")
        else:
            # Показываем «почти» — WR>60% даже если PnL данных нет
            smart_money = [
                e for e in early_buyers
                if e.get("winrate") and e["winrate"] > 60
                and e.get("total_tx") and e["total_tx"] > 20
            ]
            if smart_money:
                c.print(f"\n  [bold green]★ Сильные трейдеры среди ранних"
                        f" ({len(smart_money)}) — WR>60%:[/bold green]")
                for e in smart_money[:8]:
                    ty = "SWAP" if e.get("tx_type") == "SWAP" else "TRANSFER"
                    c.print(
                        f"  [green]WR {e['winrate']:.0f}%[/green] "
                        f"txns={e.get('total_tx','?')}  [{ty}] "
                        f"[dim]{e['wallet']}[/dim]"
                    )
                    c.print(f"  [dim]  → https://gmgn.ai/sol/address/{e['wallet']}[/dim]")
            c.print(
                "\n  [dim]Watch List пуст: нет SWAP-трейдеров "
                "с WR>50% + PnL>$3K + ≥20tx среди ранних покупателей.[/dim]"
            )

    elif HELIUS_KEY:
        c.print(
            f"  [dim]Нет данных за первые {_buy_window} мин. "
            "Helius + Solscan не вернули трансферы в этом окне. "
            "Токен возможно слишком старый или пул создан отдельно.[/dim]"
        )

    # ── Старый детектор инсайдеров (Solscan fallback, если нет Helius) ───
    if not HELIUS_KEY and SOLSCAN_OK:
        c.print("\n[bold yellow]🕵 Инсайдеры (Solscan fallback):[/bold yellow]")
        c.print("[dim]  Паттерн A: ранняя покупка 0–30 мин | WR>50% | ≥10 сделок[/dim]")
        c.print("[dim]  Паттерн B: получил токены переводом + докупил (★)[/dim]")
        insiders = find_insiders(ca, age, console=c)
        if insiders:
            it = Table(box=box.SIMPLE, padding=(0, 1))
            it.add_column("#",       width=3,  style="dim")
            it.add_column("Пат",     width=4,  justify="center")
            it.add_column("Кошелёк",width=24)
            it.add_column("Вход",   width=9,  justify="center")
            it.add_column("$buy",   width=9,  justify="right")
            it.add_column("WR%",    width=7,  justify="right")
            it.add_column("PnL",    width=11, justify="right")
            for i, ins in enumerate(insiders, 1):
                wr_col  = "green" if ins.get("winrate", 0) > 60 else "yellow"
                pnl_col = "green" if ins.get("pnl", 0) > 0 else "red"
                pat     = ins.get("pattern", "A")
                pat_str = ("[bold green]★B[/bold green]" if pat == "B" else
                           "[yellow]B?[/yellow]" if pat == "B?" else "[dim]A[/dim]")
                it.add_row(
                    str(i), pat_str,
                    f"[dim]{ins['wallet'][:22]}…[/dim]",
                    f"[dim]{ins.get('min_after','?')}[/dim]",
                    f"${ins.get('cost_usd',0):.0f}",
                    f"[{wr_col}]{ins.get('winrate',0):.0f}%[/{wr_col}]" if ins.get("winrate") else "[dim]?[/dim]",
                    f"[{pnl_col}]{fusd(ins.get('pnl',0))}[/{pnl_col}]" if ins.get("pnl") else "[dim]?[/dim]",
                )
            c.print(it)
        else:
            c.print("  [dim]Не найдено (Solscan). Добавь HELIUS_KEY для точных данных.[/dim]")

    # ── Top Holders / Traders Watch List ─────────────────────────────────
    c.print("\n[bold cyan]🌟 WATCH LIST — Топ холдеры / трейдеры:[/bold cyan]")

    th_result     = build_top_holder_watch_list(ca, console=c)
    top_watch     = th_result["watch"]
    th_dev_inside = th_result["dev_insiders"]
    th_big_dist   = th_result["big_dist"]
    th_dist_prof  = th_result["dist_profiles"]
    th_all        = th_result["all_enriched"]
    th_source     = th_result.get("data_source", "holders")

    # Извлекаем мастер-кошелёк из дев-связей (для секции 🔑 ниже)
    _monitor_master_addr  = ""
    _monitor_master_label = ""
    if th_dev_inside:
        _monitor_master_addr  = th_dev_inside[0].get("dev_master") or ""
        _monitor_master_label = th_dev_inside[0].get("dev_label")  or ""

    _src_label = {
        "holders": "текущие держатели (до 200)",
        "traders": "топ трейдеры по PnL (включая уже вышедших)",
        "none":    "нет данных",
    }.get(th_source, th_source)
    c.print(f"[dim]  Источник: {_src_label}[/dim]")

    # Пересечение с ранними покупателями
    early_wallets = {e["wallet"] for e in early_buyers} if early_buyers else set()

    # ── Блок A: качественные трейдеры ────────────────────────────────────
    if top_watch:
        c.print(
            f"\n  [bold green]📋 Качественные трейдеры среди топ холдеров "
            f"({len(top_watch)}):[/bold green]"
        )
        c.print("[dim]  Критерии: WR>50% + PnL>$3K + ≥20 сделок | бандлеры и CEX исключены[/dim]")

        ht = Table(box=box.SIMPLE, padding=(0, 1))
        ht.add_column("#",        width=3,  style="dim")
        ht.add_column("Кошелёк (полный адрес)", width=46)
        ht.add_column("Hold%",    width=7,  justify="right")
        ht.add_column("WR%",      width=7,  justify="right")
        ht.add_column("PnL 30d",  width=12, justify="right")
        ht.add_column("Txns",     width=6,  justify="right")
        ht.add_column("Вход",     width=20, justify="left")

        for idx, e in enumerate(top_watch, 1):
            wr_col  = "green" if e["winrate"] > 60 else "yellow"
            pnl_col = "green" if e["pnl"]     > 0  else "red"

            # Статус входа
            badges = []
            if e["wallet"] in early_wallets:
                badges.append("[bold yellow]★ранний[/bold yellow]")
            if e.get("dev_depth") is not None:
                badges.append(f"[bold red]🕵 ДЕВ lvl{e['dev_depth']}[/bold red]")
            if e.get("transfer_in"):
                badges.append("[yellow]transfer+buy[/yellow]")
            else:
                badges.append("[green]market buy[/green]")
            if e.get("is_new"):
                badges.append("[red](new)[/red]")
            entry_tag = " ".join(badges)

            ht.add_row(
                str(idx),
                f"[bold green]{e['wallet']}[/bold green]",
                f"[dim]{e['hold_pct']:.2f}%[/dim]",
                f"[{wr_col}]{e['winrate']:.0f}%[/{wr_col}]",
                f"[{pnl_col}]{fusd(e['pnl'])}[/{pnl_col}]",
                str(e.get("total_tx", "?")),
                entry_tag,
            )
        c.print(ht)

        cross = [e for e in top_watch if e["wallet"] in early_wallets]
        if cross:
            c.print(
                f"  [bold yellow]⚡ {len(cross)} трейдер(ов) — ранний вход + держат сейчас! "
                f"Двойной сигнал.[/bold yellow]"
            )
        c.print("  [bold]GMGN профили:[/bold]")
        for e in top_watch[:15]:
            c.print(f"  [bold cyan]★[/bold cyan] https://gmgn.ai/sol/address/{e['wallet']}")
    else:
        c.print(
            "\n  [dim]Качественных трейдеров нет — "
            "никто не прошёл WR>50% + PnL>$3K + ≥20tx.[/dim]"
        )

    # ── Блок B: дев-инсайдеры среди холдеров ─────────────────────────────
    if th_dev_inside:
        c.print(
            f"\n  [bold red]🕵 ДЕВ-СВЯЗИ среди холдеров ({len(th_dev_inside)}):[/bold red]"
        )
        c.print("  [dim]Эти холдеры находятся в дереве monitor.py (связаны с отслеживаемым девом)[/dim]")
        dt = Table(box=box.SIMPLE, padding=(0, 1))
        dt.add_column("#",          width=3,  style="dim")
        dt.add_column("Кошелёк",    width=46)
        dt.add_column("Hold%",      width=7,  justify="right")
        dt.add_column("WR%",        width=7,  justify="right")
        dt.add_column("PnL 30d",    width=12, justify="right")
        dt.add_column("Уровень",    width=8,  justify="center")
        dt.add_column("Цепочка к мастеру", width=38)
        for idx2, e in enumerate(th_dev_inside, 1):
            wr_col  = "green" if e.get("winrate", 0) > 60 else "yellow"
            pnl_col = "green" if e.get("pnl",     0) > 0  else "red"
            path    = e.get("dev_path") or []
            chain   = " → ".join(
                (f"{a[:6]}…{a[-4:]}" if len(a) > 12 else a) for a in path
            )
            label   = e.get("dev_label") or e.get("dev_master", "")[:12]
            dt.add_row(
                str(idx2),
                f"[bold red]{e['wallet']}[/bold red]",
                f"[dim]{e['hold_pct']:.2f}%[/dim]",
                f"[{wr_col}]{e.get('winrate', 0):.0f}%[/{wr_col}]",
                f"[{pnl_col}]{fusd(e.get('pnl', 0))}[/{pnl_col}]",
                f"[red]lvl {e['dev_depth']}[/red]",
                f"[dim]{chain}[/dim]",
            )
        c.print(dt)
        c.print(f"  [dim]Мастер: {th_dev_inside[0].get('dev_label') or th_dev_inside[0].get('dev_master', '')}[/dim]")
        for e in th_dev_inside[:5]:
            c.print(f"  [red]⚠[/red] https://gmgn.ai/sol/address/{e['wallet']}")
    else:
        c.print("\n  [dim]🕵 Дев-связей среди топ холдеров не найдено (monitor.db чист)[/dim]")

    # ── Блок C: фишинг / дистрибьюторы ───────────────────────────────────
    if th_big_dist:
        c.print(
            f"\n  [bold yellow]🎣 ФИШИНГ / ДИСТРИБЬЮТОРЫ "
            f"({len(th_big_dist)} кошелька разослали токены 3+ холдерам):[/bold yellow]"
        )
        c.print(
            "  [dim]Эти кошельки не покупали — они переводили токены другим. "
            "Возможные паттерны: бандл, инсайдер-дистрибьютор, фишинг.[/dim]"
        )
        for dist_addr, recipients in sorted(
            th_big_dist.items(), key=lambda x: -len(x[1])
        )[:6]:
            prof = th_dist_prof.get(dist_addr, {})
            wr_d    = prof.get("wr",   0)
            pnl_d   = prof.get("pnl",  0)
            txns_d  = prof.get("txns", 0)
            crt_d   = prof.get("created") or []
            d_depth = prof.get("dev_depth")
            d_label = prof.get("dev_label") or ""
            d_path  = prof.get("dev_path")  or []

            wr_col  = "green" if wr_d  > 60 else ("yellow" if wr_d > 40 else "dim")
            pnl_col = "green" if pnl_d > 0  else "red"

            # Заголовок дистрибьютора
            dev_badge = (
                f"  [bold red]🕵 ДЕВ-СВЯЗЬ lvl{d_depth} мастер={d_label}[/bold red]"
                if d_depth is not None else ""
            )
            c.print(
                f"\n  [bold yellow]📤 {dist_addr}[/bold yellow]"
                f"  [dim]→ разослал токены {len(recipients)} холдерам[/dim]"
                + dev_badge
            )
            if txns_d or wr_d or pnl_d:
                c.print(
                    f"  [dim]  Профиль: WR=[{wr_col}]{wr_d:.0f}%[/{wr_col}]"
                    f"  PnL=[{pnl_col}]{fusd(pnl_d)}[/{pnl_col}]"
                    f"  Txns={txns_d}[/dim]"
                )
            if crt_d:
                tok_list = ", ".join(
                    (t.get("symbol") or t.get("address","?")[:8]) for t in crt_d[:4]
                )
                c.print(
                    f"  [yellow]  Создал {len(crt_d)} токен(ов): {tok_list}"
                    + (" …" if len(crt_d) > 4 else "") + "[/yellow]"
                )
                # Проверяем создал ли он ЭТОТ токен
                _dist_created_this = any(
                    (t.get("address") or t.get("token_address") or t.get("mint") or "") == ca
                    for t in crt_d
                )
                if _dist_created_this and not token_creator:
                    # Нашли деплоера! Он создал этот токен и мы его до сих пор не знали.
                    token_creator = dist_addr
                    c.print(
                        f"  [bold green]  ✓ ЭТО ДЕПЛОЕР ТОКЕНА! Запускаю трассировку мастер-кошелька…[/bold green]"
                    )
                elif _dist_created_this:
                    c.print(
                        "  [bold yellow]  ⚡ Деплоер — добавь в monitor.py для трекинга![/bold yellow]"
                    )
                else:
                    c.print(
                        "  [bold yellow]  ⚡ Деплоер — добавь в monitor.py для трекинга![/bold yellow]"
                    )
            if d_depth is not None:
                chain_str = " → ".join(
                    (f"{a[:6]}…{a[-4:]}" if len(a) > 12 else a) for a in d_path
                )
                c.print(f"  [dim]  Цепочка: {chain_str}[/dim]")

            # Получатели (первые 8)
            recip_short = ", ".join(
                f"{r[:8]}…" for r in recipients[:8]
            )
            if len(recipients) > 8:
                recip_short += f" +ещё {len(recipients)-8}"
            c.print(f"  [dim]  Получатели: {recip_short}[/dim]")
            c.print(f"  [dim]  → https://gmgn.ai/sol/address/{dist_addr}[/dim]")
            c.print(f"  [dim]  → https://solscan.io/account/{dist_addr}[/dim]")
    else:
        c.print("\n  [dim]🎣 Значимых дистрибьюторов не найдено (никто не разослал токены 3+ адресам)[/dim]")

    # ── Ссылки ────────────────────────────────────────────────────────────
    c.print(f"\n🔗 GMGN:        https://gmgn.ai/sol/token/{ca}", style="dim")
    c.print(f"🔗 Solscan:     https://solscan.io/token/{ca}", style="dim")
    c.print(f"🔗 Rugcheck:    https://rugcheck.xyz/tokens/{ca}", style="dim")
    c.print(f"🔗 DexScreener: https://dexscreener.com/solana/{ca}", style="dim")

    # Первый получатель токенов при запуске (для trace-dev фолбэка)
    # Это кошелёк с tx_type=TRANSFER и самым ранним ts — как правило bundle bot / deployer
    first_receiver = None
    if early_buyers:
        transfers_only = [e for e in early_buyers if e.get("tx_type") != "SWAP"]
        if transfers_only:
            first_receiver = min(transfers_only, key=lambda x: x.get("ts", 999999999))["wallet"]

    # ── 🔑 АВТОПОИСК МАСТЕР-КОШЕЛЬКА (через Helius, всегда) ──────────────
    # Запускается автоматически при каждом analyze.py <CA>
    # Если мастер уже в monitor.db (_creator_mon) — пропускаем глубокую трассировку
    _auto_master_found = None

    # Если creator так и не нашли — честно говорим об этом
    if not token_creator and not _monitor_master_addr and HELIUS_KEY:
        c.print("\n[bold magenta]🔑 ПОИСК МАСТЕР-КОШЕЛЬКА[/bold magenta]")
        c.print(
            "  [yellow]⚠ Деплоер токена не определён автоматически.[/yellow]\n"
            "  [dim]GMGN security не вернул creator, Helius DAS и mintAuthority тоже пусты.[/dim]\n"
            "  [dim]Найди адрес деплоера на Solscan:[/dim]\n"
            f"  [dim]  → https://solscan.io/token/{ca} → вкладка «Creators»[/dim]\n"
            "  [dim]Затем запусти вручную:[/dim]\n"
            f"  [bold cyan]  python3 analyze.py {ca} --trace-dev --wallet <DEPLOYER_ADDR>[/bold cyan]"
        )

    # Дев-связи нашли мастер в monitor.db, но деплоер неизвестен напрямую
    if not token_creator and _monitor_master_addr:
        c.print("\n[bold magenta]🔑 МАСТЕР-КОШЕЛЁК[/bold magenta]")
        c.print(Panel(
            f"[bold yellow]★ МАСТЕР ПОДТВЕРЖДЁН ЧЕРЕЗ ДЕВ-СВЯЗИ[/bold yellow]\n"
            f"[bold]{_monitor_master_addr}[/bold]\n"
            f"[yellow]{_monitor_master_label}[/yellow]\n\n"
            f"[dim]Деплоер токена не определён напрямую, но среди холдеров/покупателей\n"
            f"обнаружены кошельки, связанные с этим мастером (monitor.db).\n"
            f"Это сильный сигнал — кошелёк уже отслеживается системой.[/dim]\n\n"
            f"[dim]→ https://gmgn.ai/sol/address/{_monitor_master_addr}[/dim]\n"
            f"[dim]→ https://solscan.io/account/{_monitor_master_addr}[/dim]\n\n"
            f"[dim]Для полного трейса деплоера найди его на Solscan и запусти:[/dim]\n"
            f"[bold cyan]  python3 analyze.py {ca} --trace-dev --wallet <DEPLOYER_ADDR>[/bold cyan]",
            border_style="yellow",
            title="[bold yellow]🔑 МАСТЕР-КОШЕЛЁК ПОДТВЕРЖДЁН (ДЕВ-СВЯЗИ)[/bold yellow]",
        ))
        _auto_master_found = _monitor_master_addr

    if token_creator and age and HELIUS_KEY and not _creator_mon:
        c.print("\n[bold magenta]🔑 ПОИСК МАСТЕР-КОШЕЛЬКА[/bold magenta]")
        c.print("  [dim]Трассировка: деплоер → прослойки → мастер (Helius on-chain)[/dim]")

        try:
            _chain = helius_trace_funding_deep(token_creator, age, max_depth=4)
        except Exception as _e:
            _chain = []
            if DEBUG:
                c.print(f"  [dim]DEBUG trace exception: {_e}[/dim]")

        if not _chain:
            c.print("  [dim]Входящих SOL не найдено в цепочке финансирования.[/dim]")
            c.print(f"  [dim]→ Проверь вручную: https://solscan.io/account/{token_creator}[/dim]")
            c.print("  [dim]→ Или запусти: python3 analyze.py <CA> --trace-dev[/dim]")
        else:
            _masters = [n for n in _chain if not n.get("is_proxy") and not n.get("known")]
            _proxies = [n for n in _chain if n.get("is_proxy")]
            _cex     = [n for n in _chain if n.get("known")]

            if _cex and not _masters:
                c.print(
                    f"  [yellow]★ Источник через CEX/биржу: "
                    f"{_cex[0]['known']}[/yellow]"
                )
                c.print(
                    f"  [dim]  {_cex[0]['funder'][:34]}…  "
                    f"{_cex[0].get('amount_sol', 0):.3f} SOL[/dim]"
                )
                c.print(
                    "  [dim]Отследить до физического кошелька невозможно "
                    "(средства вышли через биржу).[/dim]"
                )
            elif not _masters and _proxies:
                # Нашли прослойки — верхняя из них де-факто мастер
                # (трассировка выше неё невозможна: биржевой вывод или история обрезана)
                _top = max(_proxies, key=lambda x: x.get("depth", 0))
                _taddr  = _top["funder"]
                _tsol   = _top.get("amount_sol", 0)
                _tdepth = _top.get("depth", 0)
                _twr    = _top.get("wr", 0)
                _tpnl   = _top.get("pnl", 0)
                _ttx    = _top.get("tx_count", 0)
                _tgtx   = _top.get("gmgn_txns", 0)
                _tcr    = _top.get("f_created") or []

                # Символ токена для метки кластера
                _tok_sym = (info.get("symbol") or "?")

                # Сохраняем мастера и ВЕСЬ кластер в monitor.db
                _cluster_label = f"{_tok_sym}:{ca[:8]}"
                _auto_label    = f"auto-top:{_tok_sym}"
                _is_new = save_master_to_monitor(
                    _taddr, label=_auto_label, source_ca=ca
                )
                save_cluster_to_monitor(
                    master_addr=_taddr, cluster_label=_cluster_label,
                    source_ca=ca, deployer=token_creator, chain_nodes=_chain
                )

                # Трек-рекорд кластера из monitor.db
                _track = get_cluster_track_record(_taddr)
                _track_str = ""
                if _track:
                    _other = [t for t in _track if t["token_ca"] != ca]
                    if _other:
                        _track_str = (
                            "\n[dim]Предыдущие токены кластера: "
                            + ", ".join(
                                f"[cyan]{t['token_symbol'] or t['token_ca'][:8]}[/cyan]"
                                for t in _other[:6]
                            )
                            + ("[/dim]")
                        )

                _wr_col  = "green" if _twr > 50 else "yellow"
                _pnl_col = "green" if _tpnl > 0 else "red"

                # Полная цепочка с полными адресами
                _chain_sorted = sorted(_proxies, key=lambda x: x.get("depth", 0))
                _chain_lines  = []
                _chain_lines.append(f"  деплоер  → {token_creator}")
                for _cp in _chain_sorted:
                    _role = "прослойка" if _cp["funder"] != _taddr else "мастер-кандидат"
                    _chain_lines.append(
                        f"  {_role} → {_cp['funder']}  "
                        f"[dim]({_cp.get('amount_sol',0):.3f} SOL)[/dim]"
                    )
                _chain_block = "\n".join(_chain_lines)

                _saved_str = (
                    "[bold green] ✓ добавлен в monitor.db[/bold green]"
                    if _is_new else "[dim] (уже в monitor.db)[/dim]"
                )

                _panel_body = (
                    f"[bold yellow]★ ВЕРХНИЙ ТРАССИРУЕМЫЙ КОШЕЛЁК[/bold yellow]{_saved_str}\n"
                    f"[bold]{_taddr}[/bold]\n"
                    f"[dim]Глубина: {_tdepth + 1} уровень(ей) от деплоера  |  "
                    f"Отправил: {_tsol:.3f} SOL[/dim]\n"
                    f"WR: [{_wr_col}]{_twr:.1f}%[/{_wr_col}]   "
                    f"PnL: [{_pnl_col}]{fusd(_tpnl)}[/{_pnl_col}]   "
                    f"On-chain tx: {_ttx}   GMGN сделок: {_tgtx}\n\n"
                    f"[dim]── Цепочка финансирования ──[/dim]\n"
                    f"{_chain_block}\n\n"
                    f"[dim]Выше — биржевой вывод или история обрезана[/dim]\n"
                    f"[dim]→ https://gmgn.ai/sol/address/{_taddr}[/dim]\n"
                    f"[dim]→ https://solscan.io/account/{_taddr}[/dim]"
                )
                if _tcr:
                    _panel_body += (
                        f"\n[yellow]Создал {len(_tcr)} токен(ов): "
                        + ", ".join(
                            (t.get("symbol") or t.get("address", "?")[:8])
                            for t in _tcr[:6]
                        )
                        + "[/yellow]"
                    )
                if _track_str:
                    _panel_body += _track_str
                c.print(Panel(
                    _panel_body,
                    border_style="yellow",
                    title=f"[bold yellow]🔑 МАСТЕР-КАНДИДАТ · {_cluster_label}[/bold yellow]",
                ))

                if _is_new:
                    c.print(
                        f"  [bold yellow]⚡ Добавлен в monitor.db — кластер из "
                        f"{len(_chain)+1} кошельков отслеживается![/bold yellow]"
                    )
                    c.print(
                        f"  [dim]Запусти мониторинг: python3 monitor.py --watch[/dim]"
                    )
                _auto_master_found = _taddr

            elif _masters:
                # Мастер найден! Показываем компактно и сохраняем
                for _midx, _m in enumerate(_masters[:3], 1):
                    _maddr  = _m["funder"]
                    _msol   = _m.get("amount_sol", 0)
                    _mdepth = _m.get("depth", 0)
                    _mwr    = _m.get("wr", 0)
                    _mpnl   = _m.get("pnl", 0)
                    _mtx    = _m.get("tx_count", 0)
                    _mgtx   = _m.get("gmgn_txns", 0)
                    _mcr    = _m.get("f_created") or []

                    _tok_sym = (info.get("symbol") or "?")
                    _cluster_label = f"{_tok_sym}:{ca[:8]}"
                    _auto_label = f"auto:{_tok_sym}"

                    # Сохраняем мастера и весь кластер
                    _is_new = save_master_to_monitor(
                        _maddr, label=_auto_label, source_ca=ca
                    )
                    save_cluster_to_monitor(
                        master_addr=_maddr, cluster_label=_cluster_label,
                        source_ca=ca, deployer=token_creator, chain_nodes=_chain
                    )

                    # Трек-рекорд кластера
                    _track = get_cluster_track_record(_maddr)
                    _track_str = ""
                    if _track:
                        _other = [t for t in _track if t["token_ca"] != ca]
                        if _other:
                            _track_str = (
                                "\n[dim]Предыдущие токены кластера: "
                                + ", ".join(
                                    f"[cyan]{t['token_symbol'] or t['token_ca'][:8]}[/cyan]"
                                    for t in _other[:6]
                                )
                                + "[/dim]"
                            )

                    _wr_col  = "green" if _mwr > 50 else "yellow"
                    _pnl_col = "green" if _mpnl > 0 else "red"

                    # Полная цепочка с полными адресами
                    _chain_sorted_m = sorted(
                        [n for n in _proxies if n.get("depth", 99) < _mdepth],
                        key=lambda x: x.get("depth", 0)
                    )
                    _chain_lines_m = []
                    _chain_lines_m.append(f"  деплоер  → {token_creator}")
                    for _cp in _chain_sorted_m:
                        _chain_lines_m.append(
                            f"  прослойка → {_cp['funder']}  "
                            f"[dim]({_cp.get('amount_sol',0):.3f} SOL)[/dim]"
                        )
                    _chain_lines_m.append(
                        f"  [bold green]мастер    → {_maddr}[/bold green]  "
                        f"[dim]({_msol:.3f} SOL)[/dim]"
                    )
                    _chain_block_m = "\n".join(_chain_lines_m)

                    _saved_str = (
                        "[bold green] ✓ добавлен в monitor.db[/bold green]"
                        if _is_new else
                        "[dim] (уже в monitor.db)[/dim]"
                    )

                    _panel_body_m = (
                        f"[bold green]★ МАСТЕР #{_midx}[/bold green]{_saved_str}\n"
                        f"[bold]{_maddr}[/bold]\n"
                        f"[dim]Глубина: {_mdepth+1} уровень(ей) от деплоера  |  "
                        f"Отправил: {_msol:.3f} SOL[/dim]\n"
                        f"WR: [{_wr_col}]{_mwr:.1f}%[/{_wr_col}]   "
                        f"PnL: [{_pnl_col}]{fusd(_mpnl)}[/{_pnl_col}]   "
                        f"On-chain tx: {_mtx}   GMGN сделок: {_mgtx}\n\n"
                        f"[dim]── Цепочка финансирования ──[/dim]\n"
                        f"{_chain_block_m}\n\n"
                        f"[dim]→ https://gmgn.ai/sol/address/{_maddr}[/dim]\n"
                        f"[dim]→ https://solscan.io/account/{_maddr}[/dim]"
                    )
                    if _mcr:
                        _panel_body_m += (
                            f"\n[yellow]Создал {len(_mcr)} токен(ов): "
                            + ", ".join(
                                (t.get("symbol") or t.get("address","?")[:8])
                                for t in _mcr[:6]
                            )
                            + "[/yellow]"
                        )
                    if _track_str:
                        _panel_body_m += _track_str
                    c.print(Panel(
                        _panel_body_m,
                        border_style="green",
                        title=f"[bold green]🔑 МАСТЕР-КОШЕЛЁК · {_cluster_label}[/bold green]",
                    ))

                    if _is_new:
                        c.print(
                            f"  [bold green]⚡ Мастер добавлен в monitor.db — кластер из "
                            f"{len(_chain)+1} кошельков отслеживается![/bold green]"
                        )
                        c.print(
                            f"  [dim]Запусти мониторинг: python3 monitor.py --watch[/dim]"
                        )
                    _auto_master_found = _maddr

    elif token_creator and _creator_mon:
        # Мастер уже был в monitor.db — сообщаем кратко
        _cm_label = _creator_mon.get("master_label") or _creator_mon.get("master", "")[:20]
        _cm_addr  = _creator_mon.get("master", "")
        _dev_confirm = (
            "  [bold yellow]★ Дев-связи холдеров тоже указывают на этот мастер![/bold yellow]\n"
            if _monitor_master_addr and _monitor_master_addr == _cm_addr else ""
        )
        c.print(
            f"\n  [dim]🔑 Мастер из monitor.db: "
            f"[bold]{_cm_label}[/bold] [{_cm_addr[:8]}…{_cm_addr[-4:]}][/dim]"
        )
        if _dev_confirm:
            c.print(_dev_confirm)

    return {"ca": ca, "info": info, "sec": sec, "pool": pool,
            "first_receiver": first_receiver, "age": age,
            "master_wallet": _auto_master_found}

# ── Wallet trace ──────────────────────────────────────────────────────────────
def trace_wallet(wallet, ca=None):
    get_server_time()
    c = Console() if RICH else None
    if RICH:
        c.print(Panel(f"[bold]{wallet}[/bold]", title="🕵 Wallet Trace", border_style="yellow"))

    print("  Загружаю GMGN…")
    with ThreadPoolExecutor(max_workers=1) as ex:
        fa = ex.submit(g_activity, wallet, 80)
        fh = ex.submit(g_holdings, wallet)
        fs = ex.submit(g_stats,    wallet)
        fc = ex.submit(g_created,  wallet)

    activity = fa.result(); holdings = fh.result()
    stats    = fs.result(); created  = fc.result()

    if RICH:
        if stats:
            t = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
            t.add_column("", style="dim", width=20)
            t.add_column("")
            wr  = (stats.get("pnl_stat") or {}).get("winrate") or stats.get("winrate") or stats.get("win_rate")
            pnl = _scalar(stats.get("realized_profit") or stats.get("total_profit_usd") or 0)
            if wr is not None:
                try:
                    wrv = float(wr) * (100 if float(wr) <= 1 else 1)
                    col = "green" if wrv > 50 else "yellow"
                    t.add_row("🎯 Winrate", f"[{col}]{wrv:.1f}%[/{col}]")
                except: pass
            if pnl:
                col = "green" if pnl > 0 else "red"
                t.add_row("💰 PnL 30d", f"[{col}]{fusd(pnl)}[/{col}]")
            if t.row_count > 0:
                c.print("\n[bold]Статистика (30d):[/bold]")
                c.print(t)

        if holdings:
            c.print("\n[bold]Текущие холдинги:[/bold]")
            ht = Table(box=box.SIMPLE, padding=(0, 1))
            ht.add_column("Токен", width=12)
            ht.add_column("USD", justify="right")
            ht.add_column("PnL", justify="right")
            for h in holdings[:8]:
                sym = h.get("symbol") or h.get("token_symbol") or "?"
                val = _scalar(h.get("usd_value")         or h.get("value_usd") or 0)
                pnl = _scalar(h.get("unrealized_profit") or h.get("pnl") or 0)
                col = "green" if pnl > 0 else "red"
                ht.add_row(sym, fusd(val), f"[{col}]{fusd(pnl)}[/{col}]")
            c.print(ht)

        if created:
            c.print(f"\n[bold]Токены созданные кошельком ({len(created)}):[/bold]")
            ct = Table(box=box.SIMPLE, padding=(0, 1))
            ct.add_column("Символ", width=12)
            ct.add_column("CA", width=24)
            ct.add_column("МК", justify="right")
            ct.add_column("Создан", width=14)
            for tok in created[:10]:
                sym  = tok.get("symbol") or "?"
                addr = tok.get("address") or tok.get("token_address") or "?"
                mc   = _scalar(tok.get("market_cap") or tok.get("mc") or 0)
                ts   = tok.get("created_at") or tok.get("open_timestamp") or 0
                dt   = datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%m-%d %H:%M") if ts else "?"
                ct.add_row(sym, f"[dim]{str(addr)[:22]}…[/dim]", fusd(mc), f"[dim]{dt}[/dim]")
            c.print(ct)

        if activity:
            c.print(f"\n[bold]Последние сделки (GMGN):[/bold]")
            at = Table(box=box.SIMPLE, padding=(0, 1))
            at.add_column("Действие", width=8)
            at.add_column("Токен", width=14)
            at.add_column("USD", justify="right", width=12)
            at.add_column("SOL", justify="right", width=8)
            at.add_column("Время", width=14)
            for act in activity[:20]:
                action = act.get("event_type") or act.get("action") or act.get("side") or "?"
                sym    = act.get("token_symbol") or act.get("symbol") or "?"
                cost   = _scalar(act.get("cost_usd")   or act.get("amount_usd") or 0)
                sol_a  = _scalar(act.get("cost")        or act.get("sol_amount") or 0)
                ts     = act.get("timestamp") or act.get("block_time") or 0
                dt     = datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%m-%d %H:%M") if ts else "?"
                col    = "green" if "buy"  in str(action).lower() else (
                         "red"   if "sell" in str(action).lower() else "white")
                at.add_row(f"[{col}]{str(action).upper()}[/{col}]", sym,
                           fusd(cost), f"{sol_a:.2f}" if sol_a else "?",
                           f"[dim]{dt}[/dim]")
            c.print(at)

        # Solscan
        if SOLSCAN_OK:
            c.print("\n[bold yellow]🔎 Solscan: движение средств[/bold yellow]")
            print("  Загружаю Solscan…")
            top = solscan_top(wallet, 30)
            raw = solscan_transfers(wallet, 5)

            if top:
                c.print("\n[bold]Топ контрагенты (30d):[/bold]")
                tt = Table(box=box.SIMPLE, padding=(0, 1))
                tt.add_column("",       width=6)
                tt.add_column("Адрес",  width=26)
                tt.add_column("SOL",    justify="right", width=10)
                tt.add_column("CEX?",   width=16)
                for e in top[:10]:
                    addr = e.get("address", "?")
                    ai   = float(e.get("amount_in",  0) or 0) / 1e9
                    ao   = float(e.get("amount_out", 0) or 0) / 1e9
                    net  = ai - ao
                    ds   = "← IN" if net > 0 else "→ OUT"
                    col  = "green" if net > 0 else "red"
                    kn   = KNOWN_WALLETS.get(addr, "")
                    tt.add_row(
                        f"[{col}]{ds}[/{col}]",
                        f"[dim]{addr[:24]}…[/dim]",
                        f"[{col}]{abs(net):.2f}[/{col}]",
                        f"[yellow]★ {kn}[/yellow]" if kn else "[dim]—[/dim]",
                    )
                c.print(tt)

            if raw:
                c.print(f"\n[bold]Детальный лог ({min(25, len(raw))} транзакций):[/bold]")
                rt = Table(box=box.SIMPLE, padding=(0, 1))
                rt.add_column("",       width=3)
                rt.add_column("Адрес",  width=26)
                rt.add_column("Сумма",  justify="right", width=16)
                rt.add_column("Токен",  width=10)
                rt.add_column("Время",  width=14)
                rt.add_column("CEX?",   width=14)
                for tx in raw[:25]:
                    fa2  = str(tx.get("from_address") or tx.get("sender")   or "?")
                    ta2  = str(tx.get("to_address")   or tx.get("receiver") or "?")
                    amt  = tx.get("amount", 0)
                    dec  = int(tx.get("token_decimals", 9) or 9)
                    sym  = tx.get("token_symbol") or "SOL"
                    ts   = tx.get("block_time", 0) or 0
                    dt   = datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%m-%d %H:%M") if ts else "?"
                    try: val = float(amt) / (10 ** dec)
                    except: val = 0
                    is_out = fa2.lower() == wallet.lower()
                    arrow  = "→" if is_out else "←"
                    col    = "red" if is_out else "green"
                    peer   = ta2 if is_out else fa2
                    kn     = KNOWN_WALLETS.get(peer, "")
                    rt.add_row(
                        f"[{col}]{arrow}[/{col}]",
                        f"[dim]{peer[:24]}…[/dim]",
                        f"[{col}]{val:.4f}[/{col}]",
                        f"[dim]{sym}[/dim]",
                        f"[dim]{dt}[/dim]",
                        f"[yellow]{kn}[/yellow]" if kn else "[dim]—[/dim]",
                    )
                c.print(rt)

        c.print(f"\n🔗 https://gmgn.ai/sol/address/{wallet}", style="dim")
        c.print(f"🔗 https://solscan.io/account/{wallet}\n", style="dim")
    else:
        print(f"Activity: {len(activity)} | Holdings: {len(holdings)} | Created: {len(created)}")

# ── Dev trace (--trace-dev) ───────────────────────────────────────────────────
def helius_check_sol_transfers(wallet_a, wallet_b, max_sigs=500):
    """
    Двусторонняя проверка: были ли SOL переводы между wallet_a и wallet_b.
    Сканируем транзакции wallet_a — ищем любое движение SOL туда-обратно.
    Возвращает список {dir, frm, to, amount_sol, ts, sig}.
    """
    if not HELIUS_KEY:
        return []

    params = [wallet_a, {"limit": max_sigs, "commitment": "finalized"}]
    result = _helius_rpc("getSignaturesForAddress", params)
    if not isinstance(result, list) or not result:
        return []
    sigs = [r["signature"] for r in result if not r.get("err")]
    if not sigs:
        return []

    found = []
    for i in range(0, len(sigs), 100):
        batch = sigs[i:i + 100]
        try:
            r = requests.post(
                f"https://api.helius.xyz/v0/transactions/?api-key={HELIUS_KEY}",
                json={"transactions": batch},
                timeout=30,
            )
            if r.status_code != 200:
                continue
            txs = r.json()
            if not isinstance(txs, list):
                continue
        except Exception:
            continue

        for tx in txs:
            ts  = int(tx.get("timestamp") or 0)
            sig = tx.get("signature") or ""
            for nt in (tx.get("nativeTransfers") or []):
                frm = nt.get("fromUserAccount") or ""
                to  = nt.get("toUserAccount")   or ""
                try:
                    amt = float(nt.get("amount") or 0) / 1e9
                except Exception:
                    amt = 0.0
                if amt < 0.001:
                    continue
                # A → B
                if frm == wallet_a and to == wallet_b:
                    found.append({"dir": "A→B", "frm": frm, "to": to,
                                  "amount_sol": amt, "ts": ts, "sig": sig})
                # B → A
                elif frm == wallet_b and to == wallet_a:
                    found.append({"dir": "B→A", "frm": frm, "to": to,
                                  "amount_sol": amt, "ts": ts, "sig": sig})

    return sorted(found, key=lambda x: x["ts"])


def cross_project_funder_analysis(deployer, tokens, max_tokens=6):
    """
    Кросс-проектный анализ: для нескольких запусков одного деплоера
    проверяем кто финансировал его перед каждым.
    Паттерн повторяющихся адресов = подтверждённый мастер-кошелёк.
    Возвращает (rows, funder_counts) где funder_counts = {addr: кол-во проектов}.
    """
    if not HELIUS_KEY:
        return [], {}

    # Берём последние max_tokens токенов по дате создания (самые свежие)
    dated = [t for t in tokens if t.get("create_timestamp")]
    dated.sort(key=lambda t: t.get("create_timestamp", 0), reverse=True)
    sample = dated[:max_tokens]

    rows          = []   # {token, ca, launch_ts, funder, amount_sol, ts}
    funder_counts = defaultdict(int)

    for tok in sample:
        sym      = tok.get("symbol") or "?"
        ca_tok   = tok.get("token_address") or tok.get("address") or "?"
        launch   = int(tok.get("create_timestamp") or 0)
        if not launch:
            continue

        funders = helius_funding_chain(deployer, launch, window_hours=72)
        if not funders:
            funders = helius_funding_chain(deployer, launch, window_hours=168)
        if not funders:
            # расширяем до 14 дней — деплоер мог быть пополнен заранее
            funders = helius_funding_chain(deployer, launch, window_hours=336)

        for f in funders:
            funder_counts[f["funder"]] += 1
            # часов между финансированием и запуском
            hours_before = round((launch - f["ts"]) / 3600, 1) if f["ts"] else None
            rows.append({
                "token":        sym,
                "ca":           ca_tok,
                "launch_ts":    launch,
                "funder":       f["funder"],
                "amount_sol":   f["amount_sol"],
                "ts":           f["ts"],
                "hours_before": hours_before,
            })

        if not funders:
            rows.append({
                "token":        sym,
                "ca":           ca_tok,
                "launch_ts":    launch,
                "funder":       "—",
                "amount_sol":   0,
                "ts":           0,
                "hours_before": None,
            })

    return rows, dict(funder_counts)


def trace_dev(ca, launch_ts, sec, deployer_override=None, first_receiver=None):
    """
    Команда --trace-dev: строим граф финансирования от деплоера → мастер-кошелёк.

    Поиск деплоера (в порядке приоритета):
      1. deployer_override  (флаг --wallet при запуске с --trace-dev)
      2. GMGN security (creator / dev_address / owner)
      3. Helius DAS getAsset (authorities[0])
      4. first_receiver  (первый кошелёк получивший токены при запуске, из анализа)
    """
    if not RICH:
        print("Нужна библиотека rich: pip install rich")
        return

    c = Console()

    # ── Ищем деплоера всеми доступными способами ──────────────────────────────
    creator = (
        deployer_override
        or sec.get("creator") or sec.get("dev_address")
        or sec.get("owner")   or sec.get("creator_address")
        or ""
    )

    if not creator and HELIUS_KEY:
        c.print("  [dim]GMGN не вернул creator → пробую Helius DAS getAsset…[/dim]")
        creator = helius_get_creator(ca) or ""
        if creator:
            c.print(f"  [dim]DAS: creator = {creator[:20]}…[/dim]")

    if not creator and first_receiver:
        c.print(
            "  [dim]DAS тоже пуст → использую первого получателя токенов "
            "из раннего анализа[/dim]"
        )
        creator = first_receiver

    c.print(Panel(
        f"[bold]Трассировка цепочки финансирования[/bold]\n"
        f"[dim]Цель: деплоер → мастер-кошелёк → следующие запуски[/dim]\n"
        f"[dim]Токен: {ca}[/dim]",
        title="🕵  --trace-dev",
        border_style="magenta",
    ))

    if not creator:
        c.print("\n[red]❌ Деплоер не найден в данных GMGN security.[/red]")
        c.print(
            "[dim]Найди адрес деплоера вручную на Solscan "
            f"(https://solscan.io/token/{ca} → Creator) и запусти:[/dim]"
        )
        c.print(f"[dim]  python3 analyze.py {ca} --wallet <DEPLOYER_ADDR>[/dim]")
        return

    # ── Шаг 1: Деплоер ───────────────────────────────────────────────────
    c.print(f"\n[bold magenta]▶ ШАГ 1 — Деплоер токена[/bold magenta]")
    c.print(f"  [bold]{creator}[/bold]")
    c.print(f"  [dim]https://gmgn.ai/sol/address/{creator}[/dim]")
    c.print(f"  [dim]https://solscan.io/account/{creator}[/dim]")

    with ThreadPoolExecutor(max_workers=2) as ex:
        f_dep_stats   = ex.submit(g_stats,   creator)
        f_dep_created = ex.submit(g_created, creator)

    dep_stats   = f_dep_stats.result()   or {}
    dep_created = f_dep_created.result() or []

    dep_wr  = (dep_stats.get("pnl_stat") or {}).get("winrate") or dep_stats.get("winrate") or dep_stats.get("win_rate")
    dep_pnl = _scalar(dep_stats.get("realized_profit") or
                      dep_stats.get("total_profit_usd") or 0)
    dep_buys  = int(dep_stats.get("buy_count")  or dep_stats.get("buy")  or 0)
    dep_sells = int(dep_stats.get("sell_count") or dep_stats.get("sell") or 0)
    dep_txns  = dep_buys + dep_sells
    try:
        dep_wr_f = float(dep_wr) * (100 if float(dep_wr) <= 1 else 1) if dep_wr else 0
    except Exception:
        dep_wr_f = 0

    dt = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
    dt.add_column("", style="dim", width=20)
    dt.add_column("")
    if dep_wr_f:
        col = "green" if dep_wr_f > 50 else "yellow"
        dt.add_row("WR (30d)",      f"[{col}]{dep_wr_f:.1f}%[/{col}]")
    if dep_pnl:
        col = "green" if dep_pnl > 0 else "red"
        dt.add_row("PnL (30d)",     f"[{col}]{fusd(dep_pnl)}[/{col}]")
    dt.add_row("Сделок (30d)",  str(dep_txns) if dep_txns else "[dim]нет данных[/dim]")
    dt.add_row("Создал токенов", str(len(dep_created)))
    c.print(dt)

    is_serial   = len(dep_created) >= 3           # деплоил 3+ токенов = серийный
    is_human    = dep_txns >= 20 and dep_wr_f > 30  # торгует + WR > 30%

    if is_serial and not is_human:
        # Серийный деплоер — сам не торгует активно, но запускает много токенов
        # Значит кто-то финансирует его для запусков → идём глубже
        top_tokens = sorted(dep_created,
                            key=lambda t: _scalar(t.get("market_cap") or t.get("mc") or 0),
                            reverse=True)
        best_mc = _scalar(top_tokens[0].get("market_cap") or top_tokens[0].get("mc") or 0) if top_tokens else 0
        c.print(
            f"  [bold yellow]⚡ Серийный деплоер! {len(dep_created)} токенов, "
            f"лучший MC: {fusd(best_mc)}[/bold yellow]"
        )
        c.print(
            "  [dim]→ Этот кошелёк сам деплоит токены, но его финансирует мастер. "
            "Ищем мастера…[/dim]"
        )
    elif is_human:
        c.print("  [green]✓ Деплоер активно торгует — возможно и есть мастер-кошелёк[/green]")
    else:
        c.print("  [dim]→ Деплоер похож на одноразовый кошелёк (типично для схем)[/dim]")

    if dep_created:
        # Сортируем по MC убыванию — интересные токены первыми
        sorted_created = sorted(
            dep_created,
            key=lambda t: _scalar(t.get("market_cap") or t.get("mc") or 0),
            reverse=True,
        )
        c.print(f"\n  [dim]Все токены деплоера ({len(dep_created)}), по MC:[/dim]")
        for tok in sorted_created[:10]:
            sym  = tok.get("symbol") or "?"
            addr = tok.get("address") or tok.get("token_address") or "?"
            mc   = _scalar(tok.get("market_cap") or tok.get("mc") or 0)
            mc_col = "green" if mc > 100_000 else ("yellow" if mc > 10_000 else "dim")
            c.print(f"    [{mc_col}]• {sym:<10} {fusd(mc):<10}[/{mc_col}]  [dim]{addr}[/dim]")
        if len(dep_created) > 10:
            c.print(f"    [dim]… и ещё {len(dep_created)-10} токенов[/dim]")

    # ── Шаги 2-3: Рекурсивная трассировка цепочки финансирования ────────
    c.print(f"\n[bold magenta]▶ ШАГ 2-3 — Трассировка цепочки (глубина до 3 уровней)[/bold magenta]")
    c.print(
        "  [dim]Алгоритм: кто финансировал деплоера → если это прослойка (мало tx) "
        "→ идём на уровень выше → до мастера[/dim]"
    )

    if not HELIUS_KEY:
        c.print("  [red]Нужен HELIUS_KEY в .env — пропускаю шаги 2-3.[/red]")
        c.print("  [dim]→ helius.dev → Sign Up (бесплатно) → Dashboard → API Keys[/dim]")
        return

    if not launch_ts:
        c.print("  [red]Время запуска токена неизвестно — пропускаю.[/red]")
        return

    c.print(
        "  [dim]Запрашиваю Helius… "
        "(деплоер: окно 72ч; прослойки: окно 30 дней)[/dim]"
    )
    chain_nodes = helius_trace_funding_deep(creator, launch_ts, max_depth=4)

    if not chain_nodes:
        c.print("  [dim]Входящих SOL не найдено в 48ч до запуска ни на одном уровне.[/dim]")
        c.print("  [dim]Возможно деплоер пополнили раньше или цепочка > 3 уровней.[/dim]")
        c.print(f"  [dim]Проверь вручную: https://solscan.io/account/{creator}[/dim]")
    else:
        # ── Визуальное дерево цепочки ─────────────────────────────────────
        c.print(f"\n  [bold]Граф финансирования (найдено {len(chain_nodes)} кошельков):[/bold]")

        # Строки-префиксы для каждой глубины
        DEPTH_PREFIX = ["  ", "    └─ ", "        └─ ", "            └─ "]
        DEPTH_COLOR  = ["white", "cyan", "yellow", "green"]

        for node in chain_nodes:
            depth    = node.get("depth", 0)
            wallet   = node.get("funder", "?")
            sol      = node.get("amount_sol", 0)
            tx_cnt   = node.get("tx_count",   0)
            is_proxy = node.get("is_proxy",   False)
            known    = node.get("known",      "")
            target   = node.get("funded_target", "")
            ts_str   = (datetime.fromtimestamp(node["ts"], tz=timezone.utc).strftime("%m-%d %H:%M")
                        if node.get("ts") else "?")

            prefix = DEPTH_PREFIX[min(depth, 3)]
            col    = DEPTH_COLOR[min(depth, 3)]

            gmgn_tx = node.get("gmgn_txns", 0)
            n_cr    = len(node.get("f_created") or [])
            if known:
                label = f"[yellow]★ CEX: {known}[/yellow]"
            elif is_proxy:
                reason = (f"tx={tx_cnt}, торгов={gmgn_tx}, токенов={n_cr}"
                          f"{'  ← мало tx' if tx_cnt < 15 else '  ← нет активности'}")
                label = f"[red]ПРОСЛОЙКА 🔄  {reason}[/red]"
            else:
                label = (f"[bold green]МАСТЕР ✓  "
                         f"tx={tx_cnt}, торгов={gmgn_tx}, токенов={n_cr}[/bold green]")

            c.print(
                f"{prefix}[{col}]Глубина {depth+1}[/{col}]  "
                f"[bold]{wallet}[/bold]"
            )
            c.print(
                f"{prefix}  [dim]↳ {sol:.3f} SOL → "
                f"{target[:20]}…  {ts_str}  {label}[/dim]"
            )

        # ── Итоговый список мастер-кошельков ──────────────────────────────
        masters = [n for n in chain_nodes if not n.get("is_proxy") and not n.get("known")]
        known_w = [n for n in chain_nodes if n.get("known")]
        proxies = [n for n in chain_nodes if n.get("is_proxy")]

        if proxies:
            c.print(
                f"\n  [dim]Обнаружено {len(proxies)} прослоек — "
                f"трассировка прошла сквозь них автоматически.[/dim]"
            )

        if known_w:
            c.print(f"\n  [dim]Источники через CEX/биржи:[/dim]")
            for n in known_w:
                c.print(
                    f"  [yellow]★ {n['known']}[/yellow]  "
                    f"[dim]{n['funder'][:34]}…  {n['amount_sol']:.3f} SOL[/dim]"
                )

        # Если мастеров не нашли но есть прослойки — верхняя из них = кандидат
        if not masters and proxies:
            _top_p = max(proxies, key=lambda x: x.get("depth", 0))
            _ta    = _top_p["funder"]
            _ts    = _top_p.get("amount_sol", 0)
            _td    = _top_p.get("depth", 0)
            _twr   = _top_p.get("wr", 0)
            _tpnl  = _top_p.get("pnl", 0)
            _ttx   = _top_p.get("tx_count", 0)
            _tgtx  = _top_p.get("gmgn_txns", 0)
            _tcr   = _top_p.get("f_created") or []
            _wr_c  = "green" if _twr > 50 else "yellow"
            _pnl_c = "green" if _tpnl > 0 else "red"
            c.print()
            c.print(Panel(
                f"[bold yellow]★ ВЕРХНИЙ ТРАССИРУЕМЫЙ КОШЕЛЁК[/bold yellow]\n"
                f"[bold]{_ta}[/bold]\n"
                f"[dim]Глубина {_td+1} уровень(ей) от деплоера  |  "
                f"Отправил: {_ts:.3f} SOL[/dim]\n"
                f"WR: [{_wr_c}]{_twr:.1f}%[/{_wr_c}]   "
                f"PnL: [{_pnl_c}]{fusd(_tpnl)}[/{_pnl_c}]   "
                f"On-chain tx: {_ttx}   GMGN сделок: {_tgtx}\n"
                f"[dim]Выше — биржевой вывод или история обрезана (Helius free tier)[/dim]\n"
                f"[dim]→ https://gmgn.ai/sol/address/{_ta}[/dim]\n"
                f"[dim]→ https://solscan.io/account/{_ta}[/dim]"
                + (
                    f"\n[yellow]Создал {len(_tcr)} токен(ов): "
                    + ", ".join(
                        (t.get("symbol") or t.get("address", "?")[:8])
                        for t in _tcr[:5]
                    ) + "[/yellow]"
                ) if _tcr else "",
                border_style="yellow",
                title="[bold yellow]🔑 МАСТЕР-КАНДИДАТ (верхний трассируемый)[/bold yellow]",
            ))
            # Сохраняем в monitor.db
            _save_lbl = f"trace-dev-top:{ca[:8]}"
            if save_master_to_monitor(_ta, label=_save_lbl):
                c.print(f"  [bold yellow]⚡ Добавлен в monitor.db![/bold yellow]")
                c.print(f"  [dim]python3 monitor.py --watch[/dim]")

        if masters:
            c.print(
                f"\n  [bold green]★ МАСТЕР-КОШЕЛЬКИ — кандидаты ({len(masters)}):[/bold green]"
            )
            c.print(
                "  [dim](конечные источники финансирования, прошедшие через все прослойки)[/dim]"
            )

            for idx, n in enumerate(masters[:5], 1):
                c.print()
                depth_label = f"[dim](глубина {n['depth']+1})[/dim]"
                c.print(
                    f"  [bold cyan]МАСТЕР #{idx}[/bold cyan] {depth_label}  "
                    f"[bold]{n['funder']}[/bold]"
                )
                c.print(
                    f"  [dim]  ↳ Отправил [cyan]{n['amount_sol']:.3f} SOL[/cyan] "
                    f"через {n['depth']} прослой. → деплоер[/dim]"
                )
                wr_col  = "green"  if n.get("wr",  0) > 50 else "yellow"
                pnl_col = "green"  if n.get("pnl", 0) > 0  else "red"
                c.print(
                    f"  WR: [{wr_col}]{n.get('wr', 0):.1f}%[/{wr_col}]   "
                    f"PnL: [{pnl_col}]{fusd(n.get('pnl', 0))}[/{pnl_col}]   "
                    f"On-chain tx: {n.get('tx_count', '?')}   "
                    f"GMGN сделок: {n.get('gmgn_txns', '?')}"
                )

                fc = n.get("f_created") or []
                if fc:
                    c.print(f"  [yellow]  Создал {len(fc)} токен(ов):[/yellow]")
                    for tok in fc[:6]:
                        sym    = tok.get("symbol") or "?"
                        t_addr = tok.get("address") or tok.get("token_address") or "?"
                        mc     = _scalar(tok.get("market_cap") or tok.get("mc") or 0)
                        t_ts   = tok.get("created_at") or tok.get("open_timestamp") or 0
                        age_s  = fage(t_ts) + " назад" if t_ts else "?"
                        c.print(
                            f"  [dim]    • {sym:<10} {fusd(mc):<10} {age_s:<14} {t_addr}[/dim]"
                        )
                    if len(fc) > 6:
                        c.print(f"  [dim]    … и ещё {len(fc)-6} токенов[/dim]")
                    c.print(
                        "  [bold yellow]  ⚡ Добавь в трекер — "
                        "следующий запуск = твой ранний вход![/bold yellow]"
                    )
                else:
                    c.print(
                        "  [dim]  Созданных токенов в GMGN нет "
                        "(деплоит через другой адрес или ещё нет запусков)[/dim]"
                    )

                c.print(f"  [dim]  → https://gmgn.ai/sol/address/{n['funder']}[/dim]")
                c.print(f"  [dim]  → https://solscan.io/account/{n['funder']}[/dim]")

                # ── Блок 1: рекурсивная трассировка "создан кем" ────────────
                try:
                    c.print(f"\n  [bold cyan]  ↑ Цепочка создания мастер-кошелька:[/bold cyan]")
                    current   = n["funder"]
                    seen_up   = {current}
                    depth_up  = 0
                    max_up    = 4   # максимум 4 уровня вверх

                    while depth_up < max_up:
                        cur_stats  = g_stats(current) or {}
                        cur_common = cur_stats.get("common") or {}
                        ff         = cur_common.get("fund_from_address") or ""
                        ff_ts      = int(cur_common.get("fund_from_ts")  or 0)
                        ff_amt     = cur_common.get("fund_amount")        or ""
                        ff_tx      = cur_common.get("fund_tx_hash")       or ""

                        if not ff or ff in SYSTEM_PROGRAMS or ff in seen_up:
                            break
                        seen_up.add(ff)

                        indent   = "  " * (depth_up + 2)
                        ts_str   = (datetime.fromtimestamp(ff_ts, tz=timezone.utc)
                                    .strftime("%Y-%m-%d") if ff_ts else "неизвестно")
                        proxy_addrs = {p["funder"] for p in proxies}

                        # Классифицируем этот кошелёк
                        ff_stats  = g_stats(ff) or {}
                        ff_pnl_s  = ff_stats.get("pnl_stat") or {}
                        ff_buys   = int(ff_stats.get("buy")  or ff_stats.get("buy_count")  or 0)
                        ff_sells  = int(ff_stats.get("sell") or ff_stats.get("sell_count") or 0)
                        ff_txns   = ff_buys + ff_sells
                        ff_wr_raw = ff_pnl_s.get("winrate") or ff_stats.get("winrate") or 0
                        try:
                            ff_wr = float(ff_wr_raw) * (100 if float(ff_wr_raw) <= 1 else 1)
                        except Exception:
                            ff_wr = 0.0
                        ff_common2  = ff_stats.get("common") or {}
                        ff_cr_cnt   = ff_common2.get("created_token_count") or 0
                        ff_pnl_usd  = _scalar(ff_stats.get("realized_profit") or 0)

                        if ff in proxy_addrs:
                            tag = "[bold red]⚠ УЖЕ ИЗВЕСТНАЯ ПРОСЛОЙКА[/bold red]"
                        elif ff in KNOWN_WALLETS:
                            tag = f"[yellow]★ CEX: {KNOWN_WALLETS[ff]}[/yellow]"
                        elif ff_txns > 100:
                            tag = f"[green]активный трейдер  сделок={ff_txns}  WR={ff_wr:.0f}%  PnL={fusd(ff_pnl_usd)}[/green]"
                        elif ff_cr_cnt > 2:
                            tag = f"[yellow]деплоер  токенов={ff_cr_cnt}[/yellow]"
                        elif ff_txns > 10:
                            tag = f"[dim]умеренная активность  сделок={ff_txns}[/dim]"
                        else:
                            tag = "[dim]неактивен / прослойка[/dim]"

                        c.print(f"{indent}[dim]└─ {ff}[/dim]")
                        c.print(f"{indent}   [dim]создан {ts_str}  {ff_amt} SOL  {tag}[/dim]")
                        if ff_tx:
                            c.print(f"{indent}   [dim]TX: https://solscan.io/tx/{ff_tx[:60]}[/dim]")
                        c.print(f"{indent}   [dim]→ https://gmgn.ai/sol/address/{ff}[/dim]")

                        # Если нашли активный кошелёк — стопаемся
                        if ff_txns > 100 or ff in KNOWN_WALLETS or ff in proxy_addrs:
                            break

                        current = ff
                        depth_up += 1

                except Exception:
                    pass

        # ── Блок 2: Двусторонняя проверка связей прослойка ↔ мастер ─────────
        if proxies and masters:
            c.print(f"\n  [bold magenta]▶ ПРОВЕРКА СВЯЗЕЙ: прослойки ↔ мастера[/bold magenta]")
            c.print("  [dim]Ищем переводы SOL между прослойками и мастер-кошельками…[/dim]")
            found_any = False
            with ThreadPoolExecutor(max_workers=4) as ex:
                futs = {}
                for proxy in proxies[:3]:
                    for master in masters[:3]:
                        key = (proxy["funder"], master["funder"])
                        futs[ex.submit(
                            helius_check_sol_transfers,
                            master["funder"], proxy["funder"]
                        )] = key
                for fut in as_completed(futs):
                    proxy_addr, master_addr = futs[fut]
                    try:
                        transfers = fut.result()
                    except Exception:
                        transfers = []
                    if transfers:
                        found_any = True
                        total_ab = sum(t["amount_sol"] for t in transfers if t["dir"] == "A→B")
                        total_ba = sum(t["amount_sol"] for t in transfers if t["dir"] == "B→A")
                        c.print(
                            f"\n  [bold green]✓ СВЯЗЬ НАЙДЕНА:[/bold green] "
                            f"[cyan]{master_addr[:20]}…[/cyan] ↔ [cyan]{proxy_addr[:20]}…[/cyan]"
                        )
                        if total_ab > 0:
                            c.print(f"  [dim]  Мастер → Прослойка: {total_ab:.3f} SOL "
                                    f"({len([t for t in transfers if t['dir']=='A→B'])} транзакций)[/dim]")
                        if total_ba > 0:
                            c.print(f"  [dim]  Прослойка → Мастер: {total_ba:.3f} SOL "
                                    f"({len([t for t in transfers if t['dir']=='B→A'])} транзакций)[/dim]")
                        c.print(
                            f"  [bold yellow]  → Один и тот же человек контролирует оба кошелька![/bold yellow]"
                        )
                        # Показываем последние 3 транзакции
                        for t in transfers[-3:]:
                            ts_str = (datetime.fromtimestamp(t["ts"], tz=timezone.utc)
                                      .strftime("%m-%d %H:%M") if t["ts"] else "?")
                            arrow = "→" if t["dir"] == "A→B" else "←"
                            c.print(
                                f"  [dim]    {ts_str}  {t['frm'][:16]}… {arrow} "
                                f"{t['to'][:16]}…  {t['amount_sol']:.3f} SOL[/dim]"
                            )
            if not found_any:
                c.print("  [dim]  Прямых SOL-переводов между прослойками и мастерами не найдено.[/dim]")
                c.print("  [dim]  Связь возможна через промежуточные адреса или биржу.[/dim]")

        else:
            c.print(
                "\n  [yellow]Мастер-кошелёк пока не найден на глубине до 3 уровней.[/yellow]"
            )
            c.print(
                "  [dim]Схема может быть глубже. Проверь вручную самую верхнюю прослойку:[/dim]"
            )
            if proxies:
                top_proxy = max(proxies, key=lambda x: x.get("depth", 0))
                c.print(
                    f"  [dim]  → https://solscan.io/account/{top_proxy['funder']}[/dim]"
                )

    # ── Блок 3: Кросс-проектный анализ ───────────────────────────────────
    if dep_created and len(dep_created) >= 2 and HELIUS_KEY:
        c.print(f"\n[bold magenta]▶ КРОСС-ПРОЕКТНЫЙ АНАЛИЗ[/bold magenta]")
        c.print(
            f"  [dim]Проверяю кто финансировал деплоера перед каждым из последних "
            f"{min(6, len(dep_created))} запусков…[/dim]"
        )
        cross_rows, funder_counts = cross_project_funder_analysis(
            creator, dep_created, max_tokens=6
        )

        if cross_rows:
            # Таблица: токен | кто финансировал | SOL | до запуска | повторов
            ct = Table(box=box.SIMPLE, show_header=True, padding=(0, 1))
            ct.add_column("Токен",       style="cyan",  width=10)
            ct.add_column("Дата",        style="dim",   width=6)
            ct.add_column("Фандер",      style="white", width=22)
            ct.add_column("SOL",         style="green", width=8)
            ct.add_column("До запуска",  style="dim",   width=11)
            ct.add_column("Повторов",    style="yellow",width=8)

            for row in sorted(cross_rows, key=lambda x: -x["launch_ts"]):
                ts_str  = (datetime.fromtimestamp(row["launch_ts"], tz=timezone.utc)
                           .strftime("%m-%d") if row["launch_ts"] else "?")
                funder  = row["funder"]
                cnt     = funder_counts.get(funder, 0)
                cnt_str = f"[bold yellow]{cnt}x[/bold yellow]" if cnt >= 2 else str(cnt)
                short   = (funder[:20] + "…") if len(funder) > 20 else funder
                sol_str = f"{row['amount_sol']:.2f}" if row["amount_sol"] else "—"
                hb = row.get("hours_before")
                if hb is None:
                    hb_str = "—"
                elif hb < 1:
                    hb_str = f"{int(hb*60)}мин"
                elif hb < 24:
                    hb_str = f"{hb:.1f}ч"
                else:
                    hb_str = f"{hb/24:.1f}д"
                ct.add_row(row["token"], ts_str, short, sol_str, hb_str, cnt_str)
            c.print(ct)

            # Итог: кто появляется чаще всего
            if funder_counts:
                top_funder, top_cnt = max(funder_counts.items(), key=lambda x: x[1])
                total_projects = len({r["ca"] for r in cross_rows if r["funder"] != "—"})
                if top_cnt >= 2:
                    confidence = "HIGH" if top_cnt >= int(total_projects * 0.6) else "MEDIUM"
                    col = "bold green" if confidence == "HIGH" else "bold yellow"

                    # Определяем — прослойка это или операционный фандер
                    proxy_addrs  = {p["funder"] for p in proxies}
                    master_addrs = {n["funder"] for n in (masters if masters else [])}
                    is_op_funder = top_funder in proxy_addrs and top_cnt >= 3

                    if is_op_funder:
                        c.print(
                            f"\n  [bold cyan]⚡ ОПЕРАЦИОННЫЙ ФАНДЕР ({top_cnt}/{total_projects} проектов):[/bold cyan]"
                        )
                        c.print(f"  [bold]{top_funder}[/bold]")
                        c.print(
                            "  [dim]  → Этот кошелёк регулярно финансирует деплоера перед каждым запуском.[/dim]"
                        )
                        # Средний lead time
                        op_rows = [r for r in cross_rows if r["funder"] == top_funder
                                   and r.get("hours_before") is not None]
                        if op_rows:
                            avg_h = sum(r["hours_before"] for r in op_rows) / len(op_rows)
                            if avg_h < 24:
                                lead_str = f"{avg_h:.1f} часов"
                            else:
                                lead_str = f"{avg_h/24:.1f} дней"
                            c.print(
                                f"  [yellow]  ⏱ Среднее время от финансирования до запуска: {lead_str}[/yellow]"
                            )
                            c.print(
                                "  [dim]  → Как только этот кошелёк получит SOL — жди запуск через ~" + lead_str + "![/dim]"
                            )
                        c.print(f"  [dim]  → https://gmgn.ai/sol/address/{top_funder}[/dim]")
                        c.print(f"  [dim]  → https://solscan.io/account/{top_funder}[/dim]")
                    else:
                        c.print(
                            f"\n  [{col}]★ Подтверждённый фандер ({confidence}): "
                            f"{top_funder[:28]}… [{top_cnt}/{total_projects} проектов][/{col}]"
                        )
                        if top_funder in master_addrs:
                            c.print(
                                "  [bold green]  ✓ Совпадает с мастер-кошельком из трассировки![/bold green]"
                            )
                        else:
                            c.print(
                                f"  [dim]  → https://gmgn.ai/sol/address/{top_funder}[/dim]"
                            )
        else:
            c.print("  [dim]  Нет данных о финансировании по другим токенам деплоера.[/dim]")

    # ── Итог / что делать ────────────────────────────────────────────────
    c.print("\n" + "─" * 68)
    c.print("[bold]📌 Как использовать результаты:[/bold]")
    c.print("[dim]  1. Добавь мастер-кошельки в GMGN Smart Money → 'Track'[/dim]")
    c.print("[dim]  2. Когда кошелёк создаёт новый токен — проверяй его через:[/dim]")
    c.print(f"[dim]       python3 analyze.py <NEW_CA>[/dim]")
    c.print("[dim]  3. Если аналитика чистая — входи в первые 5-10 минут[/dim]")
    c.print("[dim]  4. Подробный анализ кошелька: python3 analyze.py --wallet <ADDR>[/dim]\n")


# ── CLI ───────────────────────────────────────────────────────────────────────
def main():
    global DEBUG
    parser = argparse.ArgumentParser(
        description="Solana Token Analyzer",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Примеры:\n"
            "  python3 analyze.py <CA>               — базовый анализ + Watch List\n"
            "  python3 analyze.py <CA> --trace-dev   — граф деплоер → мастер-кошелёк\n"
            "  python3 analyze.py --wallet <ADDR>    — профиль кошелька\n"
            "  python3 analyze.py <CA> --debug       — показать сырой JSON от GMGN\n"
        ),
    )
    parser.add_argument("ca",           nargs="?", help="Contract Address токена")
    parser.add_argument("--trace-dev",  action="store_true",
                        help="Трассировать деплоера → мастер-кошелёк → будущие токены")
    parser.add_argument("--deep",       action="store_true",
                        help="(устарело) трассировать dev wallet (используй --trace-dev)")
    parser.add_argument("--wallet",     metavar="ADDR", help="Профиль конкретного кошелька")
    parser.add_argument("--debug",      action="store_true",
                        help="Печатать сырой JSON от GMGN (для диагностики)")
    args = parser.parse_args()

    if args.debug:
        DEBUG = True
        print("🔧 DEBUG MODE: будет выводить сырой JSON от каждого GMGN запроса\n")

    if not args.ca and not args.wallet:
        parser.print_help(); sys.exit(0)

    if not GMGN_API_KEY:
        print("❌ Укажи GMGN_API_KEY в .env"); sys.exit(1)

    # Режим: только кошелёк
    if args.wallet and not args.ca:
        trace_wallet(args.wallet); return

    # Основной анализ токена
    result = stage1(args.ca)

    # --trace-dev: граф деплоер → прослойки → мастер-кошелёк
    if args.trace_dev or args.deep:
        sec   = result.get("sec",  {})
        info  = result.get("info", {})
        pool  = result.get("pool", {})

        # Время запуска — берём из stage1 (там уже посчитан с DexScreener фолбэком)
        launch = result.get("age") or (
            info.get("pool_creation_timestamp") or info.get("created_at") or
            info.get("open_timestamp") or
            pool.get("open_timestamp") or pool.get("created_at")
        )

        # Первый получатель токенов из анализа = запасной деплоер
        first_recv = result.get("first_receiver")

        # --wallet при --trace-dev = явный деплоер (приоритет над всеми фолбэками)
        deployer_override = args.wallet if args.wallet else None

        trace_dev(args.ca, launch, sec,
                  deployer_override=deployer_override,
                  first_receiver=first_recv)

    # --wallet без --trace-dev: профиль кошелька
    elif args.wallet and args.ca:
        trace_wallet(args.wallet, args.ca)

if __name__ == "__main__":
    main()
