#!/usr/bin/env python3
"""
Solana Dev Wallet Monitor v3
════════════════════════════
Следит за кластерами из cluster_wallets (заполняется через analyze.py).
Алертит когда любой кошелёк кластера деплоит новый токен.

Команды:
  python3 monitor.py --add <ADDR>          добавить мастер-кошелёк вручную
  python3 monitor.py --init                запомнить текущее состояние без алертов
  python3 monitor.py --test                один цикл проверки
  python3 monitor.py --watch               непрерывный мониторинг
  python3 monitor.py --clusters            показать все кластеры
  python3 monitor.py --tree [ADDR]         дерево кошельков (опц. фильтр)
  python3 monitor.py --alerts              последние алерты
  python3 monitor.py --status              краткая сводка
  python3 monitor.py --reset               сбросить всю БД

Настройки в .env:
  HELIUS_KEY=...
  GMGN_API_KEYS=key1,key2,key3,key4    пул ключей с ротацией
  MONITOR_INTERVAL=5                    минут между циклами (по умолчанию 5)
  MONITOR_MIN_SOL=0.1                   мин. сумма SOL-перевода для отслеживания
  MONITOR_MAX_DEPTH=5                   макс. глубина рекурсии кошельков

Быстрый старт:
  1. python3 analyze.py <CA>    — заполнит cluster_wallets автоматически
  2. python3 monitor.py --init  — запомнить текущие токены (без алертов)
  3. python3 monitor.py --watch — мониторинг

Или вручную (без analyze.py):
  python3 monitor.py --add <MASTER_ADDR> --label "MOMUS"
  python3 monitor.py --init
  python3 monitor.py --watch
"""

from __future__ import annotations
import os, sys, json, time, uuid, asyncio, sqlite3, argparse, textwrap, threading
import requests
from datetime import datetime, timezone
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

# ── .env ─────────────────────────────────────────────────────────────────────
def _load_env():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())
_load_env()

HELIUS_KEY   = os.environ.get("HELIUS_KEY", "")
INTERVAL_MIN = int(os.environ.get("MONITOR_INTERVAL", "5"))
MIN_SOL      = float(os.environ.get("MONITOR_MIN_SOL", "0.1"))
MAX_DEPTH    = int(os.environ.get("MONITOR_MAX_DEPTH", "5"))
CHAIN        = "sol"
GMGN_BASE    = "https://openapi.gmgn.ai"

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "monitor.db")

# ── GMGN ключи — пул с круговой ротацией ─────────────────────────────────────
_raw_keys = os.environ.get("GMGN_API_KEYS", os.environ.get("GMGN_API_KEY", ""))
_GMGN_KEYS: list[str] = [k.strip() for k in _raw_keys.split(",") if k.strip()]
_key_lock  = threading.Lock()
_key_idx   = 0

def _next_gmgn_key() -> str:
    global _key_idx
    if not _GMGN_KEYS:
        return ""
    with _key_lock:
        key = _GMGN_KEYS[_key_idx % len(_GMGN_KEYS)]
        _key_idx += 1
    return key

# ── Системные адреса (игнорируем переводы на них) ────────────────────────────
SKIP_ADDRS = {
    "11111111111111111111111111111111",
    "ComputeBudget111111111111111111111111111111",
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJe1bT4",
    "So11111111111111111111111111111111111111112",
    "SysvarRent111111111111111111111111111111111",
    "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB",
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
    "5tzFkiKscXHK5ZXCGbCAbZseha4ZRmHZmFeFiCNkGXTF",
    "AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2",
    "FWznbcNXWQuHTawe9RxvQ2LdCENssh12dsznf4RiouN5",
    "9un5wqE3q4oCjyrDkwsdD48KteCJitQX5978Vh7KKxHo",
    "GJRs4FwHtemZ5ZE9x3FNvJ8TMwitKTh21yxdRPqn7npE",
    "2ojv9BAiHUrvsm9gxDe7fJSzbNZSJcxZvf8dqmWGHG8S",
    "H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS",
    "rFqFJ9g7TGBD8Ed7TPDnvGKZ5pWLPDyxLcvcH2eRCtt",
    "TokenzQdBNbequvydDktM2rGPSZQfpzbR87YxMd6eTB",
}

# ── Известные биржи — не добавляем в кластер ─────────────────────────────────
KNOWN_CEX = {
    "5tzFkiKscXHK5ZXCGbCAbZseha4ZRmHZmFeFiCNkGXTF": "Binance",
    "AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2": "Kraken",
    "2ojv9BAiHUrvsm9gxDe7fJSzbNZSJcxZvf8dqmWGHG8S": "OKX",
    "FWznbcNXWQuHTawe9RxvQ2LdCENssh12dsznf4RiouN5": "Bybit",
    "9un5wqE3q4oCjyrDkwsdD48KteCJitQX5978Vh7KKxHo": "Gate.io",
    "GJRs4FwHtemZ5ZE9x3FNvJ8TMwitKTh21yxdRPqn7npE": "MEXC",
    "H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS": "Coinbase",
}

# ── ANSI ──────────────────────────────────────────────────────────────────────
R="\033[91m"; G="\033[92m"; Y="\033[93m"; B="\033[94m"
M="\033[95m"; C="\033[96m"; W="\033[97m"; DIM="\033[2m"; BOLD="\033[1m"; RST="\033[0m"

def ts_fmt(ts: int) -> str:
    if not ts: return "—"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%d.%m %H:%M")

def short(addr: str) -> str:
    return f"{addr[:6]}…{addr[-4:]}" if addr and len(addr) > 12 else (addr or "?")

def now() -> int:
    return int(time.time())

def log(msg: str):
    print(f"{DIM}[{datetime.now().strftime('%H:%M:%S')}]{RST} {msg}")

# ── SQLite ────────────────────────────────────────────────────────────────────
def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def db_init():
    """
    Инициализирует БД. Схема синхронизирована с analyze.py (_init_monitor_db).
    """
    with db_connect() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS masters (
            address   TEXT PRIMARY KEY,
            label     TEXT DEFAULT '',
            added_at  INTEGER,
            active    INTEGER DEFAULT 1,
            source_ca TEXT DEFAULT ''
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
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            master_addr   TEXT,
            source_ca     TEXT,
            cluster_label TEXT,
            wallet_addr   TEXT,
            role          TEXT,
            depth         INTEGER,
            funded_target TEXT,
            amount_sol    REAL,
            added_at      INTEGER,
            UNIQUE(master_addr, wallet_addr)
        );

        CREATE TABLE IF NOT EXISTS cluster_tokens (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            master_addr  TEXT,
            token_ca     TEXT,
            token_symbol TEXT,
            token_name   TEXT,
            deploy_ts    INTEGER DEFAULT 0,
            added_at     INTEGER,
            alerted      INTEGER DEFAULT 0,
            UNIQUE(master_addr, token_ca)
        );

        CREATE TABLE IF NOT EXISTS transfers (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            from_addr  TEXT,
            to_addr    TEXT,
            amount_sol REAL,
            ts         INTEGER,
            sig        TEXT UNIQUE,
            depth_from INTEGER,
            master     TEXT
        );

        CREATE TABLE IF NOT EXISTS tokens (
            ca         TEXT PRIMARY KEY,
            deployer   TEXT,
            master     TEXT,
            name       TEXT,
            symbol     TEXT,
            launch_ts  INTEGER,
            market_cap REAL,
            chain_path TEXT,
            depth      INTEGER,
            alerted    INTEGER DEFAULT 0,
            found_at   INTEGER
        );

        CREATE TABLE IF NOT EXISTS alerts (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            type      TEXT,
            master    TEXT,
            data_json TEXT,
            message   TEXT,
            ts        INTEGER
        );
        """)

        # Миграции для старых БД (безопасно — IGNORE если уже есть)
        migrations = [
            ("masters",        "source_ca", "TEXT DEFAULT ''"),
            ("wallets",        "role",      "TEXT DEFAULT 'unknown'"),
            ("cluster_tokens", "alerted",   "INTEGER DEFAULT 0"),
        ]
        for tbl, col, typedef in migrations:
            try:
                conn.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {typedef}")
                conn.commit()
            except Exception:
                pass


# ── Helius RPC ────────────────────────────────────────────────────────────────
def _rpc(method: str, params) -> Optional[object]:
    if not HELIUS_KEY:
        return None
    try:
        r = requests.post(
            f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}",
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            timeout=20,
        )
        return r.json().get("result")
    except Exception:
        return None

def _enhanced_txs(sigs: list) -> list:
    if not sigs or not HELIUS_KEY:
        return []
    out = []
    for i in range(0, len(sigs), 100):
        batch = sigs[i:i+100]
        try:
            r = requests.post(
                f"https://api.helius.xyz/v0/transactions/?api-key={HELIUS_KEY}",
                json={"transactions": batch},
                timeout=30,
            )
            data = r.json()
            if isinstance(data, list):
                out.extend(data)
        except Exception:
            pass
    return out


# ── GMGN API ──────────────────────────────────────────────────────────────────
def _gmgn_get(path: str, params: dict = None):
    key = _next_gmgn_key()
    p = {"timestamp": now(), "client_id": str(uuid.uuid4())}
    if params:
        p.update(params)
    try:
        r = requests.get(
            f"{GMGN_BASE}{path}",
            headers={"X-APIKEY": key} if key else {},
            params=p, timeout=15,
        )
        raw = r.text.strip()
        if raw in ("null", "true", "false", ""):
            return None
        d = r.json()
        if isinstance(d, dict) and d.get("code") == 0:
            return d.get("data")
    except Exception:
        pass
    return None

def _gmgn_created(wallet: str) -> list:
    d = _gmgn_get("/v1/user/created_tokens",
                  {"chain": CHAIN, "wallet_address": wallet})
    if isinstance(d, list):
        return d
    if isinstance(d, dict):
        return d.get("tokens", d.get("data", []))
    return []


# ── Парсинг исходящих SOL переводов ──────────────────────────────────────────
def _fetch_outgoing(wallet: str, last_sig: Optional[str]) -> tuple[list, Optional[str]]:
    """
    Возвращает (transfers, newest_sig).
    transfers = [{to, amount_sol, ts, sig}, ...]
    """
    sig_infos: list = []
    before_cursor   = None

    for _ in range(10):
        params: list = [wallet, {"limit": 100, "commitment": "finalized"}]
        if before_cursor:
            params[1]["before"] = before_cursor
        if last_sig:
            params[1]["until"] = last_sig

        result = _rpc("getSignaturesForAddress", params)
        if not isinstance(result, list) or not result:
            break
        sig_infos.extend(result)
        if len(result) < 100:
            break
        before_cursor = result[-1]["signature"]

    if not sig_infos:
        return [], last_sig

    newest_sig = sig_infos[0]["signature"]
    sigs = [s["signature"] for s in sig_infos if not s.get("err")][:100]
    txs  = _enhanced_txs(sigs)

    transfers: list = []
    seen_sigs: set  = set()

    for tx in txs:
        ts  = tx.get("timestamp") or 0
        sig = tx.get("signature") or ""
        if sig in seen_sigs:
            continue
        seen_sigs.add(sig)

        found = False
        # Метод 1: nativeTransfers
        for nt in (tx.get("nativeTransfers") or []):
            frm = nt.get("fromUserAccount") or ""
            to  = nt.get("toUserAccount") or ""
            amt = float(nt.get("amount") or 0) / 1e9
            if frm == wallet and to and to not in SKIP_ADDRS and to not in KNOWN_CEX and amt >= MIN_SOL:
                transfers.append({"to": to, "amount_sol": amt, "ts": ts, "sig": sig})
                found = True

        # Метод 2: accountData fallback
        if not found:
            acct_data = tx.get("accountData") or []
            src_chg   = 0.0
            for ad in acct_data:
                if ad.get("account") == wallet:
                    src_chg = float(ad.get("nativeBalanceChange") or 0) / 1e9
                    break
            if src_chg < -MIN_SOL:
                best_to, best_amt = "", 0.0
                for ad in acct_data:
                    acc = ad.get("account") or ""
                    if acc == wallet or acc in SKIP_ADDRS or acc in KNOWN_CEX:
                        continue
                    chg = float(ad.get("nativeBalanceChange") or 0) / 1e9
                    if chg >= MIN_SOL and chg > best_amt:
                        best_to, best_amt = acc, chg
                if best_to:
                    transfers.append({"to": best_to, "amount_sol": best_amt,
                                      "ts": ts, "sig": sig})

    return transfers, newest_sig


# ── Async обёртки ─────────────────────────────────────────────────────────────
_executor = ThreadPoolExecutor(max_workers=20)

async def async_fetch_outgoing(wallet: str, last_sig: Optional[str]):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _fetch_outgoing, wallet, last_sig)

async def async_gmgn_created(wallet: str):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _gmgn_created, wallet)


# ── Вспомогательные функции БД ────────────────────────────────────────────────
def _ensure_wallet_row(conn: sqlite3.Connection, address: str, master: str,
                       depth: int = 0, parent: str = None,
                       amount: float = 0.0, init_mode: bool = False):
    """Создаёт запись в wallets если её нет — для хранения last_sig / таймингов."""
    conn.execute(
        "INSERT OR IGNORE INTO wallets "
        "(address,master,role,depth,parent,path_json,amount_received,"
        " first_seen,last_sig,last_tx_scan,last_tok_scan,init_mode) "
        "VALUES (?,?,?,?,?,?,?,?,NULL,0,0,?)",
        (address, master, "unknown", depth, parent,
         json.dumps([address]), amount, now(),
         1 if init_mode else 0)
    )

def _wallet_timing(conn: sqlite3.Connection, address: str) -> dict:
    row = conn.execute(
        "SELECT last_sig, last_tx_scan, last_tok_scan, init_mode "
        "FROM wallets WHERE address=?", (address,)
    ).fetchone()
    if row:
        return dict(row)
    return {"last_sig": None, "last_tx_scan": 0, "last_tok_scan": 0, "init_mode": 0}

def _cluster_label(conn: sqlite3.Connection, master_addr: str) -> str:
    """Возвращает читаемую метку кластера."""
    row = conn.execute(
        "SELECT label FROM masters WHERE address=?", (master_addr,)
    ).fetchone()
    if row and row["label"]:
        return row["label"]
    row2 = conn.execute(
        "SELECT cluster_label FROM cluster_wallets "
        "WHERE master_addr=? AND cluster_label!='' LIMIT 1",
        (master_addr,)
    ).fetchone()
    if row2:
        return row2["cluster_label"]
    return short(master_addr)


# ── Сканирование SOL-переводов ────────────────────────────────────────────────
async def scan_wallet_transfers(wallet_addr: str, master_addr: str, depth: int,
                                conn: sqlite3.Connection,
                                init_mode: bool = False) -> list[dict]:
    """
    Сканирует исходящие SOL от wallet_addr.
    Новые получатели добавляются в cluster_wallets и wallets.
    Возвращает список новых записей [{address, depth, parent, amount, ...}].
    """
    if depth >= MAX_DEPTH:
        return []

    timing   = _wallet_timing(conn, wallet_addr)
    last_sig = timing["last_sig"]

    transfers, newest_sig = await async_fetch_outgoing(wallet_addr, last_sig)

    # Обновляем курсор
    conn.execute(
        "UPDATE wallets SET last_sig=?, last_tx_scan=? WHERE address=?",
        (newest_sig or last_sig, now(), wallet_addr)
    )

    # Контекст кластера для новых кошельков
    cw = conn.execute(
        "SELECT cluster_label, source_ca FROM cluster_wallets "
        "WHERE master_addr=? AND wallet_addr=? LIMIT 1",
        (master_addr, wallet_addr)
    ).fetchone()
    c_label    = (cw["cluster_label"] if cw else None) or _cluster_label(conn, master_addr)
    c_src_ca   = (cw["source_ca"]     if cw else None) or ""

    new_wallets: list = []
    for t in transfers:
        to_addr = t["to"]

        # Уже в кластере?
        in_cluster = conn.execute(
            "SELECT id FROM cluster_wallets WHERE master_addr=? AND wallet_addr=?",
            (master_addr, to_addr)
        ).fetchone()

        if not in_cluster:
            conn.execute(
                "INSERT OR IGNORE INTO cluster_wallets "
                "(master_addr,source_ca,cluster_label,wallet_addr,role,"
                " depth,funded_target,amount_sol,added_at) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (master_addr, c_src_ca, c_label, to_addr,
                 "new_wallet", depth + 1,
                 wallet_addr, t["amount_sol"], now())
            )
            _ensure_wallet_row(conn, to_addr, master_addr,
                               depth + 1, wallet_addr, t["amount_sol"], init_mode)
            new_wallets.append({
                "address":       to_addr,
                "depth":         depth + 1,
                "parent":        wallet_addr,
                "amount":        t["amount_sol"],
                "ts":            t["ts"],
                "sig":           t["sig"],
                "master":        master_addr,
                "cluster_label": c_label,
                "init_mode":     init_mode,
            })

        # transfers таблица — для истории
        try:
            conn.execute(
                "INSERT OR IGNORE INTO transfers "
                "(from_addr,to_addr,amount_sol,ts,sig,depth_from,master) "
                "VALUES (?,?,?,?,?,?,?)",
                (wallet_addr, to_addr, t["amount_sol"],
                 t["ts"], t["sig"], depth, master_addr)
            )
        except Exception:
            pass

    conn.commit()
    return new_wallets


# ── Сканирование токенов ──────────────────────────────────────────────────────
async def scan_wallet_tokens(wallet_addr: str, master_addr: str,
                              conn: sqlite3.Connection,
                              init_mode: bool = False) -> list[dict]:
    """
    Запрашивает GMGN на новые задеплоенные токены от wallet_addr.
    Дедуплицирует через cluster_tokens.
    Возвращает список новых токенов для алерта.
    """
    timing       = _wallet_timing(conn, wallet_addr)
    last_tok_sc  = timing["last_tok_scan"] or 0
    in_init      = timing["init_mode"] == 1 or init_mode

    # Не чаще раза в 8 минут на кошелёк
    if now() - last_tok_sc < 480:
        return []

    tokens = await async_gmgn_created(wallet_addr)
    conn.execute(
        "UPDATE wallets SET last_tok_scan=? WHERE address=?",
        (now(), wallet_addr)
    )

    c_label   = _cluster_label(conn, master_addr)
    new_toks: list = []

    for tok in (tokens or []):
        ca = (tok.get("address") or tok.get("mint") or
              tok.get("token_address") or "")
        if not ca:
            continue

        # Уже в cluster_tokens?
        exists = conn.execute(
            "SELECT id FROM cluster_tokens WHERE master_addr=? AND token_ca=?",
            (master_addr, ca)
        ).fetchone()
        if exists:
            continue

        name   = tok.get("name")   or "?"
        symbol = tok.get("symbol") or "?"
        launch = int(tok.get("open_timestamp") or tok.get("created_at") or 0)
        mc     = float(tok.get("market_cap") or tok.get("usd_market_cap") or 0)

        # Сохраняем в cluster_tokens
        conn.execute(
            "INSERT OR IGNORE INTO cluster_tokens "
            "(master_addr,token_ca,token_symbol,token_name,deploy_ts,added_at,alerted) "
            "VALUES (?,?,?,?,?,?,?)",
            (master_addr, ca, symbol, name, launch, now(),
             1 if in_init else 0)
        )
        # Обратная совместимость — tokens таблица
        conn.execute(
            "INSERT OR IGNORE INTO tokens "
            "(ca,deployer,master,name,symbol,launch_ts,market_cap,"
            " chain_path,depth,alerted,found_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (ca, wallet_addr, master_addr, name, symbol, launch, mc,
             json.dumps([wallet_addr]), 0,
             1 if in_init else 0, now())
        )
        conn.commit()

        if not in_init:
            new_toks.append({
                "ca":            ca,
                "name":          name,
                "symbol":        symbol,
                "launch_ts":     launch,
                "market_cap":    mc,
                "deployer":      wallet_addr,
                "master":        master_addr,
                "cluster_label": c_label,
            })

    conn.commit()
    return new_toks


# ── Один цикл мониторинга ─────────────────────────────────────────────────────
async def run_cycle(init_mode: bool = False, silent: bool = False) -> dict:
    conn  = db_connect()
    stats = {"new_wallets": 0, "new_tokens": 0, "alerts": []}

    def _load_cluster_rows():
        """Загружает кошельки из cluster_wallets (основной источник)."""
        rows = conn.execute("""
            SELECT DISTINCT cw.wallet_addr, cw.master_addr, cw.depth
            FROM cluster_wallets cw
            ORDER BY cw.depth ASC
        """).fetchall()
        if rows:
            return rows
        # Fallback: wallets JOIN masters (старые БД без cluster_wallets)
        return conn.execute("""
            SELECT w.address AS wallet_addr, w.master AS master_addr, w.depth
            FROM wallets w
            JOIN masters m ON w.master = m.address
            WHERE m.active = 1
            ORDER BY w.depth ASC
        """).fetchall()

    cluster_rows = _load_cluster_rows()

    if not cluster_rows:
        if not silent:
            log(f"{Y}Нет кошельков. Запусти analyze.py <CA> или monitor.py --add <ADDR>{RST}")
        conn.close()
        return stats

    masters_set = set(r["master_addr"] for r in cluster_rows)
    if not silent:
        log(f"Сканирую {len(cluster_rows)} кошельков | {len(masters_set)} кластер(ов)…")

    # Убеждаемся что у каждого кошелька есть строка в wallets (для таймингов)
    for r in cluster_rows:
        _ensure_wallet_row(conn, r["wallet_addr"], r["master_addr"], r["depth"])
    conn.commit()

    # ── Параллельно сканируем SOL-переводы ────────────────────────────────
    tx_tasks   = [
        scan_wallet_transfers(r["wallet_addr"], r["master_addr"],
                              r["depth"], conn, init_mode)
        for r in cluster_rows
    ]
    tx_results = await asyncio.gather(*tx_tasks, return_exceptions=True)

    for result in tx_results:
        if isinstance(result, Exception):
            continue
        for nw in result:
            stats["new_wallets"] += 1
            if not init_mode and not silent:
                log(f"  ➕ {short(nw['parent'])} → {short(nw['address'])}"
                    f"  {nw['amount']:.3f} SOL"
                    f"  [{nw['cluster_label']}]  глубина {nw['depth']}")

    # Перезагружаем — могли добавиться новые кошельки
    cluster_rows = _load_cluster_rows()
    for r in cluster_rows:
        _ensure_wallet_row(conn, r["wallet_addr"], r["master_addr"], r["depth"])
    conn.commit()

    # ── Параллельно проверяем токены ───────────────────────────────────────
    tok_tasks   = [
        scan_wallet_tokens(r["wallet_addr"], r["master_addr"], conn, init_mode)
        for r in cluster_rows
    ]
    tok_results = await asyncio.gather(*tok_tasks, return_exceptions=True)

    for result in tok_results:
        if isinstance(result, Exception):
            continue
        for tok in result:
            stats["new_tokens"] += 1
            msg = _format_token_alert(tok)
            stats["alerts"].append((msg, tok))

            conn.execute(
                "INSERT INTO alerts (type,master,data_json,message,ts) VALUES (?,?,?,?,?)",
                ("new_token", tok["master"], json.dumps(tok), msg, now())
            )
            conn.execute(
                "UPDATE cluster_tokens SET alerted=1 WHERE master_addr=? AND token_ca=?",
                (tok["master"], tok["ca"])
            )
            conn.execute(
                "UPDATE tokens SET alerted=1 WHERE ca=?", (tok["ca"],)
            )
            conn.commit()

    conn.close()
    return stats


# ── Форматирование алерта ─────────────────────────────────────────────────────
def _format_token_alert(tok: dict) -> str:
    master        = tok.get("master", "")
    cluster_label = tok.get("cluster_label") or short(master)
    mc            = tok.get("market_cap") or 0
    mc_str        = f"${mc:,.0f}" if mc else "неизвестно"
    symbol        = tok.get("symbol") or "?"
    name          = tok.get("name")   or "?"
    ca            = tok.get("ca")     or ""
    deployer      = tok.get("deployer") or ""
    launch        = tok.get("launch_ts") or 0

    header    = f"🚨 НОВЫЙ ДЕПЛОЙ — кластер [{cluster_label}]"
    separator = "─" * max(len(header), 50)

    lines = [
        header,
        separator,
        f"Токен    : {symbol} / {name}",
        f"Деплоер  : {deployer}",
        f"Мастер   : {master}",
        f"Запуск   : {ts_fmt(launch)}",
        f"МКап     : {mc_str}",
        "",
        f"▶ Токен   : https://gmgn.ai/sol/token/{ca}",
        f"▶ Деплоер : https://gmgn.ai/sol/address/{deployer}",
        f"▶ Мастер  : https://gmgn.ai/sol/address/{master}",
    ]
    return "\n".join(lines)


def _trigger_analyze(ca: str, cluster_label: str = ""):
    """Запускает analyze.py <ca> синхронно в том же потоке (вывод в терминал)."""
    import subprocess
    analyze_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "analyze.py")
    if not os.path.exists(analyze_path):
        log("  analyze.py не найден рядом — автоанализ пропущен")
        return
    lbl_str = f" [{cluster_label}]" if cluster_label else ""
    print(f"\n{C}{'─'*62}{RST}")
    print(f"{C}▶ Автоанализ{lbl_str}: {ca}{RST}")
    print(f"{C}{'─'*62}{RST}")
    try:
        subprocess.run([sys.executable, analyze_path, ca], timeout=120)
    except subprocess.TimeoutExpired:
        log(f"  analyze.py timeout для {ca}")
    except Exception as e:
        log(f"  analyze.py ошибка: {e}")


def _print_alert(msg: str):
    border = "═" * 62
    print(f"\n{Y}{border}{RST}")
    print(f"{Y}🚨 ALERT  {datetime.now().strftime('%d.%m %H:%M:%S')}{RST}")
    print(msg)
    print(f"{Y}{border}{RST}\n")


# ── CLI команды ───────────────────────────────────────────────────────────────
def cmd_add(addr: str, label: str = ""):
    db_init()
    with db_connect() as conn:
        existing = conn.execute(
            "SELECT address FROM masters WHERE address=?", (addr,)
        ).fetchone()
        if existing:
            print(f"{Y}Уже отслеживается: {addr}{RST}")
            if label:
                conn.execute("UPDATE masters SET label=? WHERE address=?", (label, addr))
                conn.execute(
                    "UPDATE cluster_wallets SET cluster_label=? WHERE master_addr=?",
                    (label, addr)
                )
                conn.commit()
                print(f"  Метка обновлена: {label}")
            return

        conn.execute(
            "INSERT INTO masters (address,label,added_at,active,source_ca) "
            "VALUES (?,?,?,1,'')",
            (addr, label, now())
        )
        # Seed: добавляем мастер как глубина 0 в cluster_wallets
        conn.execute(
            "INSERT OR IGNORE INTO cluster_wallets "
            "(master_addr,source_ca,cluster_label,wallet_addr,role,"
            " depth,funded_target,amount_sol,added_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (addr, "", label or short(addr), addr, "master", 0, None, 0.0, now())
        )
        _ensure_wallet_row(conn, addr, addr, 0)
        conn.commit()

    print(f"{G}✓ Мастер добавлен: {addr}{RST}")
    if label:
        print(f"  Метка: {label}")
    print()
    print(f"  {DIM}Совет: лучший способ добавить кластер — запустить{RST}")
    print(f"  {DIM}  python3 analyze.py <CA>{RST}")
    print(f"  {DIM}Он сам найдёт мастера и сохранит весь кластер.{RST}")
    print()
    print(f"  Следующий шаг: {BOLD}python3 monitor.py --init{RST}")


def cmd_init():
    print(f"\n{BOLD}🔄 INIT — запоминаем состояние без алертов{RST}")
    print(f"Это может занять 1–2 минуты…\n")
    stats = asyncio.run(run_cycle(init_mode=True, silent=False))
    print(f"\n{G}✓ Init завершён.{RST}")
    print(f"  Новых кошельков обнаружено : {stats['new_wallets']}")
    print(f"  Токены запомнены           : без алертов")
    print(f"\nТеперь запускай: {BOLD}python3 monitor.py --watch{RST}")


def cmd_test():
    print(f"\n{BOLD}🔍 TEST — один цикл{RST}\n")
    stats = asyncio.run(run_cycle(init_mode=False, silent=False))
    print(f"\n{G}─── Результат ───{RST}")
    print(f"  Новых кошельков : {stats['new_wallets']}")
    print(f"  Новых токенов   : {stats['new_tokens']}")
    for msg, tok in stats["alerts"]:
        _print_alert(msg)
        _trigger_analyze(tok["ca"], tok.get("cluster_label", ""))
    if not stats["alerts"]:
        print(f"  Алертов         : 0  (всё тихо)")


def cmd_watch(interval: int = INTERVAL_MIN):
    print(f"\n{BOLD}{'═'*62}{RST}")
    print(f"{BOLD}  🔭 МОНИТОРИНГ ЗАПУЩЕН{RST}")
    print(f"{'═'*62}")

    with db_connect() as conn:
        clusters = conn.execute("""
            SELECT master_addr, cluster_label,
                   COUNT(DISTINCT wallet_addr) AS cnt
            FROM cluster_wallets
            GROUP BY master_addr
            ORDER BY cnt DESC
        """).fetchall()

    if clusters:
        print(f"  Кластеров   : {len(clusters)}")
        for cl in clusters:
            lbl = cl["cluster_label"] or short(cl["master_addr"])
            print(f"    [{lbl}]  {cl['master_addr']}  ({cl['cnt']} кош.)")
    else:
        with db_connect() as conn:
            masters = conn.execute(
                "SELECT address, label FROM masters WHERE active=1"
            ).fetchall()
        print(f"  Мастеров    : {len(masters)}")
        for m in masters:
            lbl = m["label"] or short(m["address"])
            print(f"    [{lbl}]  {m['address']}")

    print(f"  Интервал    : {interval} мин")
    print(f"  MIN_SOL     : {MIN_SOL} SOL")
    print(f"  GMGN ключей : {len(_GMGN_KEYS)}")
    if not _GMGN_KEYS:
        print(f"  {Y}⚠️  GMGN_API_KEYS не заданы — проверка токенов не работает!{RST}")
    print(f"{'═'*62}\n")

    cycle = 0
    while True:
        cycle += 1
        log(f"{'─'*50} цикл #{cycle}")
        try:
            stats = asyncio.run(run_cycle(init_mode=False, silent=False))
            for msg, tok in stats["alerts"]:
                _print_alert(msg)
                _trigger_analyze(tok["ca"], tok.get("cluster_label", ""))
            if stats["new_wallets"] == 0 and stats["new_tokens"] == 0:
                log("Изменений нет.")
        except KeyboardInterrupt:
            print(f"\n{Y}Мониторинг остановлен.{RST}")
            break
        except Exception as e:
            log(f"{R}Ошибка цикла: {e}{RST}")
        log(f"Следующая проверка через {interval} мин.  Ctrl+C для остановки.")
        try:
            time.sleep(interval * 60)
        except KeyboardInterrupt:
            print(f"\n{Y}Мониторинг остановлен.{RST}")
            break


def cmd_clusters():
    """Показать все кластеры из cluster_wallets."""
    with db_connect() as conn:
        cluster_masters = conn.execute("""
            SELECT master_addr, cluster_label,
                   COUNT(DISTINCT wallet_addr) AS wallet_cnt
            FROM cluster_wallets
            GROUP BY master_addr
            ORDER BY wallet_cnt DESC
        """).fetchall()

        if not cluster_masters:
            print(f"\n{Y}cluster_wallets пуст.{RST}")
            print("Запусти analyze.py на токен — он заполнит кластеры автоматически.")
            print("Или: --add <ADDR> --label 'ИМЯ' для ручного добавления.")
            return

        print(f"\n{BOLD}{'═'*70}{RST}")
        print(f"{BOLD}  КЛАСТЕРЫ  ({len(cluster_masters)}){RST}")
        print(f"{'═'*70}")

        for cm in cluster_masters:
            master = cm["master_addr"]
            label  = cm["cluster_label"] or short(master)
            wcnt   = cm["wallet_cnt"]

            tokens = conn.execute("""
                SELECT token_ca, token_symbol, deploy_ts, alerted
                FROM cluster_tokens WHERE master_addr=?
                ORDER BY deploy_ts DESC
            """, (master,)).fetchall()

            roles = conn.execute("""
                SELECT role, COUNT(*) AS cnt
                FROM cluster_wallets WHERE master_addr=?
                GROUP BY role ORDER BY cnt DESC
            """, (master,)).fetchall()

            new_by_monitor = conn.execute(
                "SELECT COUNT(*) FROM cluster_wallets "
                "WHERE master_addr=? AND role='new_wallet'",
                (master,)
            ).fetchone()[0]

            roles_str = "  ".join(f"{r['role']}: {r['cnt']}" for r in roles)

            print(f"\n  {BOLD}{C}[{label}]{RST}  {DIM}{master}{RST}")
            print(f"  {'─'*60}")
            print(f"  Кошельков  : {wcnt}  ({roles_str})")
            if new_by_monitor:
                print(f"  Найдено монитором : {new_by_monitor} новых кошельков")

            if tokens:
                print(f"  Токены ({len(tokens)}):")
                for t in tokens[:5]:
                    st = f"  {G}✓{RST}" if t["alerted"] else f"  {Y}NEW{RST}"
                    print(f"    🚀 {t['token_symbol']:8}  "
                          f"{ts_fmt(t['deploy_ts'])}  "
                          f"{DIM}{t['token_ca'][:22]}…{RST}{st}")
                if len(tokens) > 5:
                    print(f"    {DIM}…ещё {len(tokens)-5}{RST}")
            else:
                print(f"  Токены     : 0")

        print(f"\n{'═'*70}")


def cmd_tree(master_filter: str = ""):
    """Показать дерево кошельков кластера."""
    with db_connect() as conn:
        if master_filter:
            cluster_masters = conn.execute("""
                SELECT DISTINCT master_addr, cluster_label
                FROM cluster_wallets WHERE master_addr LIKE ?
            """, (f"%{master_filter}%",)).fetchall()
        else:
            cluster_masters = conn.execute("""
                SELECT DISTINCT master_addr, cluster_label
                FROM cluster_wallets
            """).fetchall()

        # Fallback
        if not cluster_masters:
            cluster_masters = conn.execute(
                "SELECT address AS master_addr, label AS cluster_label "
                "FROM masters WHERE active=1"
            ).fetchall()

        if not cluster_masters:
            print("Нет кластеров. Запусти analyze.py <CA> или --add <ADDR>")
            return

        for cm in cluster_masters:
            master = cm["master_addr"]
            label  = cm["cluster_label"] or short(master)

            wallets = conn.execute("""
                SELECT wallet_addr, role, depth, funded_target, amount_sol, added_at
                FROM cluster_wallets WHERE master_addr=?
                ORDER BY depth ASC, added_at ASC
            """, (master,)).fetchall()

            tokens = conn.execute("""
                SELECT token_ca, token_symbol, token_name, deploy_ts, alerted
                FROM cluster_tokens WHERE master_addr=?
                ORDER BY deploy_ts DESC
            """, (master,)).fetchall()

            print(f"\n{BOLD}{'═'*72}{RST}")
            print(f"{BOLD}  [{label}]{RST}  {DIM}{master}{RST}")
            print(f"  {len(wallets)} кошельков  |  {len(tokens)} токен(ов)")
            print(f"{'─'*72}")

            by_depth: dict = {}
            for w in wallets:
                by_depth.setdefault(w["depth"], []).append(w)

            for depth in sorted(by_depth.keys()):
                group = by_depth[depth]
                dlabel = {-1: "ДЕПЛОЕР", 0: "МАСТЕР / исток"}.get(depth, f"Глубина {depth}")
                print(f"\n  {C}{dlabel}{RST}  ({len(group)} кош.)")
                for w in group[:30]:
                    indent   = "    " + "  " * max(0, depth)
                    role_str = (f" {DIM}[{w['role']}]{RST}"
                                if w["role"] not in ("unknown", "") else "")
                    amt_str  = (f" {w['amount_sol']:.3f} SOL"
                                if (w["amount_sol"] or 0) > 0 else "")
                    print(f"{indent}{w['wallet_addr']}"
                          f"{role_str}"
                          f"  {DIM}{amt_str}  {ts_fmt(w['added_at'])}{RST}")
                if len(group) > 30:
                    print(f"  {DIM}… ещё {len(group)-30}{RST}")

            if tokens:
                print(f"\n  {Y}ТОКЕНЫ:{RST}")
                for t in tokens:
                    flag = f"{G}✓{RST}" if t["alerted"] else f"{Y}!{RST}"
                    print(f"  {flag} {t['token_symbol']:8}  "
                          f"{ts_fmt(t['deploy_ts'])}  {t['token_ca']}")


def cmd_alerts(n: int = 20):
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT * FROM alerts ORDER BY ts DESC LIMIT ?", (n,)
        ).fetchall()
    if not rows:
        print("Алертов пока нет.")
        return
    print(f"\n{BOLD}Последние {len(rows)} алертов:{RST}")
    for row in rows:
        print(f"\n{DIM}[{ts_fmt(row['ts'])}]{RST}  {row['type']}")
        print(textwrap.indent(row["message"], "  "))


def cmd_reset():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"{G}✓ БД удалена: {DB_PATH}{RST}")
    db_init()
    print(f"{G}✓ Новая БД инициализирована.{RST}")


def cmd_status():
    with db_connect() as conn:
        masters_cnt  = conn.execute(
            "SELECT COUNT(*) FROM masters WHERE active=1"
        ).fetchone()[0]
        clusters_cnt = conn.execute(
            "SELECT COUNT(DISTINCT master_addr) FROM cluster_wallets"
        ).fetchone()[0]
        cw_cnt       = conn.execute(
            "SELECT COUNT(*) FROM cluster_wallets"
        ).fetchone()[0]
        ct_cnt       = conn.execute(
            "SELECT COUNT(*) FROM cluster_tokens"
        ).fetchone()[0]
        ct_alerted   = conn.execute(
            "SELECT COUNT(*) FROM cluster_tokens WHERE alerted=1"
        ).fetchone()[0]
        wallets_cnt  = conn.execute(
            "SELECT COUNT(*) FROM wallets"
        ).fetchone()[0]
        alerts_cnt   = conn.execute(
            "SELECT COUNT(*) FROM alerts"
        ).fetchone()[0]
        last_alert   = conn.execute(
            "SELECT ts FROM alerts ORDER BY ts DESC LIMIT 1"
        ).fetchone()

    print(f"\n{BOLD}СТАТУС МОНИТОРА{RST}")
    print(f"  Кластеров               : {clusters_cnt}  "
          f"(masters таблица: {masters_cnt})")
    print(f"  Кошельков в кластерах   : {cw_cnt}")
    print(f"  Токенов в кластерах     : {ct_cnt}  "
          f"(алертились: {ct_alerted})")
    print(f"  Записей wallets (тайм.) : {wallets_cnt}")
    print(f"  Алертов всего           : {alerts_cnt}")
    if last_alert:
        print(f"  Последний алерт         : {ts_fmt(last_alert['ts'])}")
    print(f"  GMGN ключей             : {len(_GMGN_KEYS)}", end="")
    if not _GMGN_KEYS:
        print(f"  {Y}⚠️  нет ключей!{RST}", end="")
    print()


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Solana Dev Wallet Monitor v3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
        Быстрый старт (рекомендуемый путь):
          python3 analyze.py <CA>          # строит кластер и сохраняет в monitor.db
          python3 monitor.py --init        # запомнить текущие токены без алертов
          python3 monitor.py --watch       # начать мониторинг

        Ручное добавление мастера:
          python3 monitor.py --add <ADDR> --label "MOMUS"
          python3 monitor.py --init
          python3 monitor.py --watch
        """),
    )
    parser.add_argument("--add",      metavar="ADDR",  help="Добавить мастер-кошелёк")
    parser.add_argument("--label",    metavar="LABEL", default="",
                        help="Метка кластера (для --add)")
    parser.add_argument("--init",     action="store_true",
                        help="Запомнить текущее состояние без алертов")
    parser.add_argument("--test",     action="store_true",
                        help="Один цикл проверки")
    parser.add_argument("--watch",    action="store_true",
                        help="Непрерывный мониторинг")
    parser.add_argument("--interval", type=int, default=INTERVAL_MIN,
                        help="Минут между циклами (по умолчанию 5)")
    parser.add_argument("--clusters", action="store_true",
                        help="Показать все кластеры")
    parser.add_argument("--tree",     nargs="?", const="", metavar="ADDR",
                        help="Дерево кошельков (опц. фрагмент адреса мастера)")
    parser.add_argument("--alerts",   action="store_true",
                        help="Последние алерты")
    parser.add_argument("--status",   action="store_true",
                        help="Краткая сводка")
    parser.add_argument("--reset",    action="store_true",
                        help="Удалить и пересоздать БД")
    args = parser.parse_args()

    if not HELIUS_KEY:
        print(f"{R}❌ HELIUS_KEY не задан. Добавьте в .env{RST}")
        sys.exit(1)

    db_init()

    if args.reset:
        cmd_reset()
    elif args.add:
        cmd_add(args.add, args.label)
    elif args.init:
        cmd_init()
    elif args.test:
        cmd_test()
    elif args.watch:
        cmd_watch(args.interval)
    elif args.clusters:
        cmd_clusters()
    elif args.tree is not None:
        cmd_tree(args.tree)
    elif args.alerts:
        cmd_alerts()
    elif args.status:
        cmd_status()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
