#!/usr/bin/env python3
"""
Solana Dev Wallet Monitor v2
════════════════════════════
Строит граф движения SOL от мастер-кошелька вглубь (до MAX_DEPTH уровней).
Алертит когда любой кошелёк в графе деплоит токен — с полной цепочкой.

Команды:
  python3 monitor.py --add <ADDR>          добавить мастер-кошелёк
  python3 monitor.py --init                 запомнить текущее состояние без алертов
  python3 monitor.py --test                 один цикл проверки
  python3 monitor.py --watch                непрерывный мониторинг (каждые N минут)
  python3 monitor.py --tree                 показать текущее дерево кошельков
  python3 monitor.py --alerts               последние алерты
  python3 monitor.py --reset                сбросить всё

Настройки в .env:
  HELIUS_KEY=...
  GMGN_API_KEY=...
  MONITOR_INTERVAL=5          минут между циклами (по умолчанию 5)
  MONITOR_MIN_SOL=0.1         минимальная сумма перевода для отслеживания
  MONITOR_MAX_DEPTH=5         максимальная глубина дерева
"""

from __future__ import annotations
import os, sys, json, time, uuid, asyncio, sqlite3, argparse, textwrap
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

HELIUS_KEY    = os.environ.get("HELIUS_KEY", "")
GMGN_API_KEY  = os.environ.get("GMGN_API_KEY", "")
INTERVAL_MIN  = int(os.environ.get("MONITOR_INTERVAL", "5"))
MIN_SOL       = float(os.environ.get("MONITOR_MIN_SOL", "0.1"))
MAX_DEPTH     = int(os.environ.get("MONITOR_MAX_DEPTH", "5"))
CHAIN         = "sol"
GMGN_BASE     = "https://openapi.gmgn.ai"

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "monitor.db")

# ── Системные программы (игнорируем) ─────────────────────────────────────────
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
    "5tzFkiKscXHK5ZXCGbCAbZseha4ZRmHZmFeFiCNkGXTG",
    "AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2",
    "FWznbcNXWQuHTawe9RxvQ2LdCENssh12dsznf4RiouN5",
    "9un5wqE3q4oCjyrDkwsdD48KteCJitQX5978Vh7KKxHo",
    "GJRs4FwHtemZ5ZE9x3FNvJ8TMwitKTh21yxdRPqn7npE",
    "2ojv9BAiHUrvsm9gxDe7fJSzbNZSJcxZvf8dqmWGHG8S",
    "H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS",
    "rFqFJ9g7TGBD8Ed7TPDnvGKZ5pWLPDyxLcvcH2eRCtt",  # Raydium authority
    "TokenzQdBNbequvydDktM2rGPSZQfpzbR87YxMd6eTB",   # Token-2022
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
    with db_connect() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS masters (
            address     TEXT PRIMARY KEY,
            label       TEXT DEFAULT '',
            added_at    INTEGER,
            active      INTEGER DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS wallets (
            address         TEXT PRIMARY KEY,
            master          TEXT,           -- мастер-кошелёк корня дерева
            depth           INTEGER,        -- уровень: 0=мастер, 1=прямой получатель, ...
            parent          TEXT,           -- кто отправил SOL на этот кошелёк
            path_json       TEXT,           -- JSON массив адресов от мастера до этого кошелька
            amount_received REAL,           -- сколько SOL получил от родителя
            first_seen      INTEGER,
            last_sig        TEXT,           -- последняя обработанная подпись (для инкрементальных проверок)
            last_tx_scan    INTEGER DEFAULT 0,  -- когда последний раз сканировали исходящие
            last_tok_scan   INTEGER DEFAULT 0,  -- когда последний раз проверяли токены
            init_mode       INTEGER DEFAULT 0   -- 1 = кошелёк добавлен в --init режиме, не алертить
        );

        CREATE TABLE IF NOT EXISTS transfers (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            from_addr   TEXT,
            to_addr     TEXT,
            amount_sol  REAL,
            ts          INTEGER,
            sig         TEXT UNIQUE,
            depth_from  INTEGER,
            master      TEXT
        );

        CREATE TABLE IF NOT EXISTS tokens (
            ca          TEXT PRIMARY KEY,
            deployer    TEXT,
            master      TEXT,
            name        TEXT,
            symbol      TEXT,
            launch_ts   INTEGER,
            market_cap  REAL,
            chain_path  TEXT,   -- JSON массив адресов цепочки
            depth       INTEGER,
            alerted     INTEGER DEFAULT 0,
            found_at    INTEGER
        );

        CREATE TABLE IF NOT EXISTS alerts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            type        TEXT,
            master      TEXT,
            data_json   TEXT,
            message     TEXT,
            ts          INTEGER
        );
        """)

# ── Helius RPC (синхронный — оборачиваем в executor для asyncio) ─────────────
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

def _gmgn_get(path: str, params: dict = None):
    p = {"timestamp": now(), "client_id": str(uuid.uuid4())}
    if params:
        p.update(params)
    try:
        r = requests.get(
            f"{GMGN_BASE}{path}",
            headers={"X-APIKEY": GMGN_API_KEY},
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
    d = _gmgn_get("/v1/user/created_tokens", {"chain": CHAIN, "wallet_address": wallet})
    if isinstance(d, list): return d
    if isinstance(d, dict): return d.get("tokens", d.get("data", []))
    return []

# ── Парсинг исходящих SOL-переводов ──────────────────────────────────────────
def _fetch_outgoing(wallet: str, last_sig: Optional[str]) -> tuple[list, Optional[str]]:
    """
    Возвращает (transfers, newest_sig).
    transfers = [{to, amount_sol, ts, sig}, ...]
    """
    sig_infos = []
    before_cursor = None

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

    transfers = []
    seen_sigs = set()

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
            if frm == wallet and to and to not in SKIP_ADDRS and amt >= MIN_SOL:
                transfers.append({"to": to, "amount_sol": amt, "ts": ts, "sig": sig})
                found = True

        # Метод 2: accountData fallback
        if not found:
            acct_data = tx.get("accountData") or []
            src_chg = 0.0
            for ad in acct_data:
                if ad.get("account") == wallet:
                    src_chg = float(ad.get("nativeBalanceChange") or 0) / 1e9
                    break
            if src_chg < -MIN_SOL:
                best_to, best_amt = "", 0.0
                for ad in acct_data:
                    acc = ad.get("account") or ""
                    if acc == wallet or acc in SKIP_ADDRS:
                        continue
                    chg = float(ad.get("nativeBalanceChange") or 0) / 1e9
                    if chg >= MIN_SOL and chg > best_amt:
                        best_to, best_amt = acc, chg
                if best_to:
                    transfers.append({"to": best_to, "amount_sol": best_amt, "ts": ts, "sig": sig})

    return transfers, newest_sig

# ── Async обёртки ─────────────────────────────────────────────────────────────
_executor = ThreadPoolExecutor(max_workers=20)

async def async_fetch_outgoing(wallet: str, last_sig: Optional[str]):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _fetch_outgoing, wallet, last_sig)

async def async_gmgn_created(wallet: str):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _gmgn_created, wallet)

# ── Основная логика одного цикла ─────────────────────────────────────────────
async def scan_wallet_transfers(row: sqlite3.Row, conn: sqlite3.Connection,
                                 init_mode: bool = False) -> list[dict]:
    """
    Сканирует исходящие SOL переводы для одного кошелька.
    Возвращает список новых получателей [{address, depth, parent, path, amount, ts, sig}].
    """
    wallet   = row["address"]
    depth    = row["depth"]
    master   = row["master"]
    last_sig = row["last_sig"]
    path     = json.loads(row["path_json"] or "[]")

    if depth >= MAX_DEPTH:
        return []

    transfers, newest_sig = await async_fetch_outgoing(wallet, last_sig)

    # Обновляем last_sig и время сканирования
    conn.execute(
        "UPDATE wallets SET last_sig=?, last_tx_scan=? WHERE address=?",
        (newest_sig or last_sig, now(), wallet)
    )

    new_wallets = []
    for t in transfers:
        to_addr = t["to"]

        # Уже в БД?
        existing = conn.execute(
            "SELECT address FROM wallets WHERE address=?", (to_addr,)
        ).fetchone()
        if existing:
            # Обновляем сумму в transfers если новая подпись
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO transfers "
                    "(from_addr,to_addr,amount_sol,ts,sig,depth_from,master) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (wallet, to_addr, t["amount_sol"], t["ts"], t["sig"], depth, master)
                )
            except Exception:
                pass
            continue

        # Новый кошелёк!
        new_path = path + [to_addr]
        conn.execute(
            "INSERT OR IGNORE INTO wallets "
            "(address,master,depth,parent,path_json,amount_received,first_seen,last_sig,init_mode) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (to_addr, master, depth+1, wallet,
             json.dumps(new_path), t["amount_sol"],
             t["ts"] or now(), None,
             1 if init_mode else 0)
        )
        try:
            conn.execute(
                "INSERT OR IGNORE INTO transfers "
                "(from_addr,to_addr,amount_sol,ts,sig,depth_from,master) "
                "VALUES (?,?,?,?,?,?,?)",
                (wallet, to_addr, t["amount_sol"], t["ts"], t["sig"], depth, master)
            )
        except Exception:
            pass

        new_wallets.append({
            "address": to_addr,
            "depth":   depth + 1,
            "parent":  wallet,
            "path":    new_path,
            "amount":  t["amount_sol"],
            "ts":      t["ts"],
            "sig":     t["sig"],
            "init_mode": init_mode,
        })

    conn.commit()
    return new_wallets


async def scan_wallet_tokens(row: sqlite3.Row, conn: sqlite3.Connection) -> list[dict]:
    """
    Проверяет задеплоенные токены для одного кошелька.
    Возвращает список новых токенов.
    """
    # Не проверяем чаще раза в 8 минут
    if now() - (row["last_tok_scan"] or 0) < 480:
        return []

    wallet = row["address"]
    master = row["master"]
    path   = json.loads(row["path_json"] or "[]")
    depth  = row["depth"]

    tokens = await async_gmgn_created(wallet)
    conn.execute("UPDATE wallets SET last_tok_scan=? WHERE address=?", (now(), wallet))

    new_tokens = []
    for tok in (tokens or []):
        ca = (tok.get("address") or tok.get("mint") or
              tok.get("token_address") or "")
        if not ca:
            continue

        existing = conn.execute(
            "SELECT ca FROM tokens WHERE ca=?", (ca,)
        ).fetchone()
        if existing:
            continue

        name     = tok.get("name")   or "?"
        symbol   = tok.get("symbol") or "?"
        launch   = int(tok.get("open_timestamp") or tok.get("created_at") or 0)
        mc       = float(tok.get("market_cap") or tok.get("usd_market_cap") or 0)
        in_init  = row["init_mode"] == 1

        conn.execute(
            "INSERT OR IGNORE INTO tokens "
            "(ca,deployer,master,name,symbol,launch_ts,market_cap,"
            " chain_path,depth,alerted,found_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (ca, wallet, master, name, symbol, launch, mc,
             json.dumps(path), depth,
             1 if in_init else 0,   # если init — помечаем уже "alerted" чтобы не спамить
             now())
        )
        conn.commit()

        if not in_init:
            new_tokens.append({
                "ca": ca, "name": name, "symbol": symbol,
                "launch_ts": launch, "market_cap": mc,
                "deployer": wallet, "master": master,
                "path": path, "depth": depth,
            })

    conn.commit()
    return new_tokens

# ── Один полный цикл ──────────────────────────────────────────────────────────
async def run_cycle(init_mode: bool = False, silent: bool = False) -> dict:
    conn = db_connect()
    stats = {"new_wallets": 0, "new_tokens": 0, "alerts": []}

    # Все активные кошельки в дереве
    wallets = conn.execute(
        "SELECT w.* FROM wallets w "
        "JOIN masters m ON w.master = m.address "
        "WHERE m.active=1 "
        "ORDER BY w.depth ASC"
    ).fetchall()

    if not wallets:
        if not silent:
            log("Нет кошельков для мониторинга. Добавьте мастер: --add <ADDR>")
        conn.close()
        return stats

    if not silent:
        log(f"Сканирую {len(wallets)} кошельков (глубина 0–{MAX_DEPTH})…")

    # ── Параллельно сканируем переводы ────────────────────────────────────
    tx_tasks = [scan_wallet_transfers(w, conn, init_mode) for w in wallets]
    tx_results = await asyncio.gather(*tx_tasks, return_exceptions=True)

    new_wallet_addrs = []
    for result in tx_results:
        if isinstance(result, Exception):
            continue
        for nw in result:
            stats["new_wallets"] += 1
            new_wallet_addrs.append(nw)
            if not init_mode and not silent:
                depth_str = "→ " * nw["depth"]
                log(f"  {depth_str}[lvl {nw['depth']}] {short(nw['parent'])} "
                    f"→ {short(nw['address'])}  "
                    f"{nw['amount']:.3f} SOL  {ts_fmt(nw['ts'])}")

    # ── Перечитываем wallets (добавились новые) ────────────────────────────
    wallets = conn.execute(
        "SELECT w.* FROM wallets w "
        "JOIN masters m ON w.master = m.address "
        "WHERE m.active=1"
    ).fetchall()

    # ── Параллельно проверяем токены ───────────────────────────────────────
    tok_tasks = [scan_wallet_tokens(w, conn) for w in wallets]
    tok_results = await asyncio.gather(*tok_tasks, return_exceptions=True)

    # Загружаем метки мастеров для алертов
    master_labels = {}
    for row in conn.execute("SELECT address, label FROM masters").fetchall():
        master_labels[row["address"]] = row["label"] or ""

    for result in tok_results:
        if isinstance(result, Exception):
            continue
        for tok in result:
            stats["new_tokens"] += 1
            tok["master_label"] = master_labels.get(tok["master"], "")
            msg = _format_token_alert(tok)
            stats["alerts"].append((msg, tok))
            # Сохраняем алерт в БД
            conn.execute(
                "INSERT INTO alerts (type,master,data_json,message,ts) VALUES (?,?,?,?,?)",
                ("new_token", tok["master"], json.dumps(tok), msg, now())
            )
            conn.execute(
                "UPDATE tokens SET alerted=1 WHERE ca=?", (tok["ca"],)
            )
            conn.commit()

    conn.close()
    return stats

# ── Форматирование алерта ─────────────────────────────────────────────────────
def _format_token_alert(tok: dict) -> str:
    path         = tok.get("path") or []
    depth        = tok.get("depth", 0)
    master       = tok.get("master", "")
    master_label = tok.get("master_label") or ""
    mc           = tok.get("market_cap") or 0
    mc_str       = f"${mc:,.0f}" if mc else "неизвестно"
    symbol       = tok.get("symbol") or "?"
    name         = tok.get("name") or "?"
    ca           = tok.get("ca") or ""
    launch       = tok.get("launch_ts") or 0

    # Заголовок с именем мастера
    dev_name = master_label or short(master)
    header   = f"🚀 НОВЫЙ ДЕПЛОЙ — {dev_name}"

    # Строим цепочку: МАСТЕР → lvl1 → lvl2 → ДЕПЛОЕР
    chain_parts = []
    for i, addr in enumerate(path):
        if i == 0:
            lbl = master_label or "МАСТЕР"
        elif i == len(path) - 1:
            lbl = "ДЕПЛОЕР"
        else:
            lbl = f"lvl{i}"
        chain_parts.append(f"{short(addr)}[{lbl}]")
    chain_str = " → ".join(chain_parts) if chain_parts else short(master)

    hops = max(0, len(path) - 2)  # не считаем мастер и деплоер
    if hops == 0:
        hops_str = "прямой деплой"
    elif hops == 1:
        hops_str = "1 прослойка"
    elif 2 <= hops <= 4:
        hops_str = f"{hops} прослойки"
    else:
        hops_str = f"{hops} прослоек"

    lines = [
        header,
        "─" * len(header),
        f"Цепочка : {chain_str}",
        f"Глубина : {hops_str}",
        f"",
        f"Токен   : {symbol} / {name}",
        f"Запуск  : {ts_fmt(launch)}",
        f"МКап    : {mc_str}",
        f"",
        f"▶ Токен   : https://gmgn.ai/sol/token/{ca}",
        f"▶ Деплоер : https://gmgn.ai/sol/address/{tok.get('deployer','')}",
        f"▶ Мастер  : https://gmgn.ai/sol/address/{master}",
    ]
    return "\n".join(lines)


def _trigger_analyze(ca: str, master_label: str = ""):
    """
    Запускает analyze.py на найденный токен в фоне.
    Вывод идёт в тот же поток (терминал / лог-файл).
    """
    import subprocess
    analyze_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "analyze.py")
    if not os.path.exists(analyze_path):
        log(f"  analyze.py не найден рядом со скриптом, пропускаю автоанализ")
        return
    label_str = f" ({master_label})" if master_label else ""
    print(f"\n{C}{'─'*62}{RST}")
    print(f"{C}▶ Запускаю analyze.py для {ca[:16]}…{label_str}{RST}")
    print(f"{C}{'─'*62}{RST}")
    try:
        # Запускаем синхронно чтобы вывод шёл в тот же лог
        subprocess.run(
            [sys.executable, analyze_path, ca],
            timeout=120,
        )
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

# ── Команды CLI ───────────────────────────────────────────────────────────────
def cmd_add(addr: str, label: str = ""):
    db_init()
    with db_connect() as conn:
        existing = conn.execute("SELECT address FROM masters WHERE address=?", (addr,)).fetchone()
        if existing:
            print(f"{Y}Уже отслеживается: {addr}{RST}")
            return
        conn.execute(
            "INSERT INTO masters (address,label,added_at,active) VALUES (?,?,?,1)",
            (addr, label, now())
        )
        # Добавляем сам мастер как уровень 0
        conn.execute(
            "INSERT OR IGNORE INTO wallets "
            "(address,master,depth,parent,path_json,amount_received,first_seen,last_sig,init_mode) "
            "VALUES (?,?,0,NULL,?,0,?,NULL,0)",
            (addr, addr, json.dumps([addr]), now())
        )
        conn.commit()
    print(f"{G}✓ Мастер добавлен: {addr}{RST}")
    print(f"  Запусти {BOLD}--init{RST} чтобы запомнить текущее состояние без алертов")
    print(f"  Затем {BOLD}--watch{RST} для мониторинга")


def cmd_init():
    """Сканирует текущее состояние и запоминает без алертов."""
    print(f"\n{BOLD}🔄 INIT — запоминаем текущее состояние (без алертов){RST}")
    print(f"Это может занять 1–2 минуты…\n")
    stats = asyncio.run(run_cycle(init_mode=True, silent=False))
    print(f"\n{G}✓ Init завершён.{RST}")
    print(f"  Новых кошельков добавлено : {stats['new_wallets']}")
    print(f"  Токенов запомнено         : (без алертов)")
    print(f"\nТеперь запускай: {BOLD}python3 monitor.py --watch{RST}")


def cmd_test():
    """Один цикл проверки с выводом всего."""
    print(f"\n{BOLD}🔍 TEST — один цикл проверки{RST}\n")
    stats = asyncio.run(run_cycle(init_mode=False, silent=False))
    print(f"\n{G}─── Результат ───{RST}")
    print(f"  Новых кошельков : {stats['new_wallets']}")
    print(f"  Новых токенов   : {stats['new_tokens']}")
    for msg, tok in stats["alerts"]:
        _print_alert(msg)
        _trigger_analyze(tok["ca"], tok.get("master_label", ""))
    if not stats["alerts"]:
        print(f"  Алертов         : 0  (всё тихо)")


def cmd_watch(interval: int = INTERVAL_MIN):
    print(f"\n{BOLD}{'═'*62}{RST}")
    print(f"{BOLD}  🔭 МОНИТОРИНГ ЗАПУЩЕН{RST}")
    print(f"{'═'*62}")
    with db_connect() as conn:
        masters = conn.execute("SELECT address,label FROM masters WHERE active=1").fetchall()
        wallets_cnt = conn.execute("SELECT COUNT(*) FROM wallets").fetchone()[0]
    print(f"  Мастеров    : {len(masters)}")
    for m in masters:
        lbl = f" ({m['label']})" if m['label'] else ""
        print(f"    → {m['address']}{lbl}")
    print(f"  В дереве    : {wallets_cnt} кошельков")
    print(f"  Интервал    : {interval} мин")
    print(f"  MIN_SOL     : {MIN_SOL} SOL")
    print(f"  MAX_DEPTH   : {MAX_DEPTH} уровней")
    print(f"{'═'*62}\n")

    cycle = 0
    while True:
        cycle += 1
        log(f"{'─'*50} цикл #{cycle}")
        try:
            stats = asyncio.run(run_cycle(init_mode=False, silent=False))
            for msg, tok in stats["alerts"]:
                _print_alert(msg)
                _trigger_analyze(tok["ca"], tok.get("master_label", ""))
            if stats["new_wallets"] == 0 and stats["new_tokens"] == 0:
                log(f"Изменений нет.")
        except KeyboardInterrupt:
            print(f"\n{Y}Мониторинг остановлен.{RST}")
            break
        except Exception as e:
            log(f"{R}Ошибка цикла: {e}{RST}")
        log(f"Следующая проверка через {interval} мин. Ctrl+C для остановки.")
        try:
            time.sleep(interval * 60)
        except KeyboardInterrupt:
            print(f"\n{Y}Мониторинг остановлен.{RST}")
            break


def cmd_tree():
    """Показать дерево кошельков."""
    with db_connect() as conn:
        masters = conn.execute("SELECT * FROM masters WHERE active=1").fetchall()
        if not masters:
            print("Нет активных мастеров. Добавь: --add <ADDR>")
            return

        for master in masters:
            maddr = master["address"]
            lbl   = master["label"] or short(maddr)
            wallets = conn.execute(
                "SELECT * FROM wallets WHERE master=? ORDER BY depth,first_seen",
                (maddr,)
            ).fetchall()
            tokens = conn.execute(
                "SELECT * FROM tokens WHERE master=? ORDER BY found_at DESC",
                (maddr,)
            ).fetchall()

            print(f"\n{BOLD}{'═'*62}{RST}")
            print(f"{BOLD}  МАСТЕР: {maddr}{RST}")
            print(f"  Метка : {lbl}")
            print(f"  Всего кошельков в дереве: {len(wallets)}")
            print(f"  Всего токенов найдено   : {len(tokens)}")
            print(f"{BOLD}{'─'*62}{RST}")

            # Группируем по уровням
            by_depth: dict = {}
            for w in wallets:
                d = w["depth"]
                by_depth.setdefault(d, []).append(w)

            for depth in sorted(by_depth.keys()):
                group = by_depth[depth]
                label = "МАСТЕР" if depth == 0 else f"Уровень {depth}"
                print(f"\n  {C}{label}{RST} ({len(group)} кошельков)")
                for w in group[:20]:
                    # Есть ли токены от этого кошелька?
                    tok_count = conn.execute(
                        "SELECT COUNT(*) FROM tokens WHERE deployer=?",
                        (w["address"],)
                    ).fetchone()[0]
                    tok_str = f"  {G}🚀 {tok_count} токен(ов){RST}" if tok_count else ""
                    indent  = "  " + "  " * depth
                    print(f"{indent}{short(w['address'])}"
                          f"  {DIM}{w['amount_received']:.3f} SOL  "
                          f"{ts_fmt(w['first_seen'])}{RST}{tok_str}")
                if len(group) > 20:
                    print(f"  {'  '*depth}  {DIM}…ещё {len(group)-20}{RST}")

            if tokens:
                print(f"\n  {Y}ТОКЕНЫ:{RST}")
                for tok in tokens[:10]:
                    mc = tok["market_cap"] or 0
                    mc_str = f"${mc:,.0f}" if mc else "—"
                    path = json.loads(tok["chain_path"] or "[]")
                    chain = " → ".join(short(a) for a in path)
                    print(f"  🚀 {tok['symbol']:8}  {mc_str:>12}  "
                          f"{ts_fmt(tok['launch_ts'])}  {DIM}{chain}{RST}")


def cmd_alerts(n: int = 20):
    """Показать последние алерты."""
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
        print(f"{G}✓ База данных удалена: {DB_PATH}{RST}")
    db_init()
    print(f"{G}✓ Новая БД создана.{RST}")


def cmd_status():
    """Краткая сводка."""
    with db_connect() as conn:
        masters  = conn.execute("SELECT COUNT(*) FROM masters WHERE active=1").fetchone()[0]
        wallets  = conn.execute("SELECT COUNT(*) FROM wallets").fetchone()[0]
        tokens   = conn.execute("SELECT COUNT(*) FROM tokens").fetchone()[0]
        alerts   = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        by_depth = conn.execute(
            "SELECT depth, COUNT(*) as cnt FROM wallets GROUP BY depth ORDER BY depth"
        ).fetchall()

    print(f"\n{BOLD}СТАТУС МОНИТОРА{RST}")
    print(f"  Мастеров   : {masters}")
    print(f"  Кошельков  : {wallets}")
    print(f"  Токенов    : {tokens}")
    print(f"  Алертов    : {alerts}")
    print(f"  По уровням :")
    for row in by_depth:
        bar = "█" * min(row["cnt"], 40)
        print(f"    lvl {row['depth']} : {row['cnt']:4d}  {bar}")

# ── main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Solana Dev Wallet Monitor v2",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
        Быстрый старт:
          python3 monitor.py --add D8cJRpXaCWVK8c3doDq7Ymoz2XE4WyhFhbgNytWwqptA
          python3 monitor.py --init
          python3 monitor.py --watch
        """)
    )
    parser.add_argument("--add",      metavar="ADDR",  help="Добавить мастер-кошелёк")
    parser.add_argument("--label",    metavar="LABEL", default="", help="Метка для мастера")
    parser.add_argument("--init",     action="store_true", help="Запомнить текущее состояние без алертов")
    parser.add_argument("--test",     action="store_true", help="Один цикл проверки")
    parser.add_argument("--watch",    action="store_true", help="Непрерывный мониторинг")
    parser.add_argument("--interval", type=int, default=INTERVAL_MIN, help="Минут между циклами")
    parser.add_argument("--tree",     action="store_true", help="Показать дерево кошельков")
    parser.add_argument("--alerts",   action="store_true", help="Последние алерты")
    parser.add_argument("--status",   action="store_true", help="Краткая сводка")
    parser.add_argument("--reset",    action="store_true", help="Сбросить всю БД")
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
    elif args.tree:
        cmd_tree()
    elif args.alerts:
        cmd_alerts()
    elif args.status:
        cmd_status()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
