#!/usr/bin/env python3
"""
Трассировка кошелька дева — куда ушли деньги.
Usage: python trace_wallet.py <WALLET_ADDRESS> [--ca <CA>]

Пример для MOMUS:
  python trace_wallet.py HdcrLZ2HcJkoEpMtw7XQKJ9b7YjHYfsNoiAQb1R1dzaK
  python trace_wallet.py 4nbS6VGx2yzPEVPkAPpUhPButrofWnXiyfUvRzfEkBrM
"""

import sys
import json
import argparse
import requests
from datetime import datetime, timezone
from collections import defaultdict

try:
    from free_solscan_api.api import send_api_request
except ImportError:
    print("❌ Установи: pip install git+https://github.com/paoloanzn/free-solscan-api.git")
    sys.exit(1)

try:
    from rich.console import Console
    from rich.table import Table
    from rich import box
    console = Console()
    RICH = True
except ImportError:
    RICH = False

RPC_URL = "https://api.mainnet-beta.solana.com"

KNOWN_ADDRS = {
    # CEX холодные кошельки
    "5tzFkiKscXHK5ZXCGbCAbZseha4ZRmHZmFeFiCNkGXTG": "Binance Hot",
    "AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2": "Binance",
    "2ojv9BAiHUrvsm9gxDe7fJSzbNZSJcxZvf8dqmWGHG8S": "Coinbase",
    "H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS": "Kraken",
    "FWznbcNXWQuHTawe9RxvQ2LdCENssh12dsznf4RiouN5": "OKX",
    # DEX/Bridge
    "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB": "Jupiter",
    "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP": "Orca",
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": "Whirlpool",
}

def rpc_call(method, params):
    try:
        r = requests.post(RPC_URL, json={
            "jsonrpc": "2.0", "id": 1, "method": method, "params": params
        }, timeout=15)
        return r.json().get("result")
    except:
        return None

def get_sol_balance(addr: str) -> float:
    res = rpc_call("getBalance", [addr])
    if res and "value" in res:
        return res["value"] / 1e9
    return 0.0

def get_transfers(addr: str, pages: int = 3, flow: str = None) -> list:
    """Получаем переводы через Solscan."""
    all_transfers = []
    for page in range(1, pages + 1):
        try:
            params = {
                "address": addr,
                "page": page,
                "page_size": 100,
                "exclude_amount_zero": "true",
                "sort_by": "block_time",
                "sort_order": "desc",
            }
            if flow:
                params["flow"] = flow

            data = send_api_request("/account/transfer", url_params=params)

            if isinstance(data, list):
                all_transfers.extend(data)
                if len(data) < 100:
                    break
            elif isinstance(data, dict) and "data" in data:
                all_transfers.extend(data["data"])
                if len(data["data"]) < 100:
                    break
            else:
                break
        except Exception as e:
            break
    return all_transfers

def get_defi_activity(addr: str) -> list:
    """DEX активность через Solscan."""
    try:
        data = send_api_request("/account/activity/dextrading", url_params={
            "address": addr, "page": 1, "page_size": 100
        })
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return data.get("data", [])
    except:
        pass
    return []

def categorize_address(addr: str) -> str:
    """Пытаемся определить тип адреса."""
    known = KNOWN_ADDRS.get(addr)
    if known:
        return f"[bold yellow]★ {known}[/bold yellow]" if RICH else f"★ {known}"
    return ""

def analyze_wallet(addr: str, ca: str | None = None):
    c = Console() if RICH else None

    print(f"\n{'═'*60}")
    print(f"🔎 Wallet: {addr}")
    print(f"{'═'*60}\n")

    sol = get_sol_balance(addr)
    known = KNOWN_ADDRS.get(addr, "")
    if known:
        print(f"⚠ ИЗВЕСТНЫЙ АДРЕС: {known}")
    print(f"💎 SOL баланс: {sol:.4f} SOL\n")

    # Все переводы (входящие + исходящие)
    print("⏳ Загружаю историю транзакций...\n")
    transfers = get_transfers(addr, pages=5)

    if not transfers:
        print("❌ Транзакции не найдены или API недоступен")
        print(f"🔗 Проверь вручную: https://solscan.io/account/{addr}")
        return

    # Агрегируем по получателю/отправителю
    outflow = defaultdict(lambda: {"sol": 0.0, "tokens": defaultdict(float), "count": 0})
    inflow = defaultdict(lambda: {"sol": 0.0, "tokens": defaultdict(float), "count": 0})

    raw_rows = []

    for tx in transfers:
        from_addr = tx.get("from_address", tx.get("sender", ""))
        to_addr = tx.get("to_address", tx.get("receiver", ""))
        amount = tx.get("amount", 0)
        token_dec = tx.get("token_decimals", 9)
        sym = tx.get("token_symbol", "SOL")
        ts = tx.get("block_time", 0)
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if ts else "?"

        try:
            val = float(amount) / (10 ** int(token_dec)) if amount else 0
        except:
            val = 0

        # Фильтр если нужен конкретный токен
        if ca and sym not in ("SOL", "WSOL") and tx.get("token_address", "") != ca:
            pass  # показываем всё

        is_out = str(from_addr).lower() == addr.lower()
        is_in = str(to_addr).lower() == addr.lower()

        direction = "→" if is_out else "←"
        counterpart = to_addr if is_out else from_addr

        raw_rows.append({
            "dir": direction,
            "counterpart": str(counterpart),
            "val": val,
            "sym": sym,
            "dt": dt,
            "ts": ts,
        })

        if is_out:
            if sym in ("SOL", "WSOL"):
                outflow[str(to_addr)]["sol"] += val
            else:
                outflow[str(to_addr)]["tokens"][sym] += val
            outflow[str(to_addr)]["count"] += 1
        elif is_in:
            if sym in ("SOL", "WSOL"):
                inflow[str(from_addr)]["sol"] += val
            else:
                inflow[str(from_addr)]["tokens"][sym] += val
            inflow[str(from_addr)]["count"] += 1

    # Сводка потоков
    print("━━━ ИСХОДЯЩИЕ (куда ушли деньги) ━━━")
    sorted_out = sorted(outflow.items(), key=lambda x: x[1]["sol"], reverse=True)

    if RICH:
        t = Table(box=box.SIMPLE, padding=(0,1))
        t.add_column("Адрес получателя", width=24)
        t.add_column("SOL", justify="right")
        t.add_column("Токены", width=22)
        t.add_column("Txns", justify="right")
        t.add_column("Известен?", width=18)

        for recv_addr, flow in sorted_out[:15]:
            sol_out = flow["sol"]
            tok_str = ", ".join(f"{v:.1f} {k}" for k, v in list(flow["tokens"].items())[:3])
            known_label = categorize_address(recv_addr) or "[dim]—[/dim]"
            sol_color = "red" if sol_out > 10 else ("yellow" if sol_out > 1 else "white")
            t.add_row(
                f"[dim]{recv_addr[:20]}…[/dim]",
                f"[{sol_color}]{sol_out:.2f}[/{sol_color}]",
                tok_str or "—",
                str(flow["count"]),
                known_label,
            )
        console.print(t)
    else:
        for recv_addr, flow in sorted_out[:15]:
            known = KNOWN_ADDRS.get(recv_addr, "")
            tok_str = ", ".join(f"{v:.1f} {k}" for k, v in flow["tokens"].items())
            print(f"  → {recv_addr[:22]}…  {flow['sol']:.3f} SOL  {tok_str}  [{flow['count']} txns]  {known}")

    # Детальный лог последних транзакций
    print(f"\n━━━ ДЕТАЛЬНЫЙ ЛОГ (последние {min(30, len(raw_rows))} txns) ━━━")
    if RICH:
        rt = Table(box=box.SIMPLE, padding=(0,1))
        rt.add_column("", width=2)
        rt.add_column("Адрес", width=24)
        rt.add_column("Сумма", justify="right", width=16)
        rt.add_column("Время", width=14)

        for row in sorted(raw_rows, key=lambda x: x["ts"], reverse=True)[:30]:
            known = KNOWN_ADDRS.get(row["counterpart"], "")
            addr_str = f"{row['counterpart'][:20]}…"
            if known:
                addr_str = f"[yellow]{addr_str} {known}[/yellow]"
            else:
                addr_str = f"[dim]{addr_str}[/dim]"

            color = "red" if row["dir"] == "→" else "green"
            rt.add_row(
                f"[{color}]{row['dir']}[/{color}]",
                addr_str,
                f"[{color}]{row['val']:.4f} {row['sym']}[/{color}]",
                f"[dim]{row['dt']}[/dim]"
            )
        console.print(rt)
    else:
        for row in sorted(raw_rows, key=lambda x: x["ts"], reverse=True)[:30]:
            known = KNOWN_ADDRS.get(row["counterpart"], "")
            print(f"  {row['dir']} {row['counterpart'][:22]}… {row['val']:.4f} {row['sym']}  {row['dt']}  {known}")

    # Итог
    total_out_sol = sum(v["sol"] for v in outflow.values())
    total_in_sol = sum(v["sol"] for v in inflow.values())
    print(f"\n━━━ ИТОГ ━━━")
    print(f"  Входящих SOL:  {total_in_sol:.3f}")
    print(f"  Исходящих SOL: {total_out_sol:.3f}")
    print(f"  Транзакций:    {len(raw_rows)}")

    print(f"\n🔗 https://solscan.io/account/{addr}")
    print(f"🔗 https://gmgn.ai/sol/address/{addr}")


def main():
    parser = argparse.ArgumentParser(description="Wallet Tracer")
    parser.add_argument("wallet", help="Адрес кошелька")
    parser.add_argument("--ca", help="CA токена (опционально)")
    args = parser.parse_args()

    analyze_wallet(args.wallet, args.ca)

if __name__ == "__main__":
    main()
