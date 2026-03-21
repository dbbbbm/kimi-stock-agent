#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票自动调度器 - 交互式终端仪表盘
功能：
  - 每 N 秒拉取最新股价（交易时间内）
  - 持仓股价格波动超阈值时自动触发 /operate
  - 09:30 开盘自动触发 /operate
  - 15:10 收盘触发 /daily_archive
  - 周五 15:15 触发 /weekly_archive
  - 实时展示股票指标、触发事件、claude 输出
"""

import json
import os
import subprocess
import sys
import threading
import time
from collections import deque
from datetime import datetime, time as dtime
from pathlib import Path

import schedule
import pytz
from rich import box
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

# ─── Paths ────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
CONFIG_FILE = BASE_DIR / "scheduler_config.json"
STATE_FILE  = BASE_DIR / "scheduler_state.json"
STOCKS_FILE = BASE_DIR / "stocks.xlsx"
MY_MD_FILE  = BASE_DIR / "MY.md"

# ─── Shared state (lock-protected) ────────────────────────────────────────────

_lock = threading.Lock()

stock_rows: list      = []          # latest rows from stocks.xlsx
event_log: deque      = deque(maxlen=60)
claude_buf: deque     = deque(maxlen=120)
is_running: bool      = False
current_cmd: str      = ""
last_fetch_time: str  = "—"
last_fetch_ok: bool   = True
market_status: str    = "启动中"
last_operate_dt: datetime | None = None
portfolio_codes: list = []          # refreshed every fetch cycle
positions_cache: dict = {}          # {code: {name, shares, cost}} from MY.md
available_cash: float = 0.0         # parsed from MY.md 资金情况
INITIAL_CAPITAL: float = 1_000_000.0


# ─── Config ───────────────────────────────────────────────────────────────────

def load_config() -> dict:
    defaults = {
        "price_move_threshold_pct": 2.0,
        "operate_cooldown_minutes": 30,
        "fetch_interval_seconds": 60,
        "market_open": "09:30",
        "market_close": "15:00",
        "archive_time": "15:10",
        "weekly_archive_time": "15:15",
        "timezone": "Asia/Shanghai",
    }
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, encoding="utf-8") as f:
                return {**defaults, **json.load(f)}
        except Exception:
            pass
    return defaults


# ─── State persistence ────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"last_prices": {}}


def save_state(state: dict) -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


# ─── Helpers ──────────────────────────────────────────────────────────────────

def now_tz(tz_name: str) -> datetime:
    return datetime.now(pytz.timezone(tz_name))


def log_event(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    with _lock:
        event_log.appendleft(f"[{ts}] {msg}")


def append_claude(line: str) -> None:
    with _lock:
        claude_buf.append(line.rstrip("\n\r"))


def is_market_open(cfg: dict) -> bool:
    tz = pytz.timezone(cfg["timezone"])
    now = datetime.now(tz)
    if now.weekday() >= 5:
        return False
    t = now.time()
    oh, om = map(int, cfg["market_open"].split(":"))
    ch, cm = map(int, cfg["market_close"].split(":"))
    return dtime(oh, om) <= t <= dtime(ch, cm)


def refresh_portfolio_codes() -> list:
    """Parse MY.md to get 6-digit stock codes in the 持仓明细 section only."""
    codes = []
    try:
        content = MY_MD_FILE.read_text(encoding="utf-8")
        in_section = False
        for line in content.splitlines():
            if "持仓明细" in line:
                in_section = True
                continue
            # Stop at the next ## heading
            if in_section and line.startswith("##"):
                break
            if not in_section or not line.startswith("|"):
                continue
            parts = [p.strip() for p in line.split("|")]
            if len(parts) > 1 and parts[1].isdigit() and len(parts[1]) == 6:
                if parts[1] not in codes:
                    codes.append(parts[1])
    except Exception:
        pass
    return codes


def parse_my_md() -> tuple:
    """Parse MY.md, return (positions_dict, available_cash).
    positions_dict: {code: {name, shares, cost}}
    """
    positions: dict = {}
    cash: float = 0.0
    try:
        content = MY_MD_FILE.read_text(encoding="utf-8")
        in_positions = False
        in_funds = False
        for line in content.splitlines():
            if "持仓明细" in line and line.startswith("#"):
                in_positions, in_funds = True, False
                continue
            if "资金情况" in line and line.startswith("#"):
                in_funds, in_positions = True, False
                continue
            if line.startswith("##"):
                in_positions, in_funds = False, False
                continue

            if in_positions and line.startswith("|"):
                parts = [p.strip() for p in line.split("|")]
                if len(parts) < 6:
                    continue
                code = parts[1]
                if not (code.isdigit() and len(code) == 6):
                    continue
                name = parts[2]
                shares_raw = parts[3].replace("股", "").replace(",", "")
                cost_raw   = parts[4]
                try:
                    positions[code] = {
                        "name": name,
                        "shares": int(shares_raw),
                        "cost": float(cost_raw),
                    }
                except ValueError:
                    pass

            if in_funds and "可用现金" in line and "|" in line:
                parts = [p.strip() for p in line.split("|")]
                for p in parts:
                    val = p.replace(",", "").replace("**", "").strip()
                    try:
                        cash = float(val)
                        break
                    except ValueError:
                        pass
    except Exception:
        pass
    return positions, cash


def read_stocks_xlsx() -> list:
    """Run xlsx_to_json.py, return list of row dicts."""
    try:
        result = subprocess.run(
            [sys.executable, "-X", "utf8", str(BASE_DIR / "xlsx_to_json.py"), str(STOCKS_FILE)],
            capture_output=True, text=True, encoding="utf-8",
            errors="replace", cwd=str(BASE_DIR), timeout=30,
        )
        print(result)
        if result.returncode == 0:
            data = json.loads(result.stdout)
            return data.get("data", [])
    except Exception as e:
        print(e)
    return []


def is_trading_day(rows: list, tz_name: str) -> bool:
    today = now_tz(tz_name).strftime("%Y-%m-%d")
    for row in rows:
        val = str(row.get("更新日期") or row.get("更新时间") or "")
        if val[:10] == today:
            return True
    return False


# ─── Claude runner ────────────────────────────────────────────────────────────

def run_claude_command(cmd: str, cfg: dict) -> None:
    """Run a claude slash command in a background thread, streaming output to UI."""
    global is_running, current_cmd, last_operate_dt

    with _lock:
        if is_running:
            log_event(f"跳过 {cmd}（已有任务运行中）")
            return
        if "/operate" in cmd and last_operate_dt is not None:
            cooldown_s = cfg["operate_cooldown_minutes"] * 60
            elapsed = (datetime.now() - last_operate_dt).total_seconds()
            if elapsed < cooldown_s:
                rem = int(cooldown_s - elapsed)
                log_event(f"跳过 {cmd}（冷却 {rem // 60}分{rem % 60}秒）")
                return
        is_running = True
        current_cmd = cmd
        claude_buf.clear()

    log_event(f"启动 → {cmd}")
    append_claude(f"{'─'*60}")
    append_claude(f"▶  {cmd}  [{datetime.now().strftime('%H:%M:%S')}]")
    append_claude(f"{'─'*60}")

    try:
        proc = subprocess.Popen(
            ["claude", "-p", cmd],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(BASE_DIR),
        )
        for line in proc.stdout:
            append_claude(line)
        proc.wait()
        code = proc.returncode
        append_claude(f"{'─'*60}")
        append_claude(f"■  完成  退出码={code}  [{datetime.now().strftime('%H:%M:%S')}]")
        log_event(f"{cmd} 完成（exit={code}）")
    except FileNotFoundError:
        append_claude("✗  找不到 claude 命令 —— 请确认 Claude Code CLI 已安装并在 PATH 中")
        log_event(f"{cmd} 失败：找不到 claude")
    except Exception as e:
        append_claude(f"✗  错误：{e}")
        log_event(f"{cmd} 出错：{e}")
    finally:
        with _lock:
            is_running = False
            current_cmd = ""
            if "/operate" in cmd:
                last_operate_dt = datetime.now()


def trigger_operate(cfg: dict, reason: str) -> None:
    log_event(reason)
    threading.Thread(
        target=run_claude_command, args=("/operate", cfg), daemon=True
    ).start()


def trigger_archive(cfg: dict, cmd: str, reason: str) -> None:
    log_event(reason)
    threading.Thread(
        target=run_claude_command, args=(cmd, cfg), daemon=True
    ).start()


# ─── Scheduled jobs ───────────────────────────────────────────────────────────

def job_fetch_and_check(cfg: dict, state: dict) -> None:
    global stock_rows, last_fetch_time, last_fetch_ok, market_status, portfolio_codes, positions_cache, available_cash

    if not is_market_open(cfg):
        with _lock:
            market_status = "休市"
        return

    with _lock:
        market_status = "交易中"

    # 1. fetch
    try:
        result = subprocess.run(
            [sys.executable, "-X", "utf8", str(BASE_DIR / "fetch_stocks.py")],
            capture_output=True, text=True, encoding="utf-8",
            errors="replace", cwd=str(BASE_DIR), timeout=120,
        )
        ok = result.returncode == 0
    except Exception as e:
        log_event(f"拉取失败：{e}")
        with _lock:
            last_fetch_ok = False
        return

    with _lock:
        last_fetch_ok = ok

    if not ok:
        err_line = (result.stderr or result.stdout or "").strip().splitlines()
        err_hint = err_line[-1][:80] if err_line else "未知错误"
        log_event(f"fetch 失败：{err_hint}")
        return

    # 2. read xlsx
    rows = read_stocks_xlsx()
    if not rows:
        log_event("读取 xlsx 失败")
        return

    # 3. refresh portfolio codes, positions and stock display
    new_portfolio = refresh_portfolio_codes()
    new_positions, new_cash = parse_my_md()
    with _lock:
        stock_rows = rows
        portfolio_codes = new_portfolio
        positions_cache = new_positions
        available_cash = new_cash
        last_fetch_time = datetime.now().strftime("%H:%M:%S")

    log_event(f"行情更新（{len(rows)} 只，{len(new_portfolio)} 只持仓）")

    # 4. trading day check
    if not is_trading_day(rows, cfg["timezone"]):
        with _lock:
            market_status = "非交易日"
        log_event("非交易日，跳过触发检测")
        return

    # 5. price movement detection (portfolio stocks only)
    threshold = cfg["price_move_threshold_pct"]
    triggered = []
    for row in rows:
        code = str(row.get("股票代码", "")).zfill(6)
        if code not in new_portfolio:
            continue
        price = row.get("现价")
        last  = state["last_prices"].get(code)
        if price and last and last > 0:
            chg = abs((price - last) / last * 100)
            if chg >= threshold:
                triggered.append(f"{code}({chg:+.1f}%)")

    # 6. update price snapshot
    for row in rows:
        code = str(row.get("股票代码", "")).zfill(6)
        if row.get("现价"):
            state["last_prices"][code] = row["现价"]
    save_state(state)

    # 7. trigger
    if triggered:
        trigger_operate(cfg, f"波动触发 {', '.join(triggered)}，执行 /operate")


def job_market_open(cfg: dict) -> None:
    trigger_operate(cfg, "开盘触发 /operate")


def job_daily_archive(cfg: dict) -> None:
    trigger_archive(cfg, "/daily_archive", "收盘触发 /daily_archive")


def job_weekly_archive(cfg: dict) -> None:
    trigger_archive(cfg, "/weekly_archive", "周五收盘触发 /weekly_archive")


# ─── UI rendering ─────────────────────────────────────────────────────────────

def build_header(cfg: dict) -> Panel:
    tz_str = now_tz(cfg["timezone"]).strftime("%Y-%m-%d %H:%M:%S")
    with _lock:
        status = market_status
        running = is_running
        cmd = current_cmd
        ft = last_fetch_time
        ok = last_fetch_ok

    if running:
        status_part = f"[bold yellow blink]● 运行中：{cmd}[/]"
    elif status == "交易中":
        status_part = "[bold green]● 交易中[/]"
    elif status == "非交易日":
        status_part = "[dim]○ 非交易日[/]"
    else:
        status_part = "[dim]○ 休市[/]"

    fetch_style = "green" if ok else "red"
    content = (
        f"[bold cyan]股票自动调度器[/]  │  [white]{tz_str}[/]  │  "
        f"{status_part}  │  最近拉取: [{fetch_style}]{ft}[/]"
    )
    return Panel(content, style="bold", height=3)


def build_stock_table() -> Panel:
    with _lock:
        rows = list(stock_rows)
        portfolio = list(portfolio_codes)

    table = Table(box=box.SIMPLE_HEAD, show_header=True,
                  header_style="bold cyan", expand=True, padding=(0, 1))
    table.add_column("代码",   width=7,  style="dim")
    table.add_column("名称",   width=8)
    table.add_column("现价",   width=8,  justify="right")
    table.add_column("涨跌%",  width=7,  justify="right")
    table.add_column("MA5",    width=8,  justify="right")
    table.add_column("DIF",    width=8,  justify="right")
    table.add_column("量比",   width=5,  justify="right")
    table.add_column("更新日期", width=11, justify="right")

    for row in rows:
        code  = str(row.get("股票代码", "")).zfill(6)
        name  = str(row.get("股票名称", ""))[:4]
        price = row.get("现价")
        chg   = row.get("涨跌幅")
        ma5   = row.get("MA5")
        dif   = row.get("MACD_DIF")
        vr    = row.get("量比")
        upd   = str(row.get("更新日期") or "")[:10]

        price_s = f"{price:.2f}" if isinstance(price, (int, float)) else "—"
        ma5_s   = f"{ma5:.2f}"   if isinstance(ma5,   (int, float)) else "—"
        dif_s   = f"{dif:.3f}"   if isinstance(dif,   (int, float)) else "—"
        vr_s    = f"{vr:.2f}"    if isinstance(vr,    (int, float)) else "—"

        if isinstance(chg, (int, float)):
            chg_color = "red" if chg > 0 else ("green" if chg < 0 else "white")
            chg_text  = Text(f"{chg:+.2f}%", style=chg_color)
        else:
            chg_text = Text("—", style="dim")

        row_style = "bold" if code in portfolio else ""
        table.add_row(code, name, price_s, chg_text,
                      ma5_s, dif_s, vr_s, upd, style=row_style)

    title = f"[bold]股票池[/]（[cyan]{len(rows)}[/] 只，持仓 [yellow]{len(portfolio)}[/] 只）"
    return Panel(table, title=title, border_style="blue")


def build_pnl_panel() -> Panel:
    with _lock:
        rows = list(stock_rows)
        positions = dict(positions_cache)
        cash = available_cash

    # Build price lookup from live stock data
    price_map: dict = {}
    for row in rows:
        code = str(row.get("股票代码", "")).zfill(6)
        if row.get("现价"):
            price_map[code] = float(row["现价"])

    table = Table(box=box.SIMPLE_HEAD, show_header=True,
                  header_style="bold magenta", expand=True, padding=(0, 1))
    table.add_column("名称",  width=6)
    table.add_column("现价",  width=8,  justify="right")
    table.add_column("成本",  width=8,  justify="right")
    table.add_column("盈亏%", width=7,  justify="right")
    table.add_column("盈亏额", width=10, justify="right")

    total_market_value = 0.0
    for code, pos in positions.items():
        name   = pos["name"][:4]
        shares = pos["shares"]
        cost   = pos["cost"]
        price  = price_map.get(code)

        if price:
            mv      = price * shares
            pnl_pct = (price - cost) / cost * 100
            pnl_amt = (price - cost) * shares
            total_market_value += mv
            color = "red" if pnl_pct > 0 else ("green" if pnl_pct < 0 else "white")
            table.add_row(
                name, f"{price:.2f}", f"{cost:.2f}",
                Text(f"{pnl_pct:+.2f}%", style=color),
                Text(f"{pnl_amt:+,.0f}", style=color),
            )
        else:
            table.add_row(name, "—", f"{cost:.2f}", "—", "—")

    total_assets = total_market_value + cash
    total_pnl    = total_assets - INITIAL_CAPITAL
    total_pnl_pct = total_pnl / INITIAL_CAPITAL * 100 if INITIAL_CAPITAL else 0
    pnl_color = "red" if total_pnl > 0 else ("green" if total_pnl < 0 else "white")

    summary = Text.from_markup(
        f"\n[dim]─────────────────────────────────[/]\n"
        f"[bold]市值[/] [cyan]{total_market_value:>10,.0f}[/]  "
        f"[bold]现金[/] [cyan]{cash:>10,.0f}[/]\n"
        f"[bold]总资产[/] [cyan]{total_assets:>8,.0f}[/]  "
        f"[bold]累计[/] [{pnl_color}]{total_pnl:+,.0f}[/] "
        f"[{pnl_color}]({total_pnl_pct:+.2f}%)[/]"
    )
    return Panel(Group(table, summary), title="[bold]实时持仓收益[/]",
                 border_style="magenta")


def build_event_log() -> Panel:
    with _lock:
        lines = list(event_log)
    text = "\n".join(lines[:28]) if lines else "[dim]暂无事件[/]"
    return Panel(text, title="[bold]触发事件[/]", border_style="yellow")


def build_claude_panel() -> Panel:
    with _lock:
        lines = list(claude_buf)
        running = is_running
        cmd = current_cmd

    visible = lines[-35:] if len(lines) > 35 else lines
    body = "\n".join(visible) if visible else "[dim]等待 claude 输出...[/]"

    if running:
        title = f"[bold yellow]Claude 输出 — 运行中：{cmd}[/]"
        border = "yellow"
    else:
        title = "[bold]Claude 输出[/]"
        border = "green"

    return Panel(body, title=title, border_style=border)


def render(cfg: dict) -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="top",    ratio=2),
        Layout(name="bottom", ratio=3),
    )
    layout["top"].split_row(
        Layout(name="stocks", ratio=3),
        Layout(name="pnl",    ratio=2),
    )
    layout["bottom"].split_row(
        Layout(name="claude", ratio=3),
        Layout(name="events", ratio=2),
    )
    layout["header"].update(build_header(cfg))
    layout["stocks"].update(build_stock_table())
    layout["pnl"].update(build_pnl_panel())
    layout["claude"].update(build_claude_panel())
    layout["events"].update(build_event_log())
    return layout


# ─── Schedule thread ──────────────────────────────────────────────────────────

def schedule_thread(cfg: dict, state: dict) -> None:
    iv   = cfg["fetch_interval_seconds"]
    open_t    = cfg["market_open"]
    arc_t     = cfg["archive_time"]
    warc_t    = cfg["weekly_archive_time"]

    schedule.every(iv).seconds.do(job_fetch_and_check, cfg=cfg, state=state)
    schedule.every().day.at(open_t).do(job_market_open,    cfg=cfg)
    schedule.every().day.at(arc_t).do(job_daily_archive,   cfg=cfg)
    schedule.every().friday.at(warc_t).do(job_weekly_archive, cfg=cfg)

    log_event(
        f"调度器就绪 | 拉取:{iv}s | 开盘:{open_t} | "
        f"日归档:{arc_t} | 周归档(周五):{warc_t}"
    )

    while True:
        schedule.run_pending()
        time.sleep(1)


# ─── Entry point ──────────────────────────────────────────────────────────────

def main() -> None:
    cfg   = load_config()
    state = load_state()

    # seed portfolio codes and positions once at startup
    global portfolio_codes, positions_cache, available_cash
    portfolio_codes = refresh_portfolio_codes()
    positions_cache, available_cash = parse_my_md()

    # seed stock display from existing xlsx (no blocking fetch at startup)
    rows = read_stocks_xlsx()
    if not rows:
        exit(0)
    if rows:
        with _lock:
            stock_rows[:] = rows

    # start schedule thread
    t = threading.Thread(target=schedule_thread, args=(cfg, state), daemon=True)
    t.start()

    console = Console()
    try:
        with Live(render(cfg), console=console, refresh_per_second=1,
                  screen=True) as live:
            while True:
                live.update(render(cfg))
                time.sleep(0.5)
    except KeyboardInterrupt:
        console.print("\n[yellow]调度器已停止[/]")


if __name__ == "__main__":
    main()
