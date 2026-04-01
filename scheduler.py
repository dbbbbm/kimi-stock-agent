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

import ctypes
import ctypes.wintypes as wintypes
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from collections import deque
from datetime import datetime, time as dtime
from pathlib import Path

# ─── Windows Console API structures (keyboard + mouse input) ─────────────────

_kernel32 = ctypes.windll.kernel32

_STD_INPUT_HANDLE    = -10
_ENABLE_MOUSE_INPUT  = 0x0010
_ENABLE_EXTENDED_FLAGS = 0x0080
_KEY_EVENT           = 0x0001
_MOUSE_EVENT         = 0x0002
_MOUSE_WHEELED       = 0x0004
_VK_RETURN           = 0x0D
_VK_BACK             = 0x08
_VK_ESCAPE           = 0x1B
_VK_UP               = 0x26
_VK_DOWN             = 0x28
_VK_DELETE           = 0x2E
_FROM_LEFT_1ST_BUTTON_PRESSED = 0x0001
_ENABLE_QUICK_EDIT_MODE       = 0x0040   # must be disabled to receive mouse events


class _COORD(ctypes.Structure):
    _fields_ = [("X", ctypes.c_short), ("Y", ctypes.c_short)]


class _MOUSE_EVENT_RECORD(ctypes.Structure):
    _fields_ = [
        ("dwMousePosition",   _COORD),
        ("dwButtonState",     ctypes.c_ulong),
        ("dwControlKeyState", ctypes.c_ulong),
        ("dwEventFlags",      ctypes.c_ulong),
    ]


class _KEY_EVENT_RECORD(ctypes.Structure):
    _fields_ = [
        ("bKeyDown",          ctypes.c_long),
        ("wRepeatCount",      ctypes.c_ushort),
        ("wVirtualKeyCode",   ctypes.c_ushort),
        ("wVirtualScanCode",  ctypes.c_ushort),
        ("uChar",             ctypes.c_wchar),
        ("dwControlKeyState", ctypes.c_ulong),
    ]


class _EVENT_UNION(ctypes.Union):
    _fields_ = [
        ("KeyEvent",   _KEY_EVENT_RECORD),
        ("MouseEvent", _MOUSE_EVENT_RECORD),
        ("_pad",       ctypes.c_byte * 20),
    ]


class _INPUT_RECORD(ctypes.Structure):
    _fields_ = [
        ("EventType", ctypes.c_ushort),
        ("Event",     _EVENT_UNION),
    ]

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
OPERATE_OUTPUT_DIR = BASE_DIR / "operate_logs"
OPERATE_OUTPUT_FILE = OPERATE_OUTPUT_DIR / "last_operate_output.txt"
NOTIFICATION_FLAG = BASE_DIR / "operate_notification.flag"

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
session_date: str     = ""          # date of current claude session (YYYY-MM-DD)
input_buffer: str     = ""          # current user input being typed
scroll_offsets: dict  = {"claude": 9999, "events": 9999, "stocks": 0, "pnl": 0}
focused_panel: str    = "claude"    # panel currently selected for keyboard scrolling
positions_cache: dict = {}          # {code: {name, shares, cost}} from MY.md
available_cash: float = 0.0         # parsed from MY.md 资金情况
INITIAL_CAPITAL: float = 150_000.0


# ─── Config ───────────────────────────────────────────────────────────────────

def load_config() -> dict:
    defaults = {
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


_EVENTS_VISIBLE = 28   # keep in sync with _PANEL_VISIBLE["events"]

def log_event(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    event_log.append(f"[{ts}] {msg}")

_CLAUDE_VISIBLE = 35   # visible lines in claude panel

def append_claude(line: str) -> None:
    with _lock:
        prev_len = len(claude_buf)
        claude_buf.append(line.rstrip("\n\r"))
        # Auto-scroll if user was at (or near) the bottom
        was_at_bottom = scroll_offsets["claude"] >= max(0, prev_len - _CLAUDE_VISIBLE - 2)
        if was_at_bottom:
            scroll_offsets["claude"] = max(0, len(claude_buf) - _CLAUDE_VISIBLE)


def _init_console_input() -> None:
    """Enable mouse input; disable Quick Edit Mode (which intercepts all mouse clicks)."""
    try:
        h = _kernel32.GetStdHandle(_STD_INPUT_HANDLE)
        mode = ctypes.c_ulong(0)
        _kernel32.GetConsoleMode(h, ctypes.byref(mode))
        new_mode = (mode.value | _ENABLE_MOUSE_INPUT | _ENABLE_EXTENDED_FLAGS) \
                   & ~_ENABLE_QUICK_EDIT_MODE
        _kernel32.SetConsoleMode(h, new_mode)
    except Exception:
        pass


def _get_panel_at(x: int, y: int) -> str | None:
    """Map terminal (x, y) to a panel name using the known layout ratios."""
    w, h = shutil.get_terminal_size(fallback=(120, 30))
    header_end  = 3
    input_start = max(header_end + 4, h - 5)
    content_h   = input_start - header_end
    top_h       = max(1, content_h * 2 // 5)
    bottom_start = header_end + top_h
    mid_x = w * 3 // 5          # stocks/claude take 3/5, pnl/events take 2/5

    if y < header_end or y >= input_start:
        return None
    elif y < bottom_start:
        return "stocks" if x < mid_x else "pnl"
    else:
        return "claude" if x < mid_x else "events"


_PANEL_VISIBLE = {"claude": 35, "events": 28, "stocks": 11, "pnl": 10}


def _scroll_panel(panel: str, direction: int) -> None:
    """Scroll panel by direction (+1 down, -1 up), 3 lines per notch."""
    content_len = {
        "claude": len(claude_buf),
        "events": len(event_log),
        "stocks": len(stock_rows),
        "pnl":    len(positions_cache),
    }.get(panel, 0)
    visible    = _PANEL_VISIBLE.get(panel, 20)
    max_offset = max(0, content_len - visible)
    with _lock:
        scroll_offsets[panel] = max(0, min(scroll_offsets[panel] + direction * 3, max_offset))


def is_market_open(cfg: dict) -> bool:
    tz = pytz.timezone(cfg["timezone"])
    now = datetime.now(tz)
    if now.weekday() >= 5:
        return False
    t = now.time()
    for open, close in cfg['market_open']:
        oh, om = map(int, open.split(":"))
        ch, cm = map(int, close.split(":"))
        if dtime(oh, om) <= t <= dtime(ch, cm):
            return True
    return False


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
    global is_running, current_cmd, last_operate_dt, session_date

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

    # Decide whether to continue today's session or start a new one
    today = datetime.now().strftime("%Y-%m-%d")
    if session_date == today:
        claude_args = ["claude", "--continue", "-p", cmd]
        session_note = "续接今日会话"
    else:
        claude_args = ["claude", "-p", cmd]
        session_note = "新建会话"
        session_date = today
        state = load_state()
        state["session_date"] = today
        save_state(state)

    log_event(f"启动 → {cmd}（{session_note}）")
    append_claude(f"{'─'*60}")
    append_claude(f"▶  {cmd}  [{datetime.now().strftime('%H:%M:%S')}]  {session_note}")
    append_claude(f"{'─'*60}")

    # 收集输出用于保存到文件
    output_lines = []

    try:
        proc = subprocess.Popen(
            claude_args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(BASE_DIR),
        )
        for line in proc.stdout:
            append_claude(line)
            output_lines.append(line)
        proc.wait()
        _init_console_input()   # restore console mode after claude may have changed it
        code = proc.returncode
        append_claude(f"{'─'*60}")
        append_claude(f"■  完成  退出码={code}  [{datetime.now().strftime('%H:%M:%S')}]")
        log_event(f"{cmd} 完成（exit={code}）")

        # 如果是 /operate 命令，保存输出到文件并创建通知标记
        if "/operate" in cmd:
            try:
                # 确保目录存在
                OPERATE_OUTPUT_DIR.mkdir(exist_ok=True)

                # 写入输出文件
                with open(OPERATE_OUTPUT_FILE, "w", encoding="utf-8") as f:
                    f.write(f"命令: {cmd}\n")
                    f.write(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"会话: {session_note}\n")
                    f.write(f"退出码: {code}\n")
                    f.write("=" * 60 + "\n\n")
                    f.writelines(output_lines)

                log_event(f"✓ 输出已保存到 {OPERATE_OUTPUT_FILE}")
                append_claude(f"[dim]✓ 输出已保存到 operate_logs/last_operate_output.txt[/]")
            except Exception as e:
                log_event(f"保存输出文件失败: {e}")

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

def check_market_open(cfg: dict) -> None:
    global market_status
    with _lock:
        if not is_market_open(cfg):
            market_status = "休市"
        else:
            market_status = "交易中"


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


def job_operate(cfg: dict) -> None:
    result = subprocess.run(
        [sys.executable, "-X", "utf8", str(BASE_DIR / "fetch_stocks.py")],
        capture_output=True, text=True, encoding="utf-8",
        errors="replace", cwd=str(BASE_DIR), timeout=120,
    )

    trigger_operate(cfg, "定时触发 /operate")


def job_daily_archive(cfg: dict) -> None:
    try:
        result = subprocess.run(
            [sys.executable, "-X", "utf8", str(BASE_DIR / "fetch_stocks.py"), '--archive'],
            capture_output=True, text=True, encoding="utf-8",
            errors="replace", cwd=str(BASE_DIR), timeout=120,
        )
        ok = result.returncode == 0
    except Exception as e:
        log_event(f"每日拉取收盘价失败：{e}")

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
        rows = sorted(rows, key=lambda row: row.get("涨跌幅"), reverse=True)
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

    with _lock:
        off = scroll_offsets["stocks"]
    visible = _PANEL_VISIBLE["stocks"]
    off  = min(off, max(0, len(rows) - visible))
    rows = rows[off: off + visible]

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

    with _lock:
        focused = focused_panel == "stocks"
    suffix = f" ↕{off}" if off else ""
    focus_tag = " [bold white]◀[/]" if focused else ""
    title = f"[bold]股票池[/]（[cyan]{len(stock_rows)}[/] 只，持仓 [yellow]{len(portfolio)}[/] 只）{suffix}{focus_tag}"
    return Panel(table, title=title, border_style="bright_blue" if focused else "blue")


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

    with _lock:
        off_pnl = scroll_offsets["pnl"]
    pos_items = list(positions.items())
    off_pnl   = min(off_pnl, max(0, len(pos_items) - _PANEL_VISIBLE["pnl"]))
    pos_items = pos_items[off_pnl: off_pnl + _PANEL_VISIBLE["pnl"]]

    total_market_value = 0.0
    # still accumulate full positions for totals (use original positions dict)
    for code, pos in positions.items():
        price = price_map.get(code)
        if price:
            total_market_value += price * pos["shares"]

    for code, pos in pos_items:
        name   = pos["name"][:4]
        shares = pos["shares"]
        cost   = pos["cost"]
        price  = price_map.get(code)

        if price:
            pnl_pct = (price - cost) / cost * 100
            pnl_amt = (price - cost) * shares
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
    with _lock:
        focused = focused_panel == "pnl"
    focus_tag = " [bold white]◀[/]" if focused else ""
    return Panel(Group(table, summary), title=f"[bold]实时持仓收益[/]{focus_tag}",
                 border_style="bright_magenta" if focused else "magenta")


def build_event_log() -> Panel:
    with _lock:
        lines = list(event_log)
        off   = scroll_offsets["events"]
    visible = _PANEL_VISIBLE["events"]
    off = min(off, max(0, len(lines) - visible))
    sliced = lines[off: off + visible]
    text = "\n".join(sliced) if sliced else "[dim]暂无事件[/]"
    with _lock:
        focused = focused_panel == "events"
    suffix = f" ↕{off}" if off else ""
    focus_tag = " [bold white]◀[/]" if focused else ""
    return Panel(text, title=f"[bold]触发事件[/]{suffix}{focus_tag}",
                 border_style="bright_yellow" if focused else "yellow")


def build_claude_panel() -> Panel:
    with _lock:
        lines   = list(claude_buf)
        running = is_running
        cmd     = current_cmd
        off     = scroll_offsets["claude"]

    visible = _CLAUDE_VISIBLE
    off = min(off, max(0, len(lines) - visible))
    sliced = lines[off: off + visible]
    body = "\n".join(sliced) if sliced else "[dim]等待 claude 输出...[/]"
    at_bottom = off >= max(0, len(lines) - visible - 2)
    scroll_tag = "" if at_bottom else f" ↑{off}"

    with _lock:
        focused = focused_panel == "claude"
    focus_tag = " [bold white]◀[/]" if focused else ""

    if running:
        title  = f"[bold yellow]Claude 输出 — 运行中：{cmd}{scroll_tag}{focus_tag}[/]"
        border = "bright_yellow" if focused else "yellow"
    else:
        title  = f"[bold]Claude 输出[/]{scroll_tag}{focus_tag}"
        border = "bright_green" if focused else "green"

    return Panel(body, title=title, border_style=border)


def build_input_panel() -> Panel:
    with _lock:
        buf     = input_buffer
        running = is_running
        sd      = session_date
        fp      = focused_panel

    today = datetime.now().strftime("%Y-%m-%d")
    session_tag = "[green]续接今日会话[/]" if sd == today else "[dim]新会话（首次发送后建立）[/]"
    panel_names = {"claude": "Claude输出", "events": "触发事件", "stocks": "股票池", "pnl": "持仓收益"}
    focus_hint  = f"[bold white]↑↓ 滚动「{panel_names.get(fp, fp)}」[/] · 点击面板切换"

    if running:
        body = Text.from_markup(f"[dim]Claude 运行中，请稍候...[/]\n{focus_hint}")
    else:
        body = Text.from_markup(
            f"[bold cyan]>[/] {buf}[blink]▌[/]\n"
            f"[dim]Enter 发送 · Esc 清空 · /new 新建对话 · {session_tag}[/]\n"
            f"{focus_hint}"
        )
    return Panel(body, title="[bold]对话[/]", border_style="cyan", height=6)


def render(cfg: dict) -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="top",    ratio=2),
        Layout(name="bottom", ratio=3),
        Layout(name="input",  size=6),
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
    layout["input"].update(build_input_panel())
    return layout


# ─── Input thread ────────────────────────────────────────────────────────────

def input_thread_func(cfg: dict) -> None:
    """Handle keyboard + mouse wheel via Windows ReadConsoleInputW."""
    global input_buffer, session_date, focused_panel, stock_rows

    # Wait for rich.Live to finish its console setup, then override
    time.sleep(1.5)
    _init_console_input()
    h       = _kernel32.GetStdHandle(_STD_INPUT_HANDLE)
    record  = _INPUT_RECORD()
    n_read  = ctypes.c_ulong(0)

    while True:
        try:
            # Block up to 100 ms waiting for an input event
            if _kernel32.WaitForSingleObject(h, 20) != 0:
                continue
            _kernel32.ReadConsoleInputW(h, ctypes.byref(record), 1, ctypes.byref(n_read))
            if n_read.value == 0:
                continue

            # ── Keyboard ─────────────────────────────────────────────────────
            if record.EventType == _KEY_EVENT and record.Event.KeyEvent.bKeyDown:
                vk = record.Event.KeyEvent.wVirtualKeyCode
                ch = record.Event.KeyEvent.uChar

                if vk == _VK_UP:                            # ↑ scroll focused panel up
                    _scroll_panel(focused_panel, -1)
                elif vk == _VK_DOWN:                        # ↓ scroll focused panel down
                    _scroll_panel(focused_panel, 1)
                elif vk == _VK_RETURN:                      # Enter → submit input
                    with _lock:
                        text = input_buffer
                        input_buffer = ""
                    text = text.strip()
                    if text == "/new":
                        session_date = ""
                        st = load_state()
                        st["session_date"] = ""
                        save_state(st)
                        log_event("已重置 session，下次发送将创建新对话")
                    elif text == "/update":
                        log_event("手动更新股票数据...")
                        try:
                            subprocess.run(
                                [sys.executable, "-X", "utf8", str(BASE_DIR / "fetch_stocks.py")],
                                capture_output=True, text=True, encoding="utf-8",
                                errors="replace", cwd=str(BASE_DIR), timeout=120,)
                        except Exception as e:
                            log_event("更新失败")
                        finally:
                            with _lock:
                                stock_rows = read_stocks_xlsx()
                            log_event("更新成功")
                            
                    elif text:
                        log_event(f"用户发送：{text[:50]}")
                        threading.Thread(
                            target=run_claude_command, args=(text, cfg), daemon=True
                        ).start()
                elif vk == _VK_BACK:                        # Backspace
                    with _lock:
                        input_buffer = input_buffer[:-1]
                elif vk == _VK_ESCAPE:                      # Escape → clear
                    with _lock:
                        input_buffer = ""
                elif vk == _VK_DELETE:                      # Delete → clear focused panel
                    with _lock:
                        if focused_panel == "claude":
                            claude_buf.clear()
                        elif focused_panel == "events":
                            event_log.clear()
                elif ch and ch.isprintable():               # Printable char
                    with _lock:
                        input_buffer += ch

            # ── Mouse: left-click → focus panel; wheel → scroll ──────────────
            elif record.EventType == _MOUSE_EVENT:
                flags = record.Event.MouseEvent.dwEventFlags
                btn   = record.Event.MouseEvent.dwButtonState
                mx    = record.Event.MouseEvent.dwMousePosition.X
                my    = record.Event.MouseEvent.dwMousePosition.Y
                panel = _get_panel_at(mx, my)

                if flags == 0 and (btn & _FROM_LEFT_1ST_BUTTON_PRESSED):
                    # Left click → set focus
                    if panel:
                        focused_panel = panel

                elif flags & _MOUSE_WHEELED:
                    # Wheel → scroll whichever panel the cursor is over
                    target = panel or focused_panel
                    delta  = ctypes.c_short((btn >> 16) & 0xFFFF).value
                    _scroll_panel(target, -1 if delta > 0 else 1)

        except Exception as e:
            log_event(e)


# ─── Schedule thread ──────────────────────────────────────────────────────────

def schedule_thread(cfg: dict, state: dict) -> None:
    iv   = cfg["fetch_interval_seconds"]
    arc_t     = cfg["archive_time"]
    warc_t    = cfg["weekly_archive_time"]
    operate_time_list = cfg["operate_time_list"]

    schedule.every(5).seconds.do(check_market_open, cfg=cfg)
    if iv > 0:
        schedule.every(iv).seconds.do(job_fetch_and_check, cfg=cfg, state=state)
    for opr_t in operate_time_list:
        schedule.every().day.at(opr_t).do(job_operate,    cfg=cfg)
    schedule.every().day.at(arc_t).do(job_daily_archive,   cfg=cfg)
    schedule.every().friday.at(warc_t).do(job_weekly_archive, cfg=cfg)
    for fetch_t in ["09:31", "11:31", "13:01"]:
        schedule.every().day.at(fetch_t).do(job_fetch_and_check, cfg=cfg, state=state)

    log_event(
        f"调度器就绪 | 拉取:{iv}s | 定时操作:{operate_time_list} | "
        f"日归档:{arc_t} | 周归档(周五):{warc_t}"
    )

    while True:
        schedule.run_pending()
        time.sleep(1)


# ─── Entry point ──────────────────────────────────────────────────────────────

def main() -> None:
    cfg   = load_config()
    state = load_state()

    # seed portfolio codes, positions and session state once at startup
    global portfolio_codes, positions_cache, available_cash, session_date
    portfolio_codes = refresh_portfolio_codes()
    positions_cache, available_cash = parse_my_md()
    session_date = state.get("session_date", "")

    # seed stock display from existing xlsx (no blocking fetch at startup)
    rows = read_stocks_xlsx()
    print(len(rows))
    if not rows:
        exit(0)
    if rows:
        with _lock:
            stock_rows[:] = rows

    _init_console_input()
    # start schedule thread and input thread
    threading.Thread(target=schedule_thread,   args=(cfg, state), daemon=True).start()
    threading.Thread(target=input_thread_func, args=(cfg,),       daemon=True).start()

    console = Console()
    try:
        with Live(render(cfg), console=console, refresh_per_second=60,
                  screen=True) as live:
            while True:
                live.update(render(cfg))
                time.sleep(0.5)
    except KeyboardInterrupt:
        console.print("\n[yellow]调度器已停止[/]")


if __name__ == "__main__":
    main()
