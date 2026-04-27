#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量删除 stocks.xlsx 和 data/ 目录下所有 xlsx 中指定的股票记录。
支持按股票代码或中文名筛选删除，支持通配符前缀/后缀匹配（*、?）。

用法示例:
    python -X utf8 delete_stocks.py --codes 300750,858 --names 宁德时代,五粮液
    python -X utf8 delete_stocks.py --codes 300750
    python -X utf8 delete_stocks.py --names 宁德时代
    python -X utf8 delete_stocks.py --codes 688*       # 删除所有688开头
    python -X utf8 delete_stocks.py --names *ST*       # 删除名称含ST的
    python -X utf8 delete_stocks.py --codes 300750 --names 宁德时代 --yes
"""

import argparse
import fnmatch
import sys
from pathlib import Path

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="批量删除 stocks.xlsx 和 data/*.xlsx 中的指定股票记录",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python -X utf8 delete_stocks.py --codes 300750,858
  python -X utf8 delete_stocks.py --names 宁德时代,五粮液
  python -X utf8 delete_stocks.py --codes 300750 --names 宁德时代 --yes
  python -X utf8 delete_stocks.py --codes 688*          # 删除所有688开头
  python -X utf8 delete_stocks.py --names *ST*          # 删除名称含ST的
        """,
    )
    parser.add_argument(
        "--codes",
        type=str,
        default="",
        help="要删除的股票代码，多个用逗号分隔，支持通配符 * ?，例如: 300750,858,688*",
    )
    parser.add_argument(
        "--names",
        type=str,
        default="",
        help="要删除的股票中文名，多个用逗号分隔，支持通配符 * ?，例如: 宁德时代,五粮液,*ST*",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="跳过确认，直接执行删除",
    )
    return parser.parse_args()


def load_xlsx(path: Path) -> pd.DataFrame | None:
    try:
        return pd.read_excel(path)
    except Exception as e:
        print(f"[错误] 读取失败: {path} -> {e}")
        return None


def save_xlsx(df: pd.DataFrame, path: Path) -> bool:
    try:
        df.to_excel(path, index=False)
        return True
    except Exception as e:
        print(f"[错误] 保存失败: {path} -> {e}")
        return False


def main():
    args = parse_args()

    target_codes = {c.strip() for c in args.codes.split(",") if c.strip()}
    target_names = {n.strip() for n in args.names.split(",") if n.strip()}

    if not target_codes and not target_names:
        print("[提示] 未指定任何筛选条件，请使用 --codes 或 --names 参数。")
        sys.exit(1)

    print("=" * 50)
    print("删除筛选条件:")
    if target_codes:
        print(f"  股票代码: {', '.join(target_codes)}")
    if target_names:
        print(f"  股票名称: {', '.join(target_names)}")
    print("=" * 50)

    # 收集待处理文件
    root = Path.cwd()
    files_to_process = []

    stocks_path = root / "stocks.xlsx"
    if stocks_path.exists():
        files_to_process.append(stocks_path)

    data_dir = root / "data"
    if data_dir.exists() and data_dir.is_dir():
        for f in sorted(data_dir.glob("*.xlsx")):
            files_to_process.append(f)

    if not files_to_process:
        print("[提示] 未找到 stocks.xlsx 或 data/*.xlsx 文件。")
        sys.exit(0)

    print(f"\n待扫描文件数: {len(files_to_process)}")

    # 先预览匹配结果
    total_matches = 0
    preview_rows = []
    for fp in files_to_process:
        df = load_xlsx(fp)
        if df is None or df.empty:
            continue

        mask = pd.Series(False, index=df.index)
        if "股票代码" in df.columns and target_codes:
            # 支持通配符匹配（如 688*），统一转为字符串比对
            code_series = df["股票代码"].astype(str)
            mask |= code_series.apply(
                lambda v: any(fnmatch.fnmatch(v, pat) for pat in target_codes)
            )
        if "股票名称" in df.columns and target_names:
            name_series = df["股票名称"].astype(str)
            mask |= name_series.apply(
                lambda v: any(fnmatch.fnmatch(v, pat) for pat in target_names)
            )

        match_count = mask.sum()
        if match_count > 0:
            total_matches += match_count
            preview_rows.append(f"  {fp.name}: 匹配 {match_count} 条")

    if not preview_rows:
        print("\n[结果] 未匹配到任何记录，无需删除。")
        sys.exit(0)

    print(f"\n匹配预览 (共 {total_matches} 条):")
    for line in preview_rows:
        print(line)

    # 确认
    if not args.yes:
        confirm = input("\n确认删除以上匹配记录? [y/N]: ").strip().lower()
        if confirm not in ("y", "yes"):
            print("已取消操作。")
            sys.exit(0)

    # 执行删除
    deleted_total = 0
    for fp in files_to_process:
        df = load_xlsx(fp)
        if df is None or df.empty:
            continue

        mask = pd.Series(False, index=df.index)
        if "股票代码" in df.columns and target_codes:
            code_series = df["股票代码"].astype(str)
            mask |= code_series.apply(
                lambda v: any(fnmatch.fnmatch(v, pat) for pat in target_codes)
            )
        if "股票名称" in df.columns and target_names:
            name_series = df["股票名称"].astype(str)
            mask |= name_series.apply(
                lambda v: any(fnmatch.fnmatch(v, pat) for pat in target_names)
            )

        match_count = mask.sum()
        if match_count == 0:
            continue

        df_clean = df[~mask].reset_index(drop=True)
        if save_xlsx(df_clean, fp):
            deleted_total += match_count
            print(f"[已处理] {fp.name}: 删除 {match_count} 条，剩余 {len(df_clean)} 条")
        else:
            print(f"[失败] {fp.name}: 删除 {match_count} 条时保存出错")

    print(f"\n[完成] 共删除 {deleted_total} 条记录。")


if __name__ == "__main__":
    main()
