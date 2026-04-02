#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量查看股票数据
用法: python batch_view.py [类型] [数量]

类型:
  stocks    股票数据 (默认)
  index     指数数据

示例:
  python batch_view.py              # 显示 stocks.xlsx + 最新5个
  python batch_view.py stocks 3     # 显示 stocks.xlsx + 最新3个
  python batch_view.py index 5      # 显示 index.xlsx + 最新5个
"""

import subprocess
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd


def get_excel_date(filepath):
    """从Excel文件中提取日期"""
    try:
        df = pd.read_excel(filepath)
        if '更新日期' in df.columns and len(df) > 0:
            return str(df['更新日期'].iloc[0])
    except Exception:
        pass
    return None


def parse_filename_date(filename):
    """从文件名解析日期 (stocks_20260402_150501.xlsx -> 2026-04-02)"""
    try:
        # 提取 stocks_YYYYMMDD_HHMMSS.xlsx 中的日期部分
        parts = filename.stem.split('_')
        if len(parts) >= 2 and len(parts[1]) == 8:
            date_str = parts[1]
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    except Exception:
        pass
    return None


def run_xlsx_to_table(filepath):
    """调用 xlsx_to_table.py 显示文件内容"""
    result = subprocess.run(
        ['python', '-X', 'utf8', 'xlsx_to_table.py', str(filepath)],
        capture_output=True,
        text=True,
        encoding='utf-8'
    )
    return result.stdout if result.returncode == 0 else result.stderr


def get_file_date_key(filepath):
    """从文件名提取日期作为排序键 (YYYYMMDD, HHMMSS)"""
    try:
        parts = filepath.stem.split('_')
        if len(parts) >= 3:
            # stocks_YYYYMMDD_HHMMSS -> (YYYYMMDD, HHMMSS)
            return (parts[1], parts[2])
    except Exception:
        pass
    return ("0", "0")  # 解析失败排最后


def get_latest_data_files(data_type='stocks', count=5, exclude_date=None):
    """获取data目录下最新的xlsx文件，排除指定日期（按文件名日期排序）"""
    data_dir = Path('data') if data_type == 'stocks' else Path('indices')
    if not data_dir.exists():
        return []

    # 获取指定类型的xlsx文件 (stocks_*.xlsx 或 index_*.xlsx)
    xlsx_files = list(data_dir.glob(f'{data_type}_*.xlsx'))

    # 按文件名日期降序排序
    xlsx_files.sort(key=get_file_date_key, reverse=True)

    # 过滤掉同一天的文件
    filtered_files = []
    for f in xlsx_files:
        file_date = parse_filename_date(f)
        if exclude_date and file_date == exclude_date:
            continue
        filtered_files.append(f)
        if len(filtered_files) >= count:
            break

    return filtered_files


def main():
    # 解析参数
    data_type = 'stocks'
    count = 5

    for arg in sys.argv[1:]:
        if arg.lower() in ('stocks', 'index'):
            data_type = arg.lower()
        elif arg.isdigit():
            count = int(arg)

    files_to_process = []
    current_date = None

    # 1. 检查当前文件 (stocks.xlsx 或 index.xlsx)
    current_file = Path(f'{data_type}.xlsx')
    if current_file.exists():
        current_date = get_excel_date(current_file)
        files_to_process.append((data_type, current_file))

    # 2. 获取data目录最新文件（排除同一天）
    latest_files = get_latest_data_files(data_type, count, exclude_date=current_date)
    for f in latest_files:
        files_to_process.append((data_type, f))

    if not files_to_process:
        print(f"没有找到任何 {data_type} 数据文件")
        print(f"请确保以下文件存在:")
        print(f"  - {data_type}.xlsx (当前数据)")
        print(f"  - data/{data_type}_*.xlsx (历史数据)")
        sys.exit(1)

    # 批量处理
    for i, (_, filepath) in enumerate(files_to_process):
        if i > 0:
            print()  # 文件之间空一行
        output = run_xlsx_to_table(filepath)
        print(output)

if __name__ == "__main__":
    main()
