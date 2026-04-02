#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将Excel文件以表格形式输出
用法: python xlsx_to_json.py <xlsx文件路径>
"""

import pandas as pd
import sys
import io

# 设置stdout编码为UTF-8，解决Windows终端中文乱码
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


def format_table(df, max_rows=None):
    """将DataFrame格式化为终端表格（紧凑格式）"""
    if max_rows:
        df = df.head(max_rows)

    # 处理NaN值显示
    df = df.copy()
    df = df.fillna('-')

    # 提取日期（假设所有行日期相同）
    date_str = ""
    if '更新日期' in df.columns:
        date_str = str(df['更新日期'].iloc[0]) if len(df) > 0 else ""
        df = df.drop(columns=['更新日期'])

    # 数值列右对齐，文本列左对齐
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()

    # 计算每列的最大宽度（无额外间距，完全紧凑）
    col_widths = {}
    for col in df.columns:
        header_len = len(str(col))
        max_data_len = df[col].astype(str).str.len().max()
        col_widths[col] = max(header_len, max_data_len)

    lines = []

    # 标题显示日期
    if date_str:
        lines.append(f"日期: {date_str}")
        lines.append("")

    # 表头
    header = " ".join(str(col).ljust(col_widths[col]) for col in df.columns)
    lines.append(header)
    lines.append("-" * len(header))

    # 数据行
    for _, row in df.iterrows():
        cells = []
        for col in df.columns:
            val = str(row[col])
            if col in numeric_cols:
                cells.append(val.rjust(col_widths[col]))
            else:
                cells.append(val.ljust(col_widths[col]))
        lines.append(" ".join(cells))

    lines.append("-" * len(header))

    return "\n".join(lines)


def xlsx_to_table(file_path):
    """读取Excel文件并以表格形式输出"""
    try:
        df = pd.read_excel(file_path)
        return format_table(df)

    except FileNotFoundError:
        return f"错误: 文件不存在: {file_path}"
    except Exception as e:
        return f"错误: {e}"


def main():
    if len(sys.argv) < 2:
        print("用法: python xlsx_to_json.py <xlsx文件路径>")
        print("示例: python xlsx_to_json.py stocks.xlsx")
        sys.exit(1)

    file_path = sys.argv[1]
    output = xlsx_to_table(file_path)
    print(output)


if __name__ == "__main__":
    main()
