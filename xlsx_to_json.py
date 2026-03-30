#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将Excel文件转换为JSON格式输出
用法: python xlsx_to_json.py <xlsx文件路径>
"""

import pandas as pd
import json
import sys
import io
from datetime import datetime

# 设置stdout编码为UTF-8，解决Windows终端中文乱码
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


def xlsx_to_json(file_path):
    """读取Excel文件并转换为JSON格式"""
    try:
        # 读取Excel文件
        df = pd.read_excel(file_path)

        # 将NaN值转换为None（JSON可序列化）
        df = df.where(pd.notna(df), None)

        # 转换为字典列表
        records = df.to_dict('records')

        # 输出JSON格式
        result = {
            "source_file": file_path,
            "total_records": len(records),
            "columns": df.columns.tolist(),
            "data": records
        }

        return json.dumps(result, ensure_ascii=False, indent=2)

    except FileNotFoundError:
        return json.dumps({"error": f"文件不存在: {file_path}"}, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)


def main():
    if len(sys.argv) < 2:
        print("用法: python xlsx_to_json.py <xlsx文件路径>")
        print("示例: python xlsx_to_json.py stocks.xlsx")
        sys.exit(1)

    file_path = sys.argv[1]
    json_output = xlsx_to_json(file_path)
    print(json_output)


if __name__ == "__main__":
    main()
