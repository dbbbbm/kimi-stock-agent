#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据自动获取脚本
功能：
  - 读取 stocks.xlsx 中的股票列表，自动获取最新技术指标，更新到 xlsx
  - 获取大盘指数（上证/深证/创业板/沪深300）技术指标，写入 index.xlsx
数据源：akshare（免费，无需注册）
用法：
  python fetch_stocks.py              # 更新个股 + 大盘指数
  python fetch_stocks.py --index      # 只更新大盘指数
  python fetch_stocks.py --back 3     # 获取3天前的数据
  python fetch_stocks.py --get-recent 7  # 批量获取最近7个交易日（自动跳过非交易日）
作者：Claude
"""

import pandas as pd
import akshare as ak
from datetime import datetime, timedelta
import time
import warnings
import os
warnings.filterwarnings('ignore')


def normalize_stock_code(code):
    """
    标准化股票代码，处理6位数字格式
    沪市(600/601/603/688等)加前缀 sh
    深市(000/001/002/300等)加前缀 sz
    """
    code = str(code).strip().zfill(6)
    return code


def get_stock_individual(code):
    """获取个股基本信息"""
    try:
        code = normalize_stock_code(code)
        # 获取实时行情
        df = ak.stock_zh_a_spot_em()
        stock_info = df[df['代码'] == code]
        if stock_info.empty:
            return None
        return stock_info.iloc[0]
    except Exception as e:
        print(f"获取 {code} 基本信息失败: {e}")
        return None


def get_stock_daily(code, days=60):
    """
    获取股票历史日线数据
    返回包含最近N天数据的DataFrame
    考虑 DAYS_BACK 偏移，获取历史日期的数据
    """
    global DAYS_BACK
    try:
        code = normalize_stock_code(code)

        # 计算偏移后的日期
        end_date = datetime.now() - timedelta(days=DAYS_BACK)
        start_date = end_date - timedelta(days=days)

        # 使用 akshare 获取历史数据
        df = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start_date.strftime('%Y%m%d'),
            end_date=end_date.strftime('%Y%m%d'),
            adjust="qfq"  # 前复权
        )

        if df.empty:
            return None

        return df
    except Exception as e:
        print(f"获取 {code} 历史数据失败: {e}")
        return None


def calculate_ma(df, periods=[5, 10, 20]):
    """计算移动平均线"""
    mas = {}
    for p in periods:
        if len(df) >= p:
            mas[f'MA{p}'] = round(df['收盘'].tail(p).mean(), 2)
        else:
            mas[f'MA{p}'] = None
    return mas


def calculate_macd(df, fast=12, slow=26, signal=9):
    """
    计算 MACD 指标
    返回: DIF, DEA, MACD柱状图
    """
    try:
        if len(df) < slow + signal:
            return None, None

        closes = df['收盘'].values

        # 计算EMA
        ema_fast = pd.Series(closes).ewm(span=fast, adjust=False).mean()
        ema_slow = pd.Series(closes).ewm(span=slow, adjust=False).mean()

        # DIF = EMA(12) - EMA(26)
        dif = ema_fast - ema_slow

        # DEA = EMA(DIF, 9)
        dea = dif.ewm(span=signal, adjust=False).mean()

        # MACD = 2 * (DIF - DEA)
        macd = 2 * (dif - dea)

        return round(dif.iloc[-1], 3), round(dea.iloc[-1], 3)
    except Exception as e:
        print(f"计算 MACD 失败: {e}")
        return None, None


def calculate_volume_ma(df, period=20):
    """计算成交量均线"""
    if len(df) >= period:
        return round(df['成交量'].tail(period).mean() / 10000, 2)  # 转换为万手
    return None


def get_stock_data(code, name=None):
    """
    获取单只股票的完整数据
    返回包含技术指标的字典
    """
    global DAYS_BACK

    # 计算数据日期
    data_date = datetime.now() - timedelta(days=DAYS_BACK)
    date_str = data_date.strftime('%Y-%m-%d')

    if DAYS_BACK > 0:
        print(f"正在获取 {code} {name or ''} 的数据 (日期: {date_str})...")
    else:
        print(f"正在获取 {code} {name or ''} 的数据...")

    # 获取历史数据
    df = get_stock_daily(code, days=60)
    if df is None or df.empty:
        print(f"  ❌ 无法获取 {code} 的数据")
        return None

    # 最新数据
    latest = df.iloc[-1]

    # 计算技术指标
    mas = calculate_ma(df)
    macd_dif, macd_dea = calculate_macd(df)
    volume_ma = calculate_volume_ma(df)

    # 计算涨跌幅
    prev_close = df.iloc[-2]['收盘'] if len(df) > 1 else latest['收盘']
    change_pct = round((latest['收盘'] - prev_close) / prev_close * 100, 2)

    # 量比（今日成交量 / 过去20日均量）
    volume_ratio = round(latest['成交量'] / (volume_ma * 10000), 2) if volume_ma else 1.0

    result = {
        '股票代码': normalize_stock_code(code),
        '股票名称': name or latest.get('名称', ''),
        '现价': round(latest['收盘'], 2),
        'MA5': mas.get('MA5'),
        'MA10': mas.get('MA10'),
        'MA20': mas.get('MA20'),
        '成交量(万)': round(latest['成交量'] / 10000, 2),
        '均量(万)': volume_ma,
        'MACD_DIF': macd_dif,
        'MACD_DEA': macd_dea,
        '涨跌幅': change_pct,
        '最高价': round(latest['最高'], 2),
        '最低价': round(latest['最低'], 2),
        '开盘价': round(latest['开盘'], 2),
        '换手率': latest.get('换手率', None),
        '量比': volume_ratio,
        '更新日期': data_date.strftime('%Y-%m-%d'),
        'DAYS_BACK': DAYS_BACK
    }

    print(f"  ✅ 获取成功: {result['股票名称']} 现价:{result['现价']}")
    return result

DAYS_BACK = 0


def get_recent_trading_offsets(n):
    """
    返回最近 n 个交易日距今的 DAYS_BACK 偏移列表（从旧到新排序）
    例如：今天是周一，最近3个交易日 → [3, 4, 5]（对应上周五、周四、周三）
    失败时回退到自然日模式（不过滤非交易日）
    """
    try:
        df = ak.tool_trade_date_hist_sina()
        dates = pd.to_datetime(df.iloc[:, 0]).sort_values()
        today = datetime.now().date()
        past_dates = dates[dates.dt.date < today]
        recent = past_dates.tail(n)
        offsets = sorted([(today - d.date()).days for d in recent], reverse=True)
        print(f"📅 交易日历加载成功，最近 {n} 个交易日: "
              f"{[str((datetime.now() - timedelta(days=o)).date()) for o in offsets]}")
        return offsets
    except Exception as e:
        print(f"⚠️  获取交易日历失败: {e}，回退到自然日模式")
        return list(range(n, 0, -1))


# 大盘指数列表（akshare 格式）
INDEX_LIST = [
    {"symbol": "sh000001", "name": "上证综指"},
    {"symbol": "sz399001", "name": "深证成指"},
    {"symbol": "sz399006", "name": "创业板指"},
    {"symbol": "sh000300", "name": "沪深300"},
]


def get_index_data(symbol, name):
    """获取指数技术指标"""
    global DAYS_BACK
    try:
        end_date = datetime.now() - timedelta(days=DAYS_BACK)

        df = ak.stock_zh_index_daily_em(symbol=symbol)
        df['date'] = pd.to_datetime(df['date'])
        df = df[df['date'] <= end_date].tail(60).copy()
        df = df.rename(columns={
            'close': '收盘', 'open': '开盘',
            'high': '最高', 'low': '最低', 'volume': '成交量'
        })

        if df.empty:
            return None

        latest = df.iloc[-1]
        mas = calculate_ma(df)
        macd_dif, macd_dea = calculate_macd(df)
        volume_ma = calculate_volume_ma(df)
        prev_close = df.iloc[-2]['收盘'] if len(df) > 1 else latest['收盘']
        change_pct = round((latest['收盘'] - prev_close) / prev_close * 100, 2)
        volume_ratio = round(latest['成交量'] / (volume_ma * 10000), 2) if volume_ma else 1.0

        return {
            '指数代码': symbol,
            '指数名称': name,
            '现价': round(latest['收盘'], 2),
            '涨跌幅': change_pct,
            'MA5': mas.get('MA5'),
            'MA10': mas.get('MA10'),
            'MA20': mas.get('MA20'),
            'MACD_DIF': macd_dif,
            'MACD_DEA': macd_dea,
            '成交量(万)': round(latest['成交量'] / 10000, 2),
            '均量(万)': volume_ma,
            '量比': volume_ratio,
            '更新日期': end_date.strftime('%Y-%m-%d'),
            'DAYS_BACK': DAYS_BACK,
        }
    except Exception as e:
        print(f"获取 {name}({symbol}) 失败: {e}")
        return None


def update_index_excel(output_file='index.xlsx', save_to_data_dir=True):
    """获取所有大盘指数数据，写入 index.xlsx"""
    global DAYS_BACK

    data_date = datetime.now() - timedelta(days=DAYS_BACK)
    timestamp = data_date.strftime('%Y%m%d_%H%M%S')

    print(f"\n{'='*60}")
    print(f"大盘指数数据更新")
    print(f"{'='*60}\n")

    index_data = []
    for idx in INDEX_LIST:
        print(f"正在获取 {idx['name']}({idx['symbol']})...")
        data = get_index_data(idx['symbol'], idx['name'])
        if data:
            index_data.append(data)
            print(f"  ✅ {idx['name']} 现价:{data['现价']} 涨跌幅:{data['涨跌幅']}%")
        else:
            print(f"  ❌ {idx['name']} 获取失败")
        time.sleep(0.3)

    if not index_data:
        print("❌ 所有指数数据获取失败")
        return

    df_index = pd.DataFrame(index_data)

    try:
        df_index.to_excel(output_file, index=False, sheet_name='大盘指标')
        print(f"\n✅ 大盘指标已保存: {output_file}")

        if save_to_data_dir:
            data_dir = 'indices'
            if not os.path.exists(data_dir):
                os.makedirs(data_dir)
            ts_path = os.path.join(data_dir, f"index_{timestamp}.xlsx")
            df_index.to_excel(ts_path, index=False, sheet_name='大盘指标')
            print(f"历史版本: {ts_path}")

        print("\n📊 大盘指标汇总:")
        print(df_index[['指数名称', '现价', '涨跌幅', 'MA5', 'MACD_DIF', 'MACD_DEA']].to_string(index=False))

    except Exception as e:
        print(f"保存 {output_file} 失败: {e}")


def update_stocks_excel(input_file='stocks.xlsx', output_file=None, save_to_data_dir=True):
    """
    主函数：读取 stocks.xlsx，更新数据，写回文件

    Args:
        input_file: 输入文件路径
        output_file: 输出文件路径（默认覆盖input_file）
        save_to_data_dir: 是否同时保存带时间戳的版本到data目录
    """
    global DAYS_BACK
    if output_file is None:
        output_file = input_file

    # 生成带时间戳的文件名（精确到秒）
    # 考虑 DAYS_BACK 偏移，计算实际数据日期
    data_date = datetime.now() - timedelta(days=DAYS_BACK)
    timestamp = data_date.strftime('%Y%m%d_%H%M%S')

    # 如果 DAYS_BACK > 0，在文件名中标注回溯天数
    if DAYS_BACK > 0:
        timestamp_filename = f"stocks_{timestamp}.xlsx"
        data_date_str = data_date.strftime('%Y-%m-%d')
        print(f"⚠️  回溯模式: 获取 {DAYS_BACK} 天前的数据 (日期: {data_date_str})")
    else:
        timestamp_filename = f"stocks_{timestamp}.xlsx"

    # data目录路径
    data_dir = 'data'
    if save_to_data_dir and not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f"创建目录: {data_dir}/")

    timestamp_file_path = os.path.join(data_dir, timestamp_filename) if save_to_data_dir else None

    print(f"\n{'='*60}")
    print(f"股票数据自动更新工具")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    # 1. 读取现有 stocks.xlsx
    try:
        df_existing = pd.read_excel(input_file)
        print(f"读取到 {len(df_existing)} 只股票\n")
    except FileNotFoundError:
        print(f"错误: 找不到文件 {input_file}")
        print("请确保 stocks.xlsx 存在，或创建一个新的股票列表")
        return
    except Exception as e:
        print(f"读取文件失败: {e}")
        return

    # 2. 获取每只股票的数据
    updated_data = []

    for idx, row in df_existing.iterrows():
        code = str(row.get('股票代码', '')).strip()
        name = row.get('股票名称', '')

        if not code or code == 'nan':
            continue

        # 获取数据
        stock_data = get_stock_data(code, name)

        if stock_data:
            # 保留原有的字段（如所属行业、关注理由）
            for col in df_existing.columns:
                if col not in stock_data and col in row:
                    stock_data[col] = row[col]

            updated_data.append(stock_data)
        else:
            # 如果获取失败，保留原有数据
            updated_data.append(row.to_dict())

        # 延时，避免请求过快
        time.sleep(0.5)

    # 3. 创建新的 DataFrame
    df_updated = pd.DataFrame(updated_data)

    # 4. 调整列顺序（让重要信息在前面）
    priority_cols = [
        '股票代码', '股票名称', '现价', '涨跌幅', 'MA5', 'MA10', 'MA20',
        'MACD_DIF', 'MACD_DEA', '成交量(万)', '均量(万)', '量比',
        '开盘价', '最高价', '最低价', '换手率'
    ]

    # 保留原有列的顺序，但把优先列放前面
    other_cols = [c for c in df_updated.columns if c not in priority_cols]
    final_cols = priority_cols + other_cols

    # 只保留存在的列
    final_cols = [c for c in final_cols if c in df_updated.columns]
    df_updated = df_updated[final_cols]

    # 5. 保存到 Excel
    try:
        # 5.1 保存最新版本（覆盖原文件）
        df_updated.to_excel(output_file, index=False, sheet_name='股票池')

        # 5.2 保存带时间戳的版本到data目录
        if save_to_data_dir and timestamp_file_path:
            df_updated.to_excel(timestamp_file_path, index=False, sheet_name='股票池')

        print(f"\n{'='*60}")
        print(f"✅ 数据更新成功！")
        print(f"最新版本: {output_file}")
        if timestamp_file_path:
            print(f"历史版本: {timestamp_file_path}")
        print(f"共更新 {len(df_updated)} 只股票")
        print(f"{'='*60}")

        # 打印汇总
        print("\n📊 数据汇总:")
        print(df_updated[['股票代码', '股票名称', '现价', '涨跌幅', 'MA5', 'MACD_DIF', 'MACD_DEA']].to_string(index=False))

    except Exception as e:
        print(f"保存文件失败: {e}")


def create_sample_excel(filename='stocks.xlsx'):
    """
    创建示例 stocks.xlsx 文件
    包含一些常见的股票代码
    """
    sample_data = {
        '股票代码': ['000001', '600519', '000858', '002415', '300750', '600036', '000333'],
        '股票名称': ['平安银行', '贵州茅台', '五粮液', '海康威视', '宁德时代', '招商银行', '美的集团'],
        '所属行业': ['银行', '白酒', '白酒', '安防', '新能源', '银行', '家电'],
        '关注理由': [
            '银行龙头，估值低',
            '白酒龙头，业绩稳',
            '白酒次高端',
            '安防龙头，AI概念',
            '新能源电池龙头',
            '零售银行龙头',
            '家电龙头，稳健'
        ]
    }

    df = pd.DataFrame(sample_data)
    df.to_excel(filename, index=False, sheet_name='股票池')
    print(f"✅ 创建示例文件: {filename}")
    print(f"包含 {len(df)} 只示例股票")


if __name__ == '__main__':
    import sys

    # 检查参数
    if len(sys.argv) > 1:
        if sys.argv[1] == '--create-sample':
            # 创建示例文件
            create_sample_excel()
        elif sys.argv[1] == '--index':
            # 只更新大盘指数
            # 用法: python fetch_stocks.py --index
            update_index_excel()
        elif sys.argv[1] == '--back':
            # 获取指定天数前的数据
            # 用法: python fetch_stocks.py --back 3
            DAYS_BACK = int(sys.argv[2]) if len(sys.argv) > 2 else 1
            print(f"🔄 获取 {DAYS_BACK} 天前的数据...")
            update_stocks_excel()
            update_index_excel()
        elif sys.argv[1] == '--get-recent':
            # 获取最近N个交易日的历史数据（跳过非交易日）
            # 用法: python fetch_stocks.py --get-recent 7
            days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
            print(f"🔄 批量获取最近 {days} 个交易日的历史数据（跳过非交易日）...\n")
            offsets = get_recent_trading_offsets(days)
            for i, offset in enumerate(offsets, 1):
                DAYS_BACK = offset
                data_date = datetime.now() - timedelta(days=DAYS_BACK)
                print(f"\n[{i}/{len(offsets)}] {data_date.strftime('%Y-%m-%d')}")
                update_stocks_excel()
                update_index_excel()
                time.sleep(1)  # 批次间延时
            print(f"\n✅ 完成！共获取 {len(offsets)} 个交易日的历史数据")
        elif sys.argv[1] == '--archive':
            print("每日归档模式")
            update_stocks_excel(save_to_data_dir=True)
            update_index_excel(save_to_data_dir=True)
        else:
            pass
        
    else:
        # 正常更新数据（DAYS_BACK = 0）
        update_stocks_excel(save_to_data_dir=False)
        update_index_excel(save_to_data_dir=False)
