#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
同步MY.md持仓信息到README.md
在每日归档后运行，保持README展示最新持仓
"""

import re
from datetime import datetime

def read_file(filepath):
    """读取文件内容"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        print(f"读取文件失败 {filepath}: {e}")
        return None

def write_file(filepath, content):
    """写入文件内容"""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"已更新: {filepath}")
        return True
    except Exception as e:
        print(f"写入文件失败 {filepath}: {e}")
        return False

def extract_position_info(my_content):
    """从MY.md提取持仓信息"""
    info = {
        'update_date': '',
        'positions': [],
        'total_assets': '',
        'stock_value': '',
        'cash': '',
        'total_position': '',
        'total_pnl': ''
    }

    # 提取更新日期
    date_match = re.search(r'\*\*最后更新日期\*\*：(.+)', my_content)
    if date_match:
        info['update_date'] = date_match.group(1).strip()

    # 提取持仓表格数据
    # 找到持仓明细表格
    table_pattern = r'\| 股票代码 \| 股票名称 \| 持仓数量 \| 成本价 \| 现价 \| 盈亏比例 \| 市值 \| 仓位占比 \| 累计盈亏 \|\n\|[-\| ]+\|(.+?)(?=\n\n|\n##|\Z)'
    table_match = re.search(table_pattern, my_content, re.DOTALL)

    if table_match:
        rows = table_match.group(1).strip().split('\n')
        for row in rows:
            cells = [cell.strip() for cell in row.split('|')]
            if len(cells) >= 9 and cells[1]:  # 确保有数据
                info['positions'].append({
                    'code': cells[1],
                    'name': cells[2],
                    'quantity': cells[3],
                    'cost': cells[4],
                    'price': cells[5],
                    'pnl_pct': cells[6],
                    'value': cells[7],
                    'position_pct': cells[8],
                    'pnl': cells[9] if len(cells) > 9 else ''
                })

    # 提取资金情况 - 兼容两种格式
    # 格式1: | 项目 | 金额 | 里找到总资产
    # 格式2: | 总资产 | **1,020,872** |
    assets_match = re.search(r'\|[^|]*总资产[^|]*\|\s*\*\*([^|]+?)\*\*\s*\|', my_content)
    if not assets_match:
        assets_match = re.search(r'\*\*总资产\*\* \| ([^|]+?) \|', my_content)
    if assets_match:
        info['total_assets'] = assets_match.group(1).strip()

    # 股票市值
    stock_value_match = re.search(r'\|[^|]*股票市值[^|]*\|\s*([^|]+?)\s*\|', my_content)
    if stock_value_match:
        info['stock_value'] = stock_value_match.group(1).strip()

    # 可用现金
    cash_match = re.search(r'\|[^|]*可用现金[^|]*\|\s*([^|]+?)\s*\|', my_content)
    if cash_match:
        info['cash'] = cash_match.group(1).strip()

    # 总仓位
    position_match = re.search(r'\|[^|]*总仓位[^|]*\|\s*\*\*([^|]+?)\*\*\s*\|', my_content)
    if position_match:
        info['total_position'] = position_match.group(1).strip()

    # 累计盈亏
    pnl_match = re.search(r'\*\*累计盈亏\*\*：(.+?)(?:\(|\n)', my_content)
    if pnl_match:
        info['total_pnl'] = pnl_match.group(1).strip()

    return info

def generate_position_section(info):
    """生成持仓展示段落"""

    if not info['positions']:
        return """## 📊 当前持仓

> 最后更新：{date}

**当前空仓**，等待市场机会。

---
""".format(date=info['update_date'])

    # 生成持仓表格
    table_rows = []
    for pos in info['positions']:
        # 判断盈亏颜色
        pnl_emoji = "🟢" if "+" in pos['pnl_pct'] else "🔴" if "-" in pos['pnl_pct'] else "⚪"
        # 去除价格中已有的星号
        price_clean = pos['price'].replace('**', '').replace('*', '')
        pnl_clean = pos['pnl_pct'].replace('**', '').replace('*', '')
        table_rows.append(
            f"| {pos['code']} | {pos['name']} | {pos['quantity']} | {pos['cost']} | **{price_clean}** | {pnl_emoji} **{pnl_clean}** | {pos['value']} | {pos['position_pct']} |"
        )

    positions_table = '\n'.join(table_rows)

    return """## 📊 当前持仓

> 最后更新：{date}

### 持仓明细

| 股票代码 | 股票名称 | 持仓数量 | 成本价 | 现价 | 盈亏比例 | 市值 | 仓位占比 |
|----------|----------|----------|--------|------|----------|------|----------|
{positions}

### 资金概况

| 项目 | 金额 |
|------|------|
| **总资产** | {total_assets} |
| 股票市值 | {stock_value} |
| 可用现金 | {cash} |
| **总仓位** | {total_position} |
| **累计盈亏** | {total_pnl} |

---
""".format(
        date=info['update_date'],
        positions=positions_table,
        total_assets=info['total_assets'],
        stock_value=info['stock_value'],
        cash=info['cash'],
        total_position=info['total_position'],
        total_pnl=info['total_pnl']
    )

def update_readme(readme_content, position_section):
    """更新README.md内容"""

    # 检查是否已有持仓段落
    position_pattern = r'## 📊 当前持仓.*?(?=\n## |\Z)'

    if re.search(position_pattern, readme_content, re.DOTALL):
        # 替换现有持仓段落
        new_content = re.sub(position_pattern, position_section.strip(), readme_content, flags=re.DOTALL)
    else:
        # 在文件开头添加持仓段落（标题后）
        # 找到第一个标题后的位置
        lines = readme_content.split('\n')
        insert_index = 0
        for i, line in enumerate(lines):
            if line.startswith('# ') and i > 0:
                insert_index = i + 1
                break

        lines.insert(insert_index, '\n' + position_section)
        new_content = '\n'.join(lines)

    return new_content

def main():
    """主函数"""
    print("=" * 50)
    print("同步持仓信息到 README.md")
    print("=" * 50)

    # 读取MY.md
    my_content = read_file('MY.md')
    if not my_content:
        print("错误：无法读取MY.md")
        return False

    # 读取README.md
    readme_content = read_file('README.md')
    if not readme_content:
        print("错误：无法读取README.md")
        return False

    # 提取持仓信息
    info = extract_position_info(my_content)
    print(f"\n提取到更新日期: {info['update_date']}")
    print(f"持仓数量: {len(info['positions'])}")
    print(f"总资产: {info['total_assets']}")

    # 生成持仓段落
    position_section = generate_position_section(info)

    # 更新README
    new_readme = update_readme(readme_content, position_section)

    # 写入文件
    if write_file('README.md', new_readme):
        print("\n✅ 同步完成!")
        return True
    else:
        print("\n❌ 同步失败!")
        return False

if __name__ == '__main__':
    main()
