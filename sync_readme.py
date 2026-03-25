#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将MY.md内容追加到README.md末尾
"""

def read_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        print(f"读取失败 {filepath}: {e}")
        return None

def write_file(filepath, content):
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"已更新: {filepath}")
        return True
    except Exception as e:
        print(f"写入失败 {filepath}: {e}")
        return False

def main():
    # 读取两个文件
    readme = read_file('README.md')
    my_md = read_file('MY.md')

    if not readme or not my_md:
        print("错误：无法读取文件")
        return False

    # 检查是否已有MY.md内容（通过查找特定标记）
    marker = "<!-- MY_MD_START -->"

    if marker in readme:
        # 替换现有内容
        parts = readme.split(marker)
        new_content = parts[0] + marker + "\n\n" + my_md
    else:
        # 追加新内容
        new_content = readme + "\n\n" + marker + "\n\n" + my_md

    # 写入
    if write_file('README.md', new_content):
        print("✅ 已同步MY.md到README.md")
        return True
    return False

if __name__ == '__main__':
    main()
