@echo off
chcp 65001 >nul
title 股票自动调度器
cd /d "%~dp0"
echo 正在启动股票自动调度器...
echo 工作目录: %CD%
echo.
python scheduler.py
pause
