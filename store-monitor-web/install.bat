@echo off
echo =========================================
echo    🚀 Store Monitor Web App 一键安装程序
echo =========================================
echo.
echo 正在安装必需的 Python 依赖库...
pip install -r requirements.txt
echo.
echo 正在下载 Chromium 浏览器内核 (用于绕过亚马逊拦截，这可能需要一点时间)...
playwright install chromium
echo.
echo =========================================
echo    ✅ 安装完成！
echo    请以后双击 start.bat 来运行程序。
echo =========================================
pause
