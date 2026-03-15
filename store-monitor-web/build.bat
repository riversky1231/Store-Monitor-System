@echo off
setlocal EnableExtensions
chcp 65001 >nul

echo =========================================
echo   Build AmazonStoreMonitor.exe
echo =========================================
echo.

rem Must run from store-monitor-web directory
cd /d "%~dp0"

rem Clean previous build outputs to avoid stale bundles
if exist "dist" rmdir /s /q "dist"
if exist "build" rmdir /s /q "build"

rem 1. Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python 3.10+ not found in PATH.
    pause & exit /b 1
)

rem 2. Install / upgrade dependencies
echo [1/6] Install Python dependencies...
pip install -r requirements.txt --quiet
if errorlevel 1 (
    echo [ERROR] pip install failed. Check network or requirements.txt.
    pause & exit /b 1
)

echo [2/6] Install / upgrade PyInstaller...
pip install --upgrade pyinstaller --quiet
if errorlevel 1 (
    echo [ERROR] PyInstaller install failed.
    pause & exit /b 1
)

rem 3. Download Playwright browser locally (offline bundle)
echo [3/6] Download Playwright Chromium (offline bundle)...
set "PLAYWRIGHT_BROWSERS_PATH=%cd%\playwright-browsers"
if exist "%PLAYWRIGHT_BROWSERS_PATH%\*" (
    echo [INFO] Found existing playwright-browsers, skip download.
) else (
    python -m playwright install chromium
    if errorlevel 1 (
        echo [ERROR] Playwright browser download failed. Check network.
        pause & exit /b 1
    )
)

rem 4. Convert icon.png to icon.ico
echo [4/6] Convert app icon (PNG -> ICO)...
python -c "from PIL import Image; img = Image.open('static/icon.png'); img.save('static/icon.ico')"
if errorlevel 1 (
    echo [WARN] Icon conversion failed. Falling back to PNG icon.
    powershell -Command "(gc AmazonStoreMonitor.spec) -replace 'icon.ico','icon.png' | Out-File -encoding utf8 AmazonStoreMonitor.spec"
    powershell -Command "(gc AmazonStoreMonitor.onedir.spec) -replace 'icon.ico','icon.png' | Out-File -encoding utf8 AmazonStoreMonitor.onedir.spec"
)

rem 5. Choose build mode (fixed to onedir)
echo [5/6] Build mode: Onedir (faster startup, smaller EXE)
set "BUILD_SPEC=AmazonStoreMonitor.onedir.spec"

rem 6. Run PyInstaller
echo [6/6] Building (may take 3-10 minutes)...
if exist "dist\\AmazonStoreMonitor.exe" del /f /q "dist\\AmazonStoreMonitor.exe" >nul 2>&1
if exist "dist\\AmazonStoreMonitor\\AmazonStoreMonitor.exe" del /f /q "dist\\AmazonStoreMonitor\\AmazonStoreMonitor.exe" >nul 2>&1
if exist "dist\\AmazonStoreMonitor.exe" (
    echo [ERROR] dist\\AmazonStoreMonitor.exe is in use. Close the running app and try again.
    pause & exit /b 1
)
if exist "dist\\AmazonStoreMonitor\\AmazonStoreMonitor.exe" (
    echo [ERROR] dist\\AmazonStoreMonitor\\AmazonStoreMonitor.exe is in use. Close the running app and try again.
    pause & exit /b 1
)
python -m PyInstaller --noconfirm --clean %BUILD_SPEC%
if errorlevel 1 (
    echo.
    echo [ERROR] PyInstaller build failed. See errors above.
    echo Common causes:
    echo   - Missing dependency
    echo   - File paths contain non-ASCII or spaces
    echo   - Antivirus blocking the build
    pause & exit /b 1
)
echo [INFO] PyInstaller finished successfully.

rem 7. Done
echo [7/7] Build complete!
echo.
echo =========================================
echo   Output:
echo     Onefile: dist\AmazonStoreMonitor.exe
echo     Onedir:  dist\AmazonStoreMonitor\AmazonStoreMonitor.exe
echo =========================================
echo.
echo First run note:
echo   Chromium is bundled for offline use.
echo   Re-run build.bat to update the browser version.
echo.
echo Open output folder now?
choice /c YN /m "Press Y to open, N to exit"
if errorlevel 2 goto :end
explorer dist

:end
pause
