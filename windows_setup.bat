@echo off
echo Installing Python dependencies for Windows...

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python is not installed or not in PATH
    echo Please install Python 3.11+ from https://python.org
    pause
    exit /b 1
)

REM Install requirements
echo Installing dependencies...
pip install -r requirements.txt

REM Create .env from example if it doesn't exist
if not exist .env (
    echo Creating .env file...
    copy .env.example .env
    echo Please edit .env file with your Telegram bot token and preferences
)

echo.
echo Setup complete!
echo.
echo To run the bot:
echo   python run_bot.py
echo.
echo Don't forget to edit .env file with your settings!
pause
