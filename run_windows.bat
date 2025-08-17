@echo off
echo Starting Crypto Arbitrage Bot...
echo.

REM Check if .env exists
if not exist .env (
    echo Warning: .env file not found!
    echo Please copy .env.example to .env and configure your settings
    pause
    exit /b 1
)

REM Run the bot
python run_bot.py

REM Keep window open if there's an error
if errorlevel 1 (
    echo.
    echo Bot stopped with an error. Check the logs above.
    pause
)
