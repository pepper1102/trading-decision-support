@echo off
cd /d "%~dp0"

REM サーバー起動（バックグラウンド）
start "" python run.py

REM 起動を少し待ってからブラウザを開く
timeout /t 2 /nobreak > nul
start "" http://127.0.0.1:8000
