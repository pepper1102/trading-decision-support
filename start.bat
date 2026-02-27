@echo off
cd /d "%~dp0"

REM --- Check if today's data already exists ---
python -c "import sqlite3,sys,datetime; conn=sqlite3.connect('local.db'); r=conn.execute(\"SELECT COUNT(*) FROM batch_runs WHERE status='success' AND started_at LIKE ?\", (datetime.date.today().isoformat()+'%%',)).fetchone(); sys.exit(0 if r and r[0]>0 else 1)" 2>nul
if not errorlevel 1 (
  echo Today's data already exists, skipping fetch.
  goto start_server
)

REM --- Fetch latest stock data ---
echo Fetching latest stock data...
python batch.py
if errorlevel 1 (
  echo WARNING: batch.py failed, continuing with existing data...
)

:start_server

REM --- Start scheduler in background ---
start "" python run_scheduler.py

REM --- Start server in background ---
start "" python run.py

REM --- Wait until port 8000 is ready (up to 30 sec) ---
for /l %%i in (1,1,30) do (
  powershell -NoProfile -Command "try { $c = New-Object Net.Sockets.TcpClient('127.0.0.1',8000); $c.Close(); exit 0 } catch { exit 1 }"
  if not errorlevel 1 goto open_browser
  timeout /t 1 /nobreak > nul
)

REM --- Timeout: server did not start ---
echo Server did not start on 127.0.0.1:8000
exit /b 1

:open_browser
REM --- Open browser ---
start "" "http://127.0.0.1:8000"
