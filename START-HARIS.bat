@echo off
setlocal
title HARIS - Network Resilience Console
cd /d "%~dp0"

rem ---- Python: the project's own .venv, created on first run -------------
if not exist ".venv\Scripts\python.exe" (
  echo First run - creating .venv ...
  py -3.12 -m venv .venv >nul 2>&1 || py -3.11 -m venv .venv >nul 2>&1 || python -m venv .venv
)
set "PY=.venv\Scripts\python.exe"
if not exist "%PY%" (
  echo Could not create a Python environment. Install Python 3.12 from python.org and retry.
  pause
  exit /b 1
)
"%PY%" -c "import streamlit, langgraph, pydantic_settings, fastapi, httpx" >nul 2>&1
if errorlevel 1 (
  echo First run - installing requirements, this takes a few minutes...
  "%PY%" -m pip install -r requirements.txt
)

rem ---- Settings: a safe fixture demo unless .env already exists ----------
if not exist ".env" copy /y ".env.example" ".env" >nul

rem ---- Local AI: use Ollama if it is running and has the model -----------
if not defined HARIS_LOCAL_MODEL set "HARIS_LOCAL_MODEL=qwen2.5:1.5b"
powershell -NoProfile -Command "try{$t=Invoke-RestMethod 'http://127.0.0.1:11434/api/tags' -TimeoutSec 2; if($t.models.name -contains '%HARIS_LOCAL_MODEL%'){exit 0}}catch{}; exit 1"
if errorlevel 1 (
  echo Local AI: Ollama or model %HARIS_LOCAL_MODEL% not found - deterministic policy only.
) else (
  echo Local AI: %HARIS_LOCAL_MODEL% on this machine.
  set "HARIS_LOCAL_LLM_ENABLED=true"
  set "LOCAL_LLM_BASE_URL=http://127.0.0.1:11434"
  set "LOCAL_LLM_MODEL=%HARIS_LOCAL_MODEL%"
  rem Load the model into memory now so the first cycle is fast.
  start "" /b powershell -NoProfile -Command "try{Invoke-RestMethod 'http://127.0.0.1:11434/api/generate' -Method Post -Body '{\"model\":\"%HARIS_LOCAL_MODEL%\",\"keep_alive\":\"30m\"}' -TimeoutSec 180 | Out-Null}catch{}"
)

rem ---- Start the console --------------------------------------------------
for /f "tokens=5" %%p in ('netstat -ano ^| findstr ":8501" ^| findstr "LISTENING"') do taskkill /PID %%p /F >nul 2>&1
echo Starting HARIS console...
start "HARIS console - close this window to stop" /min "%PY%" -m streamlit run app.py --server.port 8501 --server.headless true --browser.gatherUsageStats false

powershell -NoProfile -Command "for($i=0;$i -lt 90;$i++){try{Invoke-WebRequest 'http://localhost:8501' -UseBasicParsing -TimeoutSec 2 | Out-Null; exit 0}catch{Start-Sleep -Seconds 1}}; exit 1"
if errorlevel 1 (
  echo HARIS did not start. Open the minimised "HARIS console" window to see why.
  pause
  exit /b 1
)
start "" "http://localhost:8501"
endlocal
