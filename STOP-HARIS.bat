@echo off
rem Stop the HARIS console started by START-HARIS.bat.
for /f "tokens=5" %%p in ('netstat -ano ^| findstr ":8501" ^| findstr "LISTENING"') do taskkill /PID %%p /F >nul 2>&1
echo HARIS console stopped.
timeout /t 2 >nul
