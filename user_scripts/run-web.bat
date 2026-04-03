@echo off
setlocal
cd /d "%~dp0.."
powershell -ExecutionPolicy Bypass -File ".\scripts\run-web.ps1"
pause
