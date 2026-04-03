@echo off
setlocal
cd /d "%~dp0.."
powershell -ExecutionPolicy Bypass -File ".\scripts\run-scraper.ps1"
pause
