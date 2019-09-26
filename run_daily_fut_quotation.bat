echo begin
@echo off

cd .\engine\fut\ctp_ht
start /MIN python subscribe_quote.py
choice /t 1 /d y /n >nul

pause
