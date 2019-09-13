echo begin
@echo off

cd .\engine\fut\quotation
start /MIN python subscribe_quote.py
choice /t 1 /d y /n >nul

cd ..
cd .\engine
start /MIN python futEngine.py
choice /t 1 /d y /n >nul

pause
