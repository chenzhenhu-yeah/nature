echo begin
@echo off

cd .\auto_trade
start /MIN python place_order.py
choice /t 1 /d y /n >nul

start /MIN python stare.py
choice /t 1 /d y /n >nul

cd ..
cd .\engine\stk
start /MIN python stkEngine.py

pause
