echo begin
@echo off

cd .\auto_trade
start /MIN python logger.py
choice /t 1 /d y /n >nul

start /MIN python filer.py
choice /t 1 /d y /n >nul

start /MIN python place_order.py
choice /t 1 /d y /n >nul

start /MIN python stare.py
choice /t 1 /d y /n >nul

cd ..
start /MIN python scheduled_task.py
choice /t 1 /d y /n >nul

cd .\strategy\nearboll
start /MIN python tradeEngine.py

pause
