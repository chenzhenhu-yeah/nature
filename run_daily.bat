echo begin
@echo off

start /MIN python logger.py
choice /t 3 /d y /n >nul

start /MIN python filer.py
choice /t 1 /d y /n >nul

cd .\auto_trade
start /MIN python place_order.py
choice /t 1 /d y /n >nul

start /MIN python stare.py
choice /t 1 /d y /n >nul

cd ..
start /MIN python scheduled_task.py
choice /t 1 /d y /n >nul

cd .\engine\fut\quotation
start /MIN python subscribe_quote.py
choice /t 1 /d y /n >nul

cd ..
cd ..
cd ..
cd .\engine\stk
start /MIN python stkEngine.py

pause
