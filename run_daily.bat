echo begin
@echo off

start /MIN python logger.py
choice /t 1 /d y /n >nul

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

cd .\hf_ctp_py_proxy
start /MIN python test_quote.py
choice /t 1 /d y /n >nul

cd..
cd .\engine\nearboll
start /MIN python bollEngine.py

pause
