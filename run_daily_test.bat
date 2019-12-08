echo begin
@echo off

start /MIN python logger.py
choice /t 3 /d y /n >nul

start /MIN python filer.py
choice /t 1 /d y /n >nul


start /MIN python scheduled_task.py
choice /t 1 /d y /n >nul


cd .\engine\fut\ctp_ht
start /MIN python subscribe_quote.py
choice /t 1 /d y /n >nul

cd ..
cd ..
cd ..
cd .\engine\fut\engine
start /MIN python futEngine.py
choice /t 1 /d y /n >nul

cd ..
cd ..
cd ..
cd .\web
start /MIN python web_ins.py
choice /t 1 /d y /n >nul

pause
