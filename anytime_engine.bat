echo begin
@echo off

cd .\engine\fut\engine
start /MIN python futEngine.py -anytime
choice /t 1 /d y /n >nul

pause
