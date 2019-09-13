echo begin
@echo off

start /MIN python logger.py
choice /t 3 /d y /n >nul

start /MIN python filer.py
choice /t 1 /d y /n >nul

pause
