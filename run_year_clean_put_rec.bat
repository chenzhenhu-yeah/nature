echo begin
@echo off

start /MIN python clean_put_rec.py
choice /t 1 /d y /n >nul


pause
