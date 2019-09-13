echo begin
@echo off

start /MIN python scheduled_task.py
choice /t 1 /d y /n >nul


pause
