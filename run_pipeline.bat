@echo off
REM ============================================
REM Procurement Pipeline - Batch Execution
REM Schedule this file to run at 22:00 (10 PM)
REM ============================================

cd /d "c:\Users\drqsa\Desktop\procurement-project"
echo [%date% %time%] Starting Procurement Pipeline... >> logs\scheduler.log

python main.py >> logs\scheduler.log 2>&1

echo [%date% %time%] Pipeline Complete. >> logs\scheduler.log
