@echo off
set LOGFILE="C:\Users\boydk\Desktop\Personal Lifting Project\py-project-directory\logs\batchlogs\batch-logs-%date:~-4,4%%date:~-10,2%%date:~-7,2%_%time:~0,2%%time:~3,2%%time:~6,2%.txt"
echo %date% %time% - Starting script > %LOGFILE%
echo Current Directory: %CD% >> %LOGFILE%
cd /d "C:\Users\boydk\Desktop\Personal Lifting Project\py-project-directory"
echo Changed to Directory: %CD% >> %LOGFILE%
call ".\venv\Scripts\activate.bat"
echo Activated Virtual Environment >> %LOGFILE%
python ".\scripts\syncToS3.py"
set PYTHONRESULT=%ERRORLEVEL%
echo Python Script Exit Code: %PYTHONRESULT% >> %LOGFILE%
echo %date% %time% - Script Completed >> %LOGFILE%