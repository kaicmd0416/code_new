@echo off
set "SCRIPT_DIR=%~dp0"
set "PYTHONPATH=%SCRIPT_DIR%"
set "ANACONDAPATH=%ANACONDA_PATH%"

cd /d "%SCRIPT_DIR%"
%ANACONDAPATH%\python -c "from running_main import tracking_daily_update_main; tracking_daily_update_main()"


