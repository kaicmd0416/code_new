@echo off
set "SCRIPT_DIR=%~dp0"
set "PYTHONPATH=%SCRIPT_DIR%"
set "ANACONDAPATH=%ANACONDA_PATH%"

cd /d "%SCRIPT_DIR%Signal_tracking"
%ANACONDAPATH%\python -c "from update_main import update_main;update_main()"





