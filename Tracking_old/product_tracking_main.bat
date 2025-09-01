@echo off
echo Product_tracking Run...
set "SCRIPT_DIR=%~dp0"
set "PYTHONPATH=%SCRIPT_DIR%"
set "ANACONDAPATH=%ANACONDA_PATH%"

cd /d "%SCRIPT_DIR%Product_tracking"
%ANACONDAPATH%\python -c "from Product_tracking_main import update_main;update_main()"





