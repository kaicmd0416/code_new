@echo off
set "SCRIPT_DIR=%~dp0"
set "PYTHONPATH=%SCRIPT_DIR%"
set "ANACONDAPATH=%ANACONDA_PATH%"

cd /d "%SCRIPT_DIR%Portfolio_tracking"
%ANACONDAPATH%\python -c "from paper_backtesting_main import backtesting_update_main;backtesting_update_main()"





