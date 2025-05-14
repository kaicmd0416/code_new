@echo off
::title Optimizer_python

set "SCRIPT_DIR=%~dp0"
set "PYTHONPATH=%SCRIPT_DIR%"
set "ANACONDAPATH=%ANACONDA_PATH%"

for /f "tokens=1-3 delims=:." %%a in ("%time%") do (
    set hour=%%a
    set minute=%%b
)

setlocal enabledelayedexpansion
echo Start Time:%time%

echo Running data_update...
cd /d "%SCRIPT_DIR%/vix"
%ANACONDAPATH%\python -c "from vix_calculation import VIX_calculation_main; VIX_calculation_main(None,None,True)"


pause