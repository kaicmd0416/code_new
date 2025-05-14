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
cd /d "%SCRIPT_DIR%"
%ANACONDAPATH%\python -c "from update_main import daily_update_l4; daily_update_l4()"

:: 回退一层并定位到Tracking文件夹
set "SCRIPT_DIR=%SCRIPT_DIR%\.."
cd /d "%SCRIPT_DIR%\Tracking"
set "SCRIPT_DIR=%CD%"


set "PYTHONPATH=%SCRIPT_DIR%"
set "ANACONDAPATH=%ANACONDA_PATH%"

cd /d "%SCRIPT_DIR%\Product_tracking"
%ANACONDAPATH%\python -c "from Product_tracking_main import update_main;update_main()"


pause
