@echo off
::title Optimizer_python

:: ��ȡ��ǰ�ű����ڵ�Ŀ¼
set "SCRIPT_DIR=%~dp0"
set "PYTHONPATH=%SCRIPT_DIR%"
set "ANACONDAPATH=%ANACONDA_PATH%"

for /f "tokens=1-3 delims=:." %%a in ("%time%") do (
    set hour=%%a
    set minute=%%b
)

setlocal enabledelayedexpansion
echo Start Time:%time%

echo Running back test...
cd /d "%SCRIPT_DIR%\Optimizer_python"
%ANACONDAPATH%\python -c "from optimizer_history_main import history_optimizer_main; history_optimizer_main()"


:end
echo End Time:%time%
echo Script execution completed.

pause