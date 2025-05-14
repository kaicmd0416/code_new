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

set PATH_2=%SCRIPT_DIR%Optimizer_python\daily_auto_main

echo Running main_updating_part1...
cd /d "%SCRIPT_DIR%Optimizer_python\daily_auto_main"
%ANACONDAPATH%\python -c "from auto_main import main_part1_2; main_part1_2()"

set ERRORLEVEL=%ERRORLEVEL%
echo %ERRORLEVEL%

for /l %%i in (1,1,%ERRORLEVEL%) do (
	"%MATLAB_PATH%\matlab.exe" -nodisplay -nosplash -nodesktop -r "addpath('%SCRIPT_DIR%\Optimizer_python\matlab');optimizer_matlab_V7(%%i,'%PATH_2%\\output_part1%%i.txt'); exit;"
)

set /a count=0

:loop
set /a count+=30
timeout /t 30 >nul
set /a num=0
for /l %%i in (1,1,%ERRORLEVEL%) do (
    if exist result%%i.txt set /a num+=1
)

if !num! neq %ERRORLEVEL% goto loop

if !count! gtr 3600 goto end

echo MATLAB scripts have finished.

echo Running main_updating_part2...
cd /d "%SCRIPT_DIR%Optimizer_Backtesting\updating"
%ANACONDAPATH%\python -c "from portfolio_updating import portfolio_updating_auto; portfolio_updating_auto()"

cd /d "%SCRIPT_DIR%Optimizer_python\daily_auto_main"

for /l %%i in (1,1,%ERRORLEVEL%) do (
    if exist output_part1%%i.txt del /F output_part1%%i.txt
    if exist result%%i.txt del /F result%%i.txt
)

echo trading_weight
cd /d "%SCRIPT_DIR%Trading"
%ANACONDAPATH%\python -c "from trading_main import trading_weight_main;trading_weight_main()"
%ANACONDAPATH%\python -c "from trading_main import xy_trading_main;xy_trading_main()"
%ANACONDAPATH%\python -c "from trading_main import rr_trading_main;rr_trading_main()"

:end
echo End Time:%time%
echo Script execution completed.
pause
