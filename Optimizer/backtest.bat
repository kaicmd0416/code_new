@echo off
::title Optimizer_python

:: 获取当前脚本所在的目录
set "SCRIPT_DIR=%~dp0"
set "PYTHONPATH=%SCRIPT_DIR%"
set "ANACONDAPATH=%ANACONDA_PATH%"

for /f "tokens=1-3 delims=:." %%a in ("%time%") do (
    set hour=%%a
    set minute=%%b
)

setlocal enabledelayedexpansion
echo Start Time:%time%
set PATH_2=%SCRIPT_DIR%Optimizer_python\history_manual_main

:: 运行第一部分更新
echo Running main_updating_part1...
cd /d "%SCRIPT_DIR%\Optimizer_python\history_manual_main"
%ANACONDAPATH%\python -c "from manual_main import backtest_main_part1; backtest_main_part1()"

:: 获取Python脚本的退出状态
set ERRORLEVEL=%ERRORLEVEL%
echo %ERRORLEVEL%

:: 循环处理每个输出文件
for /l %%i in (1,1,%ERRORLEVEL%) do (
	"%MATLAB_PATH%\matlab.exe" -nodisplay -nosplash -nodesktop -r "addpath('%SCRIPT_DIR%\Optimizer_python\matlab');optimizer_matlab_V7(%%i,'%PATH_2%\\output_part1%%i.txt'); exit;"
)

:: 初始化计数器
set /a count=0

:: 循环等待MATLAB脚本完成
:loop
set /a count+=30
timeout /t 30 >nul
set /a num=0
for /l %%i in (1,1,%ERRORLEVEL%) do (
    if exist result%%i.txt set /a num+=1
)

:: 检查是否所有结果文件都已生成
if !num! neq %ERRORLEVEL% goto loop

:: 如果超过60分钟，结束循环
if !count! gtr 3600 goto end

echo MATLAB scripts have finished.

:: 运行第二部分更新
echo Running main_updating_part2...
cd /d %SCRIPT_DIR%\Optimizer_Backtesting
%ANACONDAPATH%\python -c "from optimizer_backtesting_main import history_running_main; history_running_main()"

cd /d "%SCRIPT_DIR%\Optimizer_python\history_manual_main"
:: 清理临时文件
for /l %%i in (1,1,%ERRORLEVEL%) do (
    if exist output_part1%%i.txt del /F output_part1%%i.txt
    if exist result%%i.txt del /F result%%i.txt
)


:end
echo End Time:%time%
echo Script execution completed.

pause