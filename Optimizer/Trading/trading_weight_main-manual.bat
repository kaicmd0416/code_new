@echo off

cd /d D:\Optimizer\Trading
set PYTHONPATH=D:\Optimizer\Trading
C:\ProgramData\Anaconda3\python -c "from trading_main import trading_weight_main;trading_weight_main()"



pause
