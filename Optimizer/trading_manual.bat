@echo off

set "SCRIPT_DIR=%~dp0"
set "PYTHONPATH=%SCRIPT_DIR%"
set "ANACONDAPATH=%ANACONDA_PATH%"


cd /d "%SCRIPT_DIR%Trading"
%ANACONDAPATH%\python -c "from trading_main import trading_weight_main;trading_weight_main()"
%ANACONDAPATH%\python -c "from trading_main import xy_trading_main;xy_trading_main()"
%ANACONDAPATH%\python -c "from trading_main import rr_trading_main;rr_trading_main()"



pause