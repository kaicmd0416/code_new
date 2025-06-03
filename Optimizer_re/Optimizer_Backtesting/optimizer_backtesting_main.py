import pandas as pd
import os
import global_setting.global_dic as glv
from Optimizer_Backtesting.main.backtest_main import backtesting_main
def history_config_withdraw():
    inputpath = glv.get('backtest_config')
    df = pd.read_excel(inputpath,sheet_name='portfolio_info')
    return df
def history_running_main():
    df_config=history_config_withdraw()
    bm=backtesting_main()
    bm.optimizer_history_backtesting_main(df_config)

if __name__ == '__main__':
    history_running_main()
    pass
