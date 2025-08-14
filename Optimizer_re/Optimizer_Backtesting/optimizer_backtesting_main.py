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
    df_config['start_date'] = pd.to_datetime(df_config['start_date'])
    df_config['end_date'] = pd.to_datetime(df_config['end_date'])
    start_date= df_config['start_date'].min()
    end_date= df_config['end_date'].max()
    bm=backtesting_main(start_date,end_date)
    bm.optimizer_history_backtesting_main(df_config)

if __name__ == '__main__':
    history_running_main()
    pass
