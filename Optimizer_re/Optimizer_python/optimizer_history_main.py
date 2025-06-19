import os
import sys
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt
import global_setting.global_dic as glv
import pandas as pd
from Optimizer_python.main.optimizer_main_python import Optimizer_main
from Optimizer_python.data_prepare.data_prepare import stable_data_preparing
from call_matlab_opt import call_matlab_running_main
from Optimizer_Backtesting.optimizer_backtesting_main import history_running_main
def history_config_withdraw():
    inputpath = glv.get('backtest_config')
    df = pd.read_excel(inputpath,sheet_name='portfolio_info')
    return df
def history_optimizer_main(): #部署自动化
    df_config=history_config_withdraw()
    stable_data=stable_data_preparing()
    df_st, df_stockuniverse=stable_data.stable_data_preparing()
    opm=Optimizer_main(df_st, df_stockuniverse)
    outputpath_list,outputpath_list_yes=opm.optimizer_history_main(df_config)
    call_matlab_running_main(outputpath_list, outputpath_list_yes)
    history_running_main()
if __name__ == '__main__':
    history_optimizer_main()
    # config_path = glv.get('config_path')
    # df=gt.data_getting("SELECT * FROM data_indexcomponent WHERE valuation_date='2022-01-05' AND organization='zz2000'",config_path)
    # print(df)