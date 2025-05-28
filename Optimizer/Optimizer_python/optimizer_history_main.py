import os
import sys
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt
import pandas as pd
from Optimizer_python.main.optimizer_main_python import Optimizer_main
from Optimizer_python.data_prepare.data_prepare import stable_data_preparing
from call_matlab_opt import call_matlab_running_main
def history_config_withdraw():
    inputpath = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    inputpath=os.path.join(inputpath,'config_history.xlsx')
    df = pd.read_excel(inputpath)
    return df
def history_optimizer_main(): #部署自动化
    df_config=history_config_withdraw()
    stable_data=stable_data_preparing()
    df_st, df_stockuniverse=stable_data.stable_data_preparing()
    opm=Optimizer_main(df_st, df_stockuniverse)
    outputpath_list,outputpath_list_yes=opm.optimizer_history_main(df_config)
    call_matlab_running_main(outputpath_list, outputpath_list_yes)
def matlab_test(portfolio_name,start_date,end_date):
    inputpath=os.path.join('D:\Optimizer_python_data\processing_data',portfolio_name)
    working_days_list=gt.working_days_list(start_date,end_date)
    input_list=[]
    input_list_yes=[]
    for days in working_days_list:
        yes=gt.last_workday_calculate(days)
        daily_inputpath=os.path.join(inputpath,days)
        daily_inputpath_yes=os.path.join(inputpath,yes)
        input_list.append(daily_inputpath)
        input_list_yes.append(daily_inputpath_yes)
    #call_matlab_running_main(input_list, input_list_yes)
if __name__ == '__main__':
    history_optimizer_main()