import os
import pandas as pd
from Optimizer_python.main.optimizer_main_python import Optimizer_main
from Optimizer_python.data_prepare.data_prepare import stable_data_preparing
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
    outputpath_list=opm.optimizer_history_main(df_config)
    return outputpath_list

if __name__ == '__main__':
    history_optimizer_main()