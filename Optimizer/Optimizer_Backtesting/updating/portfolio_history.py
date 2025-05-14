import sys
from datetime import date
import datetime
sys.path.append('D:\OneDrive\global_tools_func')
import Optimizer_Backtesting.global_setting.global_dic as glv
import global_tools_func.global_tools as gt
import pandas as pd
import os
def portfolio_updating(score_name_list,start_date,end_date):
    inputpath_portfolio=glv.get('portfolio_data')
    outputpath_weight=glv.get('output_weight')
    working_days_list=gt.working_days_list(start_date,end_date)
    for score_name in score_name_list:
        daily_outputpath = os.path.join(outputpath_weight, score_name)
        gt.folder_creator2(daily_outputpath)
        for target_date in working_days_list:
            target_date2 = gt.intdate_transfer(target_date)
            daily_outputpath2 = os.path.join(daily_outputpath, str(score_name) + '_' + target_date2 + '.csv')
            daily_inputpath = os.path.join(inputpath_portfolio, score_name)
            daily_inputpath2 = os.path.join(daily_inputpath, target_date)
            inputpath_weight = os.path.join(daily_inputpath2, 'weight.csv')
            inputpath_code = os.path.join(daily_inputpath2, 'Stock_code.csv')
            try:
                df_weight = pd.read_csv(inputpath_weight, header=None)
                df_code = pd.read_csv(inputpath_code)
            except:
                print(str(score_name) + '没有更新')
                continue
            df_weight.columns = ['weight']
            df_code.columns = ['code']
            df_final = pd.concat([df_code, df_weight], axis=1)
            if df_final['weight'].sum() < 0.99 or df_final['weight'].sum() > 1.01:
                print(str(score_name) + '没有更新')
            df_final['weight'] = df_final['weight'] / df_final['weight'].sum()
            df_final.to_csv(daily_outputpath2, index=False)
def all_score_name_list_withdraw():
    inputpath=glv.get('mode_dic')
    df=pd.read_excel(inputpath)
    score_name_list=df['score_name'].tolist()
    return score_name_list
def all_portfolio_updating(start_date,end_date):
    score_name_list=all_score_name_list_withdraw()
    portfolio_updating(score_name_list, start_date, end_date)

