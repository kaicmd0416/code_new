import Signal_tracking.global_setting.global_dic as glv
import global_tools_func.global_tools as gt
import os
import pandas as pd
from Signal_tracking.main.running_main import analyse_main
import datetime
from datetime import date
from timeseries_pro.timeSeries_weightTracking import ScoreTracking_timeSeries_main_weight
from timeseries_pro.timeSeries_portSplitTracking import ScoreTracking_timeSeries_main_port
from timeseries_pro.timeSeries_signalSplitTracking import SignalTracking_timeSeries_main_port
from sql.signalTracking_sql import SignalTracking_sql
def timeSeries_main(portfolio_name_list,base_score):
    ScoreTracking_timeSeries_main_weight(portfolio_name_list)
    ScoreTracking_timeSeries_main_port(portfolio_name_list)
    SignalTracking_timeSeries_main_port(base_score)
def tracking_score_withdraw():
    inputpath=glv.get('valid_score')
    df=pd.read_excel(inputpath)
    base_score = df['base_score'].unique().tolist()
    base_score=[i for i in base_score if str(i)[:2]=='rr' or str(i)[:4]=='vp02']
    score_name_list=df['score_name'].tolist()
    index_type_list=['沪深300','中证500','中证1000','中证A500']
    return score_name_list,base_score,index_type_list
def cross_section_update_main(target_date):
    score_name_list,base_score,index_type_list=tracking_score_withdraw()
    arm=analyse_main(target_date)
    arm.score_running_main(base_score,index_type_list)
    arm.portfolio_running_main(score_name_list)
    return score_name_list,base_score
def update_main(): #触发这个
    today=date.today()
    if gt.is_workday2()==True:
        target_date=today
    else:
        target_date=gt.last_workday_calculate(today)
    target_date=gt.strdate_transfer(target_date)
    start_date=target_date
    for i in range(3):
        start_date=gt.last_workday_calculate(start_date)
    working_days_list=gt.working_days_list(start_date,target_date)
    for target_date2 in working_days_list:
        print(target_date2)
        try:
            score_name_list, base_score = cross_section_update_main(target_date2)
        except:
            print('signal_tracking在' + str(target_date2) + '更新存在错误')
    try:
        timeSeries_main(score_name_list, base_score)
    except:
        print('signal_tracking时序更新在'+str(target_date)+'更新存在错误')
    try:
        sts= SignalTracking_sql(start_date,target_date)
        sts.signalSplit_sql_main()
    except:
        print('signal_tracking更新sql在'+str(target_date)+'更新存在错误')

def history_main(start_date,end_date):
    working_days_list=gt.working_days_list(start_date,end_date)
    for target_date in working_days_list:
        print(target_date)
        try:
             score_name_list,base_score=cross_section_update_main(target_date)
        except:
            print('signal_tracking日频更新在'+str(target_date)+'更新存在错误')
    try:
        timeSeries_main(score_name_list, base_score)
    except:
        print('signal_tracking时序更新在'+str(target_date)+'更新存在错误')
    try:
        sts= SignalTracking_sql(start_date,target_date)
        sts.signalSplit_sql_main()
    except:
        print('signal_tracking更新sql在'+str(target_date)+'更新存在错误')

if __name__ == '__main__':
    update_main()
    #history_main(start_date='2025-01-01',end_date='2025-03-17')
