import datetime
import os
import pandas as pd
import Signal_tracking.global_setting.global_dic as glv
import global_tools_func.global_tools as gt
from Signal_tracking.tools_func.tools_func import *
def inputpath_withdraw():
    inputpath=glv.get('weight_tracking')
    inputpath=os.path.join(inputpath)
    return inputpath
def cross_section_data_withdraw(target_date,portfolio_name):
    inputpath=inputpath_withdraw()
    target_date2=gt.intdate_transfer(target_date)
    inputpath=gt.file_withdraw(inputpath,target_date2)
    df=gt.readcsv(inputpath)
    slice_df = df[df['portfolio_name'] == portfolio_name]
    return slice_df
def mode_decision(inputpath,portfolio_name_list):
    inputpath_excess= inputpath_withdraw()
    date_list = se_date_withdraw(inputpath_excess)
    end_date=date_list[-1]
    mode = 'w'
    sheet_exist = None
    status='Not_run'
    try:
        df = pd.read_excel(inputpath)
        status2='exist'
    except:
        status2='Not_exist'
    if status2=='exist':
        try:
            xls = pd.ExcelFile(inputpath)
            sheet_names = xls.sheet_names
        except:
            sheet_names=[]
        if sheet_names==portfolio_name_list:
            now_date_list = df['valuation_date'].tolist()
            requiring_date = list(set(date_list) - set(now_date_list))
            if len(requiring_date) > 0:
                mode = 'a'
                sheet_exist = 'replace'
                status = 'run'
        else:
            mode = 'w'
            sheet_exist = None
            status = 'run'
    else:
        status='run'
    return status, mode, sheet_exist, end_date
def ScoreTracking_timeSeries_main_weight(portfolio_name_list):
    inputpath_original=glv.get('output_weight')
    gt.folder_creator2(inputpath_original)
    inputpath=os.path.join(inputpath_original,'portflioSplit_weight.xlsx')
    #判断是否有文件
    status, mode, sheet_exist, end_date=mode_decision(inputpath,portfolio_name_list)
    if status == 'run':
        with pd.ExcelWriter(inputpath, mode=mode, engine='openpyxl', if_sheet_exists=sheet_exist) as writer:
            inputpath_ps = inputpath_withdraw()
            date_list = se_date_withdraw(inputpath_ps)
            for portfolio_name in portfolio_name_list:
                try:
                    df = pd.read_excel(writer, sheet_name=portfolio_name)
                except:
                    df = pd.DataFrame()
                if len(df) == 0:
                    start_date = date_list[0]
                    end_date = date_list[-1]
                    running_date_list = gt.working_days_list(start_date, end_date)
                else:
                    now_date_list = df['valuation_date'].tolist()
                    requiring_date = list(set(date_list) - set(now_date_list))
                    if len(requiring_date) > 0:
                        running_date_list = requiring_date
                    else:
                        running_date_list = []
                if len(running_date_list) > 0:
                    for date in running_date_list:
                        slice_df = cross_section_data_withdraw(date, portfolio_name)
                        df = pd.concat([df, slice_df], ignore_index=False)
                    df.sort_values(by='valuation_date', ascending=True, inplace=True)
                    df.drop('portfolio_name', inplace=True, axis=1)
                    df.to_excel(writer, sheet_name=portfolio_name, index=False)
    else:
        print('portflioSplit_weight已经更新到最新日期:'+str(end_date))

