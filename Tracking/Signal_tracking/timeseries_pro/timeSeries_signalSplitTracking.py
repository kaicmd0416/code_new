import datetime
import os
import pandas as pd
import Signal_tracking.global_setting.global_dic as glv
import global_tools_func.global_tools as gt
from Signal_tracking.tools_func.tools_func import *
def inputpath_withdraw(portfolio_name):
    inputpath=glv.get('signal_split_tracking')
    inputpath=os.path.join(inputpath,portfolio_name)
    return inputpath
def cross_section_data_withdraw(target_date,portfolio_name,index_type):
    inputpath=inputpath_withdraw(portfolio_name)
    target_date2=gt.intdate_transfer(target_date)
    inputpath=gt.file_withdraw(inputpath,target_date2)
    try:
        df=gt.readcsv(inputpath)
        slice_df=df[df['index_type']==index_type]
    except:
        slice_df=pd.DataFrame()
    return slice_df
def net_value_processing(df):
    df.set_index('valuation_date',inplace=True,drop=True)
    df.fillna(0,inplace=True)
    df=(1+df.astype(float)/10000).cumprod()
    df.reset_index(inplace=True)
    return df
def mode_decision(inputpath,inputpath2,portfolio_name_list):
    inputpath_excess= inputpath_withdraw(portfolio_name_list[0])
    date_list = se_date_withdraw(inputpath_excess)
    end_date=date_list[-1]
    mode = 'w'
    sheet_exist = None
    status='Not_run'
    try:
        df = pd.read_excel(inputpath)
        df2 = pd.read_excel(inputpath2)
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
def SignalTracking_timeSeries_main_port(portfolio_name_list):
    inputpath_original=glv.get('output_signal_split')
    for portfolio_name in portfolio_name_list:
        inputpath_original1=os.path.join(inputpath_original,portfolio_name)
        gt.folder_creator2(inputpath_original1)
        inputpath = os.path.join(inputpath_original1, str(portfolio_name)+'Split_excessReturn.xlsx')
        inputpath2 = os.path.join(inputpath_original1, str(portfolio_name)+'signalSplit_excessNetvalue.xlsx')
        # 判断是否有文件
        status, mode, sheet_exist, end_date = mode_decision(inputpath, inputpath2, portfolio_name_list)
        if status == 'run':
            with pd.ExcelWriter(inputpath, mode=mode, engine='openpyxl',
                                if_sheet_exists=sheet_exist) as writer, pd.ExcelWriter(
                inputpath2, engine='openpyxl') as writer2:
                inputpath_ps = inputpath_withdraw(portfolio_name)
                date_list = se_date_withdraw(inputpath_ps)
                for index_type in ['沪深300', '中证500', '中证1000', '中证A500']:
                    try:
                        df = pd.read_excel(writer, sheet_name=index_type)
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
                            slice_df = cross_section_data_withdraw(date, portfolio_name, index_type)
                            if len(slice_df)>0:
                                 df = pd.concat([df, slice_df], ignore_index=False)
                        df.sort_values(by='valuation_date', ascending=True, inplace=True)
                        df.to_excel(writer, sheet_name=index_type, index=False)
                        df.drop('index_type', inplace=True, axis=1)
                        df2 = net_value_processing(df)
                        df2.to_excel(writer2, sheet_name=index_type, index=False)
        else:
            print(str(portfolio_name)+'Split_excessReturn和'+str(portfolio_name)+'signalSplit_excessNetvalue已经更新到最新日期:' + str(end_date))



