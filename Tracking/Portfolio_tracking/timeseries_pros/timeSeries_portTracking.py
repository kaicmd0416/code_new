import datetime
import os
import pandas as pd
import Portfolio_tracking.global_setting.global_dic as glv
import global_tools_func.global_tools as gt
from Portfolio_tracking.tools_func.tools_func import *
def portfolio_name_list(index_type):
    inputpath_config = glv.get('portfolio_dic')
    df_config = pd.read_excel(inputpath_config)
    df_config['new_name']=df_config['score_name'].apply(lambda x: str(x)[:4])
    df_config=df_config[df_config['new_name']!='vp01']
    df_config['is_top']=df_config['score_name'].apply(lambda x: 'top' in x)
    df_config=df_config[df_config['is_top']==False]
    portfolio_list=df_config[df_config['index_type']==index_type]['score_name'].tolist()
    portfolio_list=['valuation_date'] + portfolio_list
    if index_type=='中证500':
        top_list=['a1_top200','a3_top200','timeselecting_zz500']
        portfolio_list=portfolio_list+top_list
<<<<<<< Updated upstream
=======
    if index_type=='沪深300':
        top_list=['timeselecting_hs300','timeselecting_hs300_pro','a1_hs300_top150','a3_hs300_top150','a1_hs300_top50','a3_hs300_top50']
        portfolio_list=portfolio_list+top_list
    if index_type=='中证1000':
        top_list = ['a1_top200', 'a3_top200', 'a3_zz2000_top100']
        portfolio_list = portfolio_list + top_list
    if index_type=='中证A500':
        top_list = ['timeselecting_zzA500','timeselecting_zzA500_pro']
        portfolio_list = portfolio_list + top_list
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
    return portfolio_list
def inputpath_withdraw(index_type):
    index_short = index_shortname_withdraw(index_type)
    inputpath = glv.get('cross_section_output')
    inputpath_excess = os.path.join(inputpath, 'excess_return')
    inputpath_return = os.path.join(inputpath, 'return')
    inputpath_excess = os.path.join(inputpath_excess, index_short)
    inputpath_return = os.path.join(inputpath_return, index_short)
    return inputpath_excess,inputpath_return
def cross_section_data_withdraw(target_date,index_type):
    inputpath1,inputpath2=inputpath_withdraw(index_type)
    target_date2=gt.intdate_transfer(target_date)
    inputpath_excess=gt.file_withdraw(inputpath1,target_date2)
    inputpath_return=gt.file_withdraw(inputpath2,target_date2)
    df_excess=gt.readcsv(inputpath_excess)
    df_return = gt.readcsv(inputpath_return)
    return df_excess,df_return
def Netvalue_processing(df):
    df=df.copy()
    df.set_index('valuation_date',inplace=True,drop=True)
    df=(1+df).cumprod()
    df.reset_index(inplace=True)
    return df
def mode_decision(inputpath,inputpath2):
    inputpath_excess, inputpath_return = inputpath_withdraw(index_type='沪深300')
    date_list = se_date_withdraw(inputpath_excess)
    end_date=date_list[-1]
    mode = 'w'
    sheet_exist = None
    status='Not_run'
    try:
        df = pd.read_excel(inputpath)
        df2 = pd.read_excel(inputpath2)
    except:
        df=pd.DataFrame()
        df2=pd.DataFrame()
    if len(df)!=0 and len(df2)!=0:
        columns_list = portfolio_name_list(index_type='沪深300')
        bu_list = list(set(columns_list) - set(df2.columns.tolist()))

        now_date_list = df['valuation_date'].tolist()
        requiring_date = list(set(date_list) - set(now_date_list))
        if len(bu_list)>0:
            status='run'
        else:
            if len(requiring_date)>0:
                mode='a'
                sheet_exist = 'replace'
                status = 'run'
    else:
        status='run'
    return status,mode,sheet_exist,end_date


def PortTracking_timeSeries_main():
    inputpath_original=glv.get('output_portfolio')
    gt.folder_creator2(inputpath_original)
    inputpath=os.path.join(inputpath_original,'portfolio_return.xlsx')
    inputpath2=os.path.join(inputpath_original,'portfolio_excessReturn.xlsx')
    inputpath3=os.path.join(inputpath_original,'portfolio_netvalue.xlsx')
    inputpath4=os.path.join(inputpath_original,'portfolio_excessNetvalue.xlsx')
    #判断是否有文件
    status, mode, sheet_exist,end_date=mode_decision(inputpath,inputpath2)
    if status=='run':
        with pd.ExcelWriter(inputpath, mode=mode, engine='openpyxl',
                            if_sheet_exists=sheet_exist) as writer, pd.ExcelWriter(inputpath2, mode=mode,
                                                                                   engine='openpyxl',
                                                                                   if_sheet_exists=sheet_exist) as writer2, pd.ExcelWriter(
            inputpath3, engine='openpyxl') as writer3, pd.ExcelWriter(inputpath4, engine='openpyxl') as writer4:
            for index_type in ['沪深300', '中证500', '中证A500', '中证1000']:
                inputpath_excess, inputpath_return = inputpath_withdraw(index_type)
                columns_list = portfolio_name_list(index_type)
                date_list = se_date_withdraw(inputpath_excess)
                try:
                    df = pd.read_excel(writer, sheet_name=index_type)
                    df2 = pd.read_excel(writer2, sheet_name=index_type)
                except:
                    df = pd.DataFrame(columns=columns_list)
                    df2 = pd.DataFrame(columns=columns_list)
                if len(df) == 0 or len(df2) == 0:
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
                        df_excess, df_return = cross_section_data_withdraw(date, index_type)
                        bu_list = list(set(columns_list) - set(df_excess.columns.tolist()))
                        if len(bu_list) != 0:
                            df_excess[bu_list] = None
                            df_return[bu_list] = None
                        df_excess = df_excess[columns_list]
                        df_return = df_return[columns_list]
                        df = pd.concat([df, df_return], ignore_index=False)
                        df2 = pd.concat([df2, df_excess], ignore_index=False)
                    df.sort_values(by='valuation_date', ascending=True, inplace=True)
                    df2.sort_values(by='valuation_date', ascending=True, inplace=True)
                    df3 = Netvalue_processing(df)
                    df4 = Netvalue_processing(df2)
                    df.to_excel(writer, sheet_name=index_type, index=False)
                    df2.to_excel(writer2, sheet_name=index_type, index=False)
                    df3.to_excel(writer3, sheet_name=index_type, index=False)
                    df4.to_excel(writer4, sheet_name=index_type, index=False)
    else:
        print('portfolio_return 和 portfolio_excess_return已经更新到最新日期:'+str(end_date))
