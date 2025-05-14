import datetime
import os
import pandas as pd
import Product_tracking.global_setting.global_dic as glv
import global_tools_func.global_tools as gt
from Product_tracking.tools_func.tools_func2 import *
from Product_tracking.tools_func.tools_func import *
import numpy as np
def inputpath_withdraw(product_name):
    inputpath = glv.get('product_tracking')
    inputpath=os.path.join(inputpath,product_name)
    return inputpath
def valid_productname():
    inputpath = glv.get('product_tracking')
    product_list=os.listdir(inputpath)
    return product_list
def cross_section_data_withdraw(target_date,product_name):
    inputpath=inputpath_withdraw(product_name)
    target_date2=gt.intdate_transfer(target_date)
    inputpath_product=gt.file_withdraw(inputpath,target_date2)
    df_info = pd.read_excel(inputpath_product,sheet_name='产品表现汇总')
    df_factor=pd.read_excel(inputpath_product,sheet_name='产品风险因子暴露')
    return df_info,df_factor
def factor_return_calculate(df_factor,target_date):
    inputpath=glv.get('factor_return')
    target_date2=gt.intdate_transfer(target_date)
    inputpath2=gt.file_withdraw(inputpath,target_date2)
    df_factor_return=gt.readcsv(inputpath2)
    df_factor_return.drop(columns='valuation_date',inplace=True)
    df_factor2=df_factor.copy()
    factor_exposure = list(
                np.array(np.multiply(np.mat(df_factor2.values), np.mat(df_factor_return.values).T)))
    df_factor_return=pd.DataFrame(factor_exposure,columns=df_factor2.columns.tolist())
    return df_factor_return
def factor_processing(df_factor,df_factor_return,factor_name_list):
    df_final=pd.DataFrame()
    for name in df_factor.columns:
        name2=name[:-9]
        slice_df1=df_factor[[name]]
        slice_df2=df_factor_return[[name]]
        slice_df1['factor_name']=factor_name_list
        slice_df2['factor_name']=factor_name_list
        slice_df1['factor_name']=slice_df1['factor_name'].apply(lambda x: name2+'_'+str(x)+'_exposure')
        slice_df2['factor_name'] = slice_df2['factor_name'].apply(lambda x: name2 + '_' + str(x) + '_return')
        slice_df1.set_index('factor_name',inplace=True,drop=True)
        slice_df2.set_index('factor_name', inplace=True,drop=True)
        slice_df1=slice_df1.T
        slice_df2=slice_df2.T
        slice_df1.reset_index(inplace=True,drop=True)
        slice_df2.reset_index(inplace=True,drop=True)
        df_final=pd.concat([df_final,slice_df1],axis=1)
        df_final=pd.concat([df_final,slice_df2],axis=1)
    return df_final

def data_processing(target_date,product_name):
    df_info,df_factor=cross_section_data_withdraw(target_date,product_name)
    df_factor.rename(columns={'difference':'net_exposure'},inplace=True)
    factor_name_list=df_factor['factor_name'].tolist()
    df_factor=df_factor[['stock_exposure','future_exposure','option_exposure','cb_exposure','portfolio_exposure','net_exposure']]
    product_type2=product_type(product_name)
    if product_type2!='指增':
        df_factor['net_exposure']=df_factor['portfolio_exposure']
    df_factor.fillna(0,inplace=True)
    df_factor = df_factor.replace([np.inf, -np.inf], 0)
    df_factor_return=factor_return_calculate(df_factor,target_date)
    df_factor_info=factor_processing(df_factor, df_factor_return, factor_name_list)
    product_list=df_info['product_split'].tolist()
    product_list_1=[str(i)+'_超额(本身)' for i in product_list]
    product_list_2 = [str(i) + '超额贡献' for i in product_list]
    product_list2=product_list_2+['杠杆率','股票占比','期货占比','期权占比','可转债占比','etf占比']
    info_list=df_info[df_info['info_name'].isin(['杠杆率','股票占比','期货占比','期权占比','可转债占比','etf占比'])]['money'].tolist()
    yingkui_list=df_info['盈亏'].tolist()
    excess_list=df_info['超额贡献bp'].tolist()
    excess_list2=df_info['超额(本身)bp'].tolist()
    return_list=df_info['收益率(本身)bp'].tolist()
    df_proinfo=pd.DataFrame(columns=product_list2)
    df_return=pd.DataFrame(columns=product_list)
    df_excessreturn=pd.DataFrame(columns=product_list_1)
    df_yingkui=pd.DataFrame(columns=product_list)
    df_return.loc['add_row']=return_list
    df_excessreturn.loc['add_row'] = excess_list2
    df_proinfo.loc['add_row'] = excess_list+info_list
    df_yingkui.loc['add_row'] = yingkui_list
    df_return['valuation_date']=target_date
    df_excessreturn['valuation_date']=target_date
    df_yingkui['valuation_date']=target_date
    df_proinfo['valuation_date']=target_date
    df_return=df_return[['valuation_date']+df_return.columns.tolist()[:-1]]
    df_excessreturn= df_excessreturn[['valuation_date'] + df_excessreturn.columns.tolist()[:-1]]
    df_yingkui =  df_yingkui[['valuation_date'] + df_yingkui.columns.tolist()[:-1]]
    df_proinfo= df_proinfo[['valuation_date'] + df_proinfo.columns.tolist()[:-1]]
    df_proinfo.reset_index(inplace=True,drop=True)
    df_factor_info.reset_index(inplace=True,drop=True)
    df_proinfo=pd.concat([df_proinfo,df_factor_info],axis=1)
    df_yingkui.drop(columns='杠杆率',inplace=True)
    return df_return,df_excessreturn,df_yingkui,df_proinfo
def mode_decision(inputpath,inputpath2,inputpath3,inputpath4,product_name):
    inputpath_excess= inputpath_withdraw(product_name)
    date_list = se_date_withdraw(inputpath_excess)
    end_date=date_list[-1]
    mode = 'w'
    sheet_exist = None
    status='Not_run'
    try:
        df = pd.read_excel(inputpath)
        df2 = pd.read_excel(inputpath2)
        df3 = pd.read_excel(inputpath3)
        df4 = pd.read_excel(inputpath4)
        status2='exist'
    except:
        status2='Not_exist'
    if status2=='exist':
        try:
            df = pd.read_excel(inputpath, sheet_name=product_name)
            df2 = pd.read_excel(inputpath2, sheet_name=product_name)
            df3 = pd.read_excel(inputpath3, sheet_name=product_name) 
            df4 = pd.read_excel(inputpath4, sheet_name=product_name)
        except:
            df = pd.DataFrame()
            df2 = pd.DataFrame()
            df3 = pd.DataFrame()
            df4 = pd.DataFrame()
        if len(df) != 0 and len(df2) != 0 and len(df3) != 0 and len(df4) != 0:
            now_date_list = df['valuation_date'].tolist()
            requiring_date = list(set(date_list) - set(now_date_list))
            if len(requiring_date) > 0:
                mode = 'a'
                sheet_exist = 'replace'
                status = 'run'
        else:
            mode = 'a'
            sheet_exist = 'replace'
            status = 'run'
    else:
        status='run'
    return status,mode,sheet_exist,end_date
def ProdTracking_timeSeries_main(product_name):
    inputpath_original = glv.get('output_product')
    gt.folder_creator2(inputpath_original)
    inputpath3 = os.path.join(inputpath_original, 'product_yingkui.xlsx')
    inputpath = os.path.join(inputpath_original, 'product_return.xlsx')
    inputpath2 = os.path.join(inputpath_original, 'product_excessreturn.xlsx')
    inputpath4 = os.path.join(inputpath_original, 'product_proinfo.xlsx')
    # 判断是否有文件
    status,mode,sheet_exist,end_date=mode_decision(inputpath,inputpath2,inputpath3,inputpath4,product_name)
    if status=='run':
        with pd.ExcelWriter(inputpath, mode=mode, engine='openpyxl',
                            if_sheet_exists=sheet_exist) as writer, pd.ExcelWriter(inputpath2, mode=mode,
                                                                                   engine='openpyxl',
                                                                                   if_sheet_exists=sheet_exist) as writer2, pd.ExcelWriter(inputpath3, mode=mode,
                                                                                   engine='openpyxl',
                                                                                   if_sheet_exists=sheet_exist) as writer3,pd.ExcelWriter(inputpath4, mode=mode,
                                                                                   engine='openpyxl',
                                                                                   if_sheet_exists=sheet_exist) as writer4:
            inputpath_pro = inputpath_withdraw(product_name)
            date_list = se_date_withdraw(inputpath_pro)
            try:
                df = pd.read_excel(writer, sheet_name=product_name)
                df2 = pd.read_excel(writer2, sheet_name=product_name)
                df3 = pd.read_excel(writer3, sheet_name=product_name)
                df4 = pd.read_excel(writer4, sheet_name=product_name)
            except:
                df = pd.DataFrame()
                df2 = pd.DataFrame()
                df3 = pd.DataFrame()
                df4 = pd.DataFrame()
            if len(df) == 0 or len(df2) == 0 or len(df3) == 0 or len(df4) == 0:
                running_date_list = date_list
            else:
                now_date_list = df['valuation_date'].tolist()
                requiring_date = list(set(date_list) - set(now_date_list))
                if len(requiring_date) > 0:
                    running_date_list = requiring_date
                else:
                    running_date_list = []
            if len(running_date_list) > 0:
                for date in running_date_list:
                    df_return, df_excessreturn, df_yingkui, df_proinfo = data_processing(date, product_name)
                    df = pd.concat([df, df_return], ignore_index=False)
                    df2 = pd.concat([df2, df_excessreturn], ignore_index=False)
                    df3 = pd.concat([df3, df_yingkui], ignore_index=False)
                    df4 = pd.concat([df4, df_proinfo], ignore_index=False)
                df.sort_values(by='valuation_date', ascending=True, inplace=True)
                df2.sort_values(by='valuation_date', ascending=True, inplace=True)
                df3.sort_values(by='valuation_date', ascending=True, inplace=True)
                df.to_excel(writer, sheet_name=product_name, index=False)
                df2.to_excel(writer2, sheet_name=product_name, index=False)
                df3.to_excel(writer3, sheet_name=product_name, index=False)
                df4.to_excel(writer4, sheet_name=product_name, index=False)
    else:
        print(str(product_name)+'的product_return,product_excessreturn,product_yingkui和product_proinfo已经更新到最新日期:' + str(end_date))