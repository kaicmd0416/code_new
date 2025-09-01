import sys
import os
import pandas as pd
import datetime
from Portfolio_tracking.portfolio_performance.portfolio_weight_withdraw import portfolio_weight_withdraw
from Portfolio_tracking.real_time.realtimeStock_sqlSaving import realtimeStock_sqlSaving
import Portfolio_tracking.global_setting.global_dic as glv
import global_tools_func.global_tools as gt


def index_code_withdraw(index_type):
    if index_type == '沪深300':
        index_code = '000300.SH'
    elif index_type == '中证500':
        index_code = '000905.SH'
    elif index_type=='中证1000':
        index_code = '000852.SH'
    elif index_type=='中证A500':
        index_code='000510.CSI'
    elif index_type=='中证2000':
         index_code='932000.CSI'
    else:
        raise ValueError
    return index_code


def real_time_stock_return():
    inputpath = glv.get('realtime_data')
    df = pd.read_excel(inputpath, sheet_name='stockprice')
    df_etf=pd.read_excel(inputpath,sheet_name='etf_info')
    df = df[['代码', 'return']]
    df.columns = ['code', 'return']
    df['return']=df['return']/100
    df_etf['return']=(df_etf['现价']-df_etf['前收'])/df_etf['前收']
    df_etf=df_etf[['代码','return']]
    df_etf.columns= ['code', 'return']
    df=pd.concat([df,df_etf])
    return df

def real_time_index_return():
    inputpath = glv.get('realtime_data')
    df = pd.read_excel(inputpath, sheet_name='indexreturn')
    columns_list=df.columns.tolist()
    if '000510.SH' in columns_list:
        df.rename(columns={'000510.SH':'000510.CSI'},inplace=True)
    return df

def realtime_portfolio_return(df_weight, df_return, df_index, index_type):
    df_weight['code']=df_weight['code'].apply(lambda x: str(x)[:6])
    df_weight=gt.code_transfer(df_weight)
    df_weight = df_weight.merge(df_return, on='code', how='left')
    df_weight['portfolio_return'] = df_weight['weight'] * df_weight['return']
    portfolio_return = df_weight['portfolio_return'].sum()
    index_code = index_code_withdraw(index_type)
    index_return = df_index[index_code].tolist()[0]
    excess_return = portfolio_return - index_return
    return excess_return, portfolio_return
def portfolio_index_withdraw(score_name):
    inputpath = os.path.split(os.path.realpath(__file__))[0]
    inputpath = os.path.join(os.path.dirname(inputpath), 'realtime_config.xlsx')
    df_config = pd.read_excel(inputpath)
    index_type=df_config[df_config['score_name']==score_name]['index_type'].tolist()[0]
    return index_type
def procut_excess_return_calculate(df_final,df_index):
    inputpath_config=glv.get('product_detail')
    df_proindex=pd.read_excel(inputpath_config,sheet_name='product_detail')
    xls=pd.ExcelFile(inputpath_config)
    sheet_name_list=xls.sheet_names
    portfolio_return_list=[]
    excess_return_list=[]
    for product_name in sheet_name_list[1:]:
        index_type=df_proindex[df_proindex['product_name']==product_name]['index_type'].tolist()[0]
        index_code = index_code_withdraw(index_type)
        index_return = df_index[index_code].tolist()[0]
        index_return_bp=round(index_return*10000,2)
        df = pd.read_excel(inputpath_config,sheet_name=product_name)
        df=df.merge(df_final,on='score_name',how='left')
        df['excess_return(bp)'] = df['portfolio_return(bp)'] - index_return_bp
        df['portfolio_return(bp)']=df['portfolio_return(bp)']*df['weight']
        df['excess_return(bp)'] = df['excess_return(bp)']*df['weight']
        portfolio_return=df['portfolio_return(bp)'].sum()
        excess_return=df['excess_return(bp)'].sum()
        portfolio_return_list.append(portfolio_return)
        excess_return_list.append(excess_return)
    df_final=pd.DataFrame()
    df_final['product_name']=sheet_name_list[1:]
    df_final['portfolio_return(bp)']=portfolio_return_list
    df_final['excess_return(bp)']=excess_return_list
    return df_final
def stock_realtime_main(): #实时触发这个
    outputpath=glv.get('realtime_output')
    #outputpath2=glv.get('realtime_output2')
    gt.folder_creator2(outputpath)
    #outputpath2 = gt.folder_creator2(outputpath2)
    outputpath=os.path.join(outputpath,'portfolio_realtime.xlsx')
    #outputpath2=os.path.join(outputpath2,'portfolio_realtime.xlsx')
    inputpath = os.path.split(os.path.realpath(__file__))[0]
    inputpath = os.path.join(os.path.dirname(inputpath), 'realtime_config.xlsx')
    df_config=pd.read_excel(inputpath)
    score_name_list=df_config['score_name'].tolist()
    df_final = pd.DataFrame()
    date = datetime.date.today()
    date = gt.strdate_transfer(date)
    df_index = real_time_index_return()
    df_return = real_time_stock_return()
    excess_return_list = []
    index_type_list = []
    portfolio_return_list = []
    score_name_list2=[]
    for score_name in score_name_list:
        df_weight = portfolio_weight_withdraw(score_name, date,yesterday=False)
        if len(df_weight)==0:
            pass
        else:
            index_type = portfolio_index_withdraw(score_name)
            excess_return, portfolio_return = realtime_portfolio_return(df_weight, df_return, df_index, index_type)
            excess_return = excess_return * 10000
            portfolio_return = portfolio_return * 10000
            excess_return = round(excess_return, 2)
            portfolio_return = round(portfolio_return, 2)
            score_name_list2.append(score_name)
            excess_return_list.append(excess_return)
            portfolio_return_list.append(portfolio_return)
            index_type_list.append(index_type)
    df_final['score_name'] = score_name_list
    df_final['excess_return(bp)'] = excess_return_list
    df_final['portfolio_return(bp)'] = portfolio_return_list
    df_final2=procut_excess_return_calculate(df_final, df_index)
    rs=realtimeStock_sqlSaving(df_final,df_final2)
    rs.realtimeStock_savingmain()
    with pd.ExcelWriter(outputpath, engine='openpyxl') as writer:
         df_final.to_excel(writer,sheet_name='portfolio_realtime',index=False)
         df_final2.to_excel(writer, sheet_name='product_realtime', index=False)
    # with pd.ExcelWriter(outputpath2, engine='openpyxl') as writer:
    #      df_final.to_excel(writer,sheet_name='portfolio_realtime',index=False)
    #      df_final2.to_excel(writer, sheet_name='product_realtime', index=False)
