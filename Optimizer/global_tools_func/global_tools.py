import pandas as pd
from datetime import time, datetime, timedelta,date
import os
import time
import numpy as np
import shutil
from scipy.io import loadmat
import warnings
import sys
# sys.path.append('../global_tools_func/global_setting')
import global_tools_func.global_setting.global_dic as glv
warnings.filterwarnings("ignore")
# print(os.getcwd())
def readcsv(filepath, dtype=None, index_col=None):
    for en in ['gbk', 'utf-8', 'ANSI', 'utf_8_sig']:
        try:
            df = pd.read_csv(filepath, encoding=en, dtype=dtype, index_col=index_col)
            return df
        except Exception as e:
            pass
    else:
        #print(f" ERR: readcsv采用四种格式读取csv 失败 filepath = {filepath}")
        return pd.DataFrame()
def factor_name_old():
    inputpath=glv.get('input_factor_old')
    input_list=os.listdir(inputpath)
    input_list=[i for i in input_list if 'LNMODELACTIVE-' in str(i)]
    input_list.sort()
    input_name=input_list[-1]
    inputpath_factor=os.path.join(inputpath,input_name)
    annots = loadmat(inputpath_factor)['lnmodel_active_daily']['factornames'][0][0][0]
    annots=[np.array(i)[0] for i in annots]
    industry_name=[i for i in annots if '\u4e00' <= i <= '\u9fff']
    barra_name=[i for i in annots if '\u4e00' > i or i > '\u9fff']
    return barra_name,industry_name
def factor_name_new():
    inputpath=glv.get('input_factor')
    input_list=os.listdir(inputpath)
    input_list=[i for i in input_list if 'LNMODELACTIVE-' in str(i) and len(i)==26]
    input_list.sort()
    input_name=input_list[-1]
    inputpath_factor=os.path.join(inputpath,input_name)
    annots = loadmat(inputpath_factor)['lnmodel_active_daily']['factornames'][0][0][0]
    annots=[np.array(i)[0] for i in annots]
    industry_name=[i for i in annots if '\u4e00' <= i <= '\u9fff']
    barra_name=[i for i in annots if '\u4e00' > i or i > '\u9fff']
    return barra_name,industry_name

def factor_universe_withdraw():
    inputpath=glv.get('input_other')
    inputpath=os.path.join(inputpath,'StockUniverse_new.csv')
    # inputpath = 'static_data/StockUniverse_new.csv'
    df_universe=readcsv(inputpath)
    return df_universe

def factor_name(inputpath_factor):
    # print(loadmat(inputpath_factor))
    annots = loadmat(inputpath_factor)['lnmodel_active_daily']['factornames'][0][0][0]
    annots = [np.array(i)[0] for i in annots]
    industry_name = [i for i in annots if '\u4e00' <= i <= '\u9fff']
    barra_name = [i for i in annots if '\u4e00' > i or i > '\u9fff']
    return barra_name, industry_name
def move_specific_files(old_path, new_path, files_to_move=None):
    """
    Move specific files from old_path to new_path.

    :param old_path: The source directory from where files are to be moved.
    :param new_path: The destination directory to which files are to be moved.
    :param files_to_move: A list of filenames to be moved. If None, all files in old_path are moved.
    """
    print("Source Directory:", old_path)
    print("Destination Directory:", new_path)

    if files_to_move is None:
        # If no specific files are mentioned, move all files
        filelist = os.listdir(old_path)
    else:
        # Only consider the specified files to move
        filelist = files_to_move

    print("Files to be moved:", filelist)
    for file in filelist:
        src = os.path.join(old_path, file)
        if not os.path.exists(src):
            print(f"File does not exist: {src}")
            continue
        dst = os.path.join(new_path, file)
        print('Moving from:', src)
        print('To:', dst)
        shutil.copy(src, dst)
#work_day系列
global df_date
inputpath_perfattribution=glv.get('input_other')
# print(inputpath_perfattribution)
inputpath_date=os.path.join(inputpath_perfattribution,'chinese_valuation_date.xlsx')
# print(inputpath_date)
df_date=pd.read_excel(inputpath_date)
def next_workday():
    today = date.today()
    today=today.strftime('%Y-%m-%d')
    try:
        index_today = df_date[df_date['valuation_date'] == today].index.tolist()[0]
        index_tommorow = index_today + 1
        tommorow = df_date.iloc[index_tommorow].tolist()[0]
    except:
        index_today = df_date[df_date['valuation_date'] >= today].index.tolist()[0]
        tommorow = df_date.iloc[index_today].tolist()[0]
    return tommorow
def last_workday():
    today = date.today()
    today=today.strftime('%Y-%m-%d')
    try:
        index_today = df_date[df_date['valuation_date'] == today].index.tolist()[0]
        index_tommorow = index_today - 1
        yesterday = df_date.iloc[index_tommorow].tolist()[0]
    except:
        index_today = df_date[df_date['valuation_date']<= today].index.tolist()[-1]
        yesterday = df_date.iloc[index_today].tolist()[0]
    return yesterday
def last_workday_calculate(x):
    today = x
    try:
       today = today.strftime('%Y-%m-%d')
    except:
        today=today
    yesterday = df_date[df_date['valuation_date'] < today]['valuation_date'].tolist()[-1]
    # print(f'--{yesterday}--')
    # index_tommorow = index_today - 1
    # yesterday = df_date.iloc[index_tommorow].tolist()[0]
    return yesterday
def next_workday_calculate(x):
    today = x
    try:
       today = today.strftime('%Y-%m-%d')
    except:
        today=today
    try:
        index_today = df_date[df_date['valuation_date'] == today].index.tolist()[0]
        index_tommorow = index_today + 1
        tommorow = df_date.iloc[index_tommorow].tolist()[0]
    except:
        index_today = df_date[df_date['valuation_date'] >= today].index.tolist()[0]
        tommorow = df_date.iloc[index_today].tolist()[0]
    return tommorow

def last_workday_calculate2(df_score):
    df_final=pd.DataFrame()
    date_list = df_score['date'].unique().tolist()
    for date in date_list:
        slice_df = df_score[df_score['date'] == date]
        yesterday = last_workday_calculate(date)
        slice_df['date'] = yesterday
        df_final=pd.concat([df_final,slice_df])
    return df_final
def is_workday(today):
    try:
        df_date2=df_date[df_date['valuation_date'] == today]
    except:
        df_date2=pd.DataFrame()
    if len(df_date2)!=1:
        return False
    else:
        return True
def working_days(df):
    date_list=df_date['valuation_date'].tolist()
    df=df[df['date'].isin(date_list)]
    return df
def is_workday2():
        today = date.today()
        today = today.strftime('%Y-%m-%d')
        try:
            df_date2 = df_date[df_date['valuation_date'] == today]
        except:
            df_date2 = pd.DataFrame()
        if len(df_date2) != 1:
            return False
        else:
            return True
def is_month_firstweek():
    today = date.today()
    today = today.strftime('%Y-%m-%d')
    inputpath_first = os.path.join(inputpath_perfattribution, 'month_first_6days.xlsx')
    df_date=pd.read_excel(inputpath_first)
    date_list=df_date['valuation_date'].tolist()
    if today in date_list:
        return True
    else:
        return False
def intdate_transfer(date):
    date=pd.to_datetime(date)
    date=date.strftime('%Y%m%d')
    return date
def strdate_transfer(date):
    date=pd.to_datetime(date)
    date=date.strftime('%Y-%m-%d')
    return date
def working_days_list(start_date,end_date):
    inputpath_date = os.path.join(inputpath_perfattribution, 'chinese_valuation_date.xlsx')
    df_date = pd.read_excel(inputpath_date)
    df_date.rename(columns={'valuation_date':'date'},inplace=True)
    df_date=df_date[(df_date['date']>='2014-12-31')&(df_date['date']<='2030-01-01')]
    df_date['target_date']=df_date['date']
    df_date.dropna(inplace=True)
    df_date=df_date[(df_date['date']>=start_date)&(df_date['date']<=end_date)]
    date_list=df_date['target_date'].tolist()
    return date_list
def working_day_count(start_date,end_date):
    inputpath_date = os.path.join(inputpath_perfattribution, 'chinese_valuation_date.xlsx')
    df_date = pd.read_excel(inputpath_date)
    #start_date=datetime.strptime(start_date,'%Y-%m-%d')
    #end_date = datetime.strptime(end_date, '%Y-%m-%d')
    slice_df_date=df_date[df_date['valuation_date']>start_date]
    slice_df_date = slice_df_date[slice_df_date['valuation_date'] <= end_date]
    total_day=len(slice_df_date)
    return total_day
def month_lastday():
    df_date['year_month']=df_date['valuation_date'].apply(lambda x: str(x)[:7])
    month_lastday=df_date.groupby('year_month')['valuation_date'].tail(1).tolist()
    return month_lastday

def last_weeks_lastday():
    today=date.today()
    today=today.strftime('%Y-%m-%d')
    inputpath_lastday=os.path.join(inputpath_perfattribution, 'weeks_lastday.xlsx')
    df_lastday=pd.read_excel(inputpath_lastday)
    lastday=df_lastday[df_lastday['valuation_date']<today]['valuation_date'].tolist()[-1]
    return lastday
def weeks_firstday(date):
    inputpath_firstday=os.path.join(inputpath_perfattribution, 'weeks_firstday.xlsx')
    df_firstday=pd.read_excel(inputpath_firstday)
    firstday=df_firstday[df_firstday['valuation_date']<date]['valuation_date'].tolist()[-1]
    return firstday

def last_weeks_lastday2(date):
    date=pd.to_datetime(date)
    date = date.strftime('%Y-%m-%d')
    inputpath_lastday = os.path.join(inputpath_perfattribution, 'weeks_lastday.xlsx')
    df_lastday = pd.read_excel(inputpath_lastday)
    lastday = df_lastday[df_lastday['valuation_date'] < date]['valuation_date'].tolist()[-1]
    return lastday
# print(last_weeks_lastday2(date='2025-01-08'))
#指数数据系列
def index_shortname(index_type):
    if index_type == '上证50':
        return 'sz50'
    elif index_type == '沪深300':
        return 'hs300'
    elif index_type == '中证500':
        return 'zz500'
    elif index_type == '中证1000':
        return 'zz1000'
    elif index_type == '中证2000':
        return 'zz2000'
    else:
        return 'zzA500'
def index_weight_withdraw(index_type,available_date): #提取指数权重股数据
    available_date2=intdate_transfer(available_date)
    inputpath_index=glv.get('input_indexcomponent')
    short_name=index_shortname(index_type)
    inputpath_index=os.path.join(inputpath_index,short_name)
    inputpath_index=file_withdraw(inputpath_index,available_date2)
    if inputpath_index==None:
        raise ValueError
    else:
        df = readcsv(inputpath_index, dtype=str)
        df = df[['code', 'weight']]
    return df
def crossSection_index_return_withdraw(index_type,available_date):
    available_date2 = intdate_transfer(available_date)
    inputpath_indexreturn=glv.get('input_indexreturn')
    inputpath_index = file_withdraw(inputpath_indexreturn, available_date2)
    if inputpath_index!=None:
        df = readcsv(inputpath_index, dtype=str)
        index_return = df[index_type].tolist()[0]
        return index_return
    else:
         raise ValueError
def crossSection_index_factorexposure_withdraw_new(index_type,available_date):
    available_date2=intdate_transfer(available_date)
    inputpath_indexexposure=glv.get('input_index_exposure')
    short_name = index_shortname(index_type)
    inputpath_indexexposure = os.path.join(inputpath_indexexposure, short_name)
    file_name = file_withdraw(inputpath_indexexposure, available_date2)
    if file_name==None:
        df=pd.DataFrame()
    else:
        inputpath_index = os.path.join(inputpath_indexexposure, file_name)
        df = readcsv(inputpath_index, dtype=str)
    return df
def crossSection_index_factorexposure_withdraw_old(index_type,available_date):
    available_date2=intdate_transfer(available_date)
    inputpath_indexexposure=glv.get('input_index_exposure_old')
    short_name = index_shortname(index_type)
    inputpath_indexexposure = os.path.join(inputpath_indexexposure, short_name)
    file_name = file_withdraw(inputpath_indexexposure, available_date2)
    if file_name==None:
        df=pd.DataFrame()
    else:
        inputpath_index = os.path.join(inputpath_indexexposure, file_name)
        df = readcsv(inputpath_index, dtype=str)
    return df
def timeSeries_index_return_withdraw():
    inputpath_indexreturn=glv.get('input_timeSeries')
    inputpath_index = os.path.join(inputpath_indexreturn, 'index_return.csv')
    df = readcsv(inputpath_index, dtype=str)
    df['valuation_date'] = pd.to_datetime(df['valuation_date'])
    df['valuation_date'] = df['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    return df
#portfolio系列回测
def portfolio_return_calculate(df,target_date):
    df=weight_sum_check(df)
    target_date2=intdate_transfer(target_date)
    inputpath_stockreturn=glv.get('input_stockreturn')
    input_name=file_withdraw(inputpath_stockreturn,target_date2)
    inputpath_stockreturn=os.path.join(inputpath_stockreturn,input_name)
    df_stock=readcsv(inputpath_stockreturn)
    df_stock.set_index('valuation_date',inplace=True)
    df_stock=df_stock.T
    df_stock.reset_index(inplace=True)
    df_stock.columns=['code','return']
    df=df.merge(df_stock,on='code',how='left')
    df.fillna(0,inplace=True)
    df['portfolio']=df['return']*df['weight']
    portfolio_return=df['portfolio'].sum()
    return portfolio_return
def portfolio_excess_return_calculate(df,target_date,index_type):
    portfolio_return=portfolio_return_calculate(df,target_date)
    index_return=crossSection_index_return_withdraw(index_type,target_date)
    excess_portfolio_return=float(portfolio_return)-float(index_return)
    return excess_portfolio_return
#Score分数生成
def rr_score_processing(df_score): #标准化分数生成
    date_list = df_score['valuation_date'].unique().tolist()
    final_list_score = []
    for i in date_list:
         slice_df=df_score[df_score['valuation_date']==i]
         stock_code2=slice_df['code'].unique().tolist()
         list_score=np.array(range(len(stock_code2)))
         list_score=(list_score-np.mean(list_score))/np.std(list_score)
         list_score=list(reversed(list_score))
         final_list_score=final_list_score+list_score
         df_score['final_score'] = final_list_score
    df_score.reset_index(inplace=True, drop=True)
    df_score['valuation_date'] = pd.to_datetime(df_score['valuation_date'])
    df_score['valuation_date'] = df_score['valuation_date'].astype(str)
    return df_score
def code_transfer(df):
    df.dropna(subset=['code'],axis=0,inplace=True)
    df['code'] = df['code'].astype(int)
    df['code'] = df['code'].apply(lambda x: '{:06d}'.format(x))

    def sz_sh(x):
        if str(x)[0] == '6':
            x = str(x) + '.SH'
        else:
            x = str(x) + '.SZ'
        return x

    df['code'] = df['code'].apply(lambda x: sz_sh(x))
    return df
def chunks(lst, n):
    """等分list"""
    return [lst[i::n] for i in range(n)]
def file_withdraw(inputpath,available_date):
    input_list=os.listdir(inputpath)
    try:
        file_name=[file for file in input_list if available_date in file][0]
    except:
        print('there is not available_date that you search in the file'+inputpath)
        file_name=None
    if file_name !=None:
         inputpath_result=os.path.join(inputpath,file_name)
         return inputpath_result
    else:
         return None

def folder_creator(inputpath):
    try:
        os.mkdir(inputpath)
    except:
        pass

def weight_sum_check(df):
    weight_sum=df['weight'].sum()
    if weight_sum<0.99:
        df['weight']=df['weight']/weight_sum
    else:
        df=df
    return df
def weight_sum_warning(df):
    weight_sum = df['weight'].sum()
    if weight_sum < 0.99 or weight_sum>1.01:
        print('warning'+str(weight_sum))
    else:
        print(weight_sum)

def folder_creator2(path):#创建路径
    # 检查路径是否存在
    if not os.path.exists(path):
        # 如果路径不存在，则创建路径
        os.makedirs(path, exist_ok=True)
        print(f"目录 {path} 已创建.")

def folder_creator3(file_path):#创建文件的路径
    # 获取文件所在的目录路径
    directory = os.path.dirname(file_path)

    # 检查目录是否存在
    if not os.path.exists(directory):
        # 如果目录不存在，则创建目录
        os.makedirs(directory, exist_ok=True)
        print(f"目录 {directory} 已创建.")

def next_weeks_lastday2(date):
    date=pd.to_datetime(date)
    date = date.strftime('%Y-%m-%d')
    inputpath_lastday = os.path.join(inputpath_perfattribution, 'weeks_lastday.xlsx')
    df_lastday = pd.read_excel(inputpath_lastday)
    lastday = df_lastday[df_lastday['valuation_date'] > date]['valuation_date'].tolist()[0]
    return lastday

def code_transfer2(df):
    df.dropna(subset=['code'],axis=0,inplace=True)
    df['code'] = df['code'].astype(int)
    df['code'] = df['code'].apply(lambda x: '{:06d}'.format(x))
    def sz_sh(x):
        if str(x)[:2] == '11':
            x = str(x) + '.SH'
        else:
            x = str(x) + '.SZ'
        return x
    df['code'] = df['code'].apply(lambda x: sz_sh(x))
    return df