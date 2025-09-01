import pandas as pd
from datetime import time, datetime, timedelta,date
import os
import time
import numpy as np
import shutil
from scipy.io import loadmat
import warnings
import sys
sys.path.append('D:\Optimizer\global_tools_func\global_setting')
import global_dic as glv
warnings.filterwarnings("ignore")

inputpath_perfattribution=glv.get('input_other')
inputpath_data_optimizer=glv.get('input_index')
inputpath_date=os.path.join(inputpath_perfattribution,'chinese_valuation_date.xlsx')
df_date=pd.read_excel(inputpath_date)
def weeks_firstday_lastday_withdraw(df_date):
    outputpath_fist=os.path.join(inputpath_perfattribution,'weeks_fistday.xlsx')
    outputpath_last = os.path.join(inputpath_perfattribution, 'weeks_lastday.xlsx')
    df_date['valuation_date']=pd.to_datetime(df_date['valuation_date'])
    df_date['yesterday']=df_date['valuation_date'].shift(1)
    df_date.dropna(inplace=True)
    df_date['difference']=df_date['valuation_date']-df_date['yesterday']
    df_date['difference']=df_date['difference'].apply(lambda x: x.days)
    df_date=df_date[df_date['difference']>1]
    df_date['valuation_date']=df_date['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    df_date['yesterday'] = df_date['yesterday'].apply(lambda x: x.strftime('%Y-%m-%d'))
    df_date_first_day=df_date['valuation_date']
    df_date_last_day = df_date[['yesterday']]
    df_date_last_day.columns=['valuation_date']
    df_date_first_day.to_excel(outputpath_fist,index=False)
    df_date_last_day.to_excel(outputpath_last, index=False)
def month_first7days_withdraw(df_date):
    df_final=pd.DataFrame()
    outputpath_first=os.path.join(inputpath_perfattribution,'month_fist_7days.xlsx')
    df_date['year_month']=df_date['valuation_date'].apply(lambda x: str(x)[:7])
    year_month_list=df_date['year_month'].unique().tolist()
    for year_month in year_month_list:
        slice_df_date=df_date[df_date['year_month']==year_month]
        slice_df_date.reset_index(inplace=True,drop=True)
        slice_df_date=slice_df_date.loc[:6]
        df_final=pd.concat([df_final,slice_df_date])
    df_final.to_excel(outputpath_first,index=False)

