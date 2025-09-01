import os
import pandas as pd
import sys
import datetime as datetime
sys.path.append('D:\global_tools_func')
import global_tools as gt
import global_setting.global_dic as glv
def se_date_withdraw(inputpath):
    input_list=os.listdir(inputpath)
    input_s=input_list[0]
    index=input_s.rindex('_')
    date_list=[gt.strdate_transfer(str(i)[index+1:-4]) for i in input_list]
    return date_list
def portfolio_list_withdraw():
    inputpath=glv.get('mode_dic')
    df=pd.read_excel(inputpath)
    portfolio_list=df['score_name'].tolist()
    return portfolio_list
def index_shortname_withdraw(index_type):
    if index_type=='沪深300':
        return 'hs300'
    if index_type=='中证500':
        return 'zz500'
    if index_type=='中证A500':
        return 'zzA500'
    if index_type=='中证1000':
        return 'zz1000'
