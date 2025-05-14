import os
import sys
sys.path.append('D:\global_tools_func')
import global_tools as gt
import pandas as pd
def cross_section_risk_withdraw(target_date,score_name):
    inputpath='D:\kai\D\Optimizer_V5_data\processing_data'
    inputpath=os.path.join(inputpath,score_name)
    inputpath=os.path.join(inputpath,target_date)
    inputpath_barra=os.path.join(inputpath,'barra_risk.csv')
    inputapth_industry=os.path.join(inputpath,'industry_risk.csv')
    df_barra=pd.read_csv(inputpath_barra,header=None)
    df_industry=pd.read_csv(inputapth_industry,header=None)
    df_barra.columns=['portfolio_risk','index_risk','proportion','TE','weight_sum']
    df_industry.columns=['portfolio_risk','index_risk','proportion']
    barra_name,industry_name=gt.factor_name_new()
    df_barra['factor_name']=barra_name[1:]
    df_industry['factor_name']=industry_name
    print(industry_name)
    print(df_barra,df_industry)
