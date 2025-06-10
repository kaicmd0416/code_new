import os
import pandas as pd
import global_setting.global_dic as glv
import sys
from datetime import datetime
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt
global source,config_path
source=glv.get('source')
config_path=glv.get('config_path')
class holding_construction:
    def __init__(self,df_portfolio,df_mkt,stock_money):
        self.df_portfolio=df_portfolio
        self.df_mkt=df_mkt
        self.stock_money=stock_money
    def consturction_main(self):
        df=self.df_portfolio.copy()
        df_mkt=self.df_mkt.copy()
        df['target_money']=df['weight']*self.stock_money
        df=df.merge(df_mkt,on='code',how='left')
        df['close']=df['close'].astype(float)
        slice_df=df[(df['close'].isna())|(df['close']==0)]
        missing_list=[]
        if len(slice_df)>0:
            print(slice_df)
            print('以上数据为空')
            missing_list=slice_df['code'].tolist()
        if len(missing_list)>0:
            df=df[~(df['code'].isin(missing_list))]
        df['quantity']=df['target_money']/df['close']
        df['quantity']=round(df['quantity']/100,0)*100
        df=df[['code','quantity']]
        return df

