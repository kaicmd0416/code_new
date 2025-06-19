import pandas as pd
import os
import sys
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import json
import global_tools as gt
import global_setting.global_dic as glv
from Optimizer_Backtesting.backtesting.backtesting_history import Back_testing_processing
global source,config_path,config_path2
source=glv.get('source')
config_path=glv.get('config_path')
config_path2=glv.get('backtest_config')
class backtesting_main:
    def __init__(self):
        self.df_index_return=self.index_return_withdraw()
        self.df_stock_return=self.stock_return_withdraw()
    def index_return_withdraw(self):
        df=gt.timeSeries_index_return_withdraw()
        return df
    def stock_return_withdraw(self):
        inputpath_stockreturn = glv.get('input_timeseriesstockreturn')
        df=gt.data_getting(inputpath_stockreturn,config_path)
        if source=='sql':
              df=df[['valuation_date','code','pct_chg']]
              df=gt.sql_to_timeseries(df)
        df['valuation_date'] = pd.to_datetime(df['valuation_date'])
        df['valuation_date'] = df['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df.set_index('valuation_date',inplace=True,drop=True)
        df=df.astype(float)
        df.reset_index(inplace=True)
        return df

    def optimizer_history_backtesting_main(self,df_config):
        bt = Back_testing_processing(self.df_index_return, self.df_stock_return)
        df_config['start_date']=pd.to_datetime(df_config['start_date'])
        df_config['end_date'] = pd.to_datetime(df_config['end_date'])
        df_config['start_date']=df_config['start_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df_config['end_date'] = df_config['end_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        for i in range(len(df_config)):
            start_date=df_config['start_date'].tolist()[i]
            end_date=df_config['end_date'].tolist()[i]
            #end_date=gt.last_workday_calculate(end_date)
            score_name=df_config['portfolio_name'].tolist()[i]
            index_type=df_config['index_type'].tolist()[i]
            user_name=df_config['user_name'].tolist()[i]
            bt.back_testing_main_history(index_type, score_name,user_name, start_date, end_date)

if __name__ == '__main__':
    bm=backtesting_main()
    print(bm.df_index_return())
    print(bm.df_stock_return())


