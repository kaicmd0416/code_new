import pandas as pd
import os
import sys
import Optimizer_Backtesting.global_setting.global_dic as glv
import global_tools_func.global_tools as gt
from Optimizer_Backtesting.backtesting.backtesting_history import Back_testing_processing
class backtesting_main:
    def __init__(self):
        self.df_index_return=self.index_return_withdraw()
        self.df_stock_return=self.stock_return_withdraw()
    def portfolio_index_finding(self,score_name):
        inputpath_mode_dic=glv.get('mode_dic')
        df_mode=pd.read_excel(inputpath_mode_dic)
        index_type=df_mode[df_mode['score_name']==score_name]['index_type'].tolist()[0]
        return index_type
    def index_return_withdraw(self):
        inputpath_indexreturn = glv.get('input_timeSeries')
        inputpath_index = os.path.join(inputpath_indexreturn, 'index_return.csv')
        df = gt.readcsv(inputpath_index, dtype=str)
        df['valuation_date'] = pd.to_datetime(df['valuation_date'])
        df['valuation_date'] = df['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df.set_index('valuation_date',inplace=True,drop=True)
        df=df.astype(float)
        df.reset_index(inplace=True)
        return df

    def stock_return_withdraw(self):
        inputpath_stockreturn = glv.get('input_timeSeries')
        inputpath_stockreturn = os.path.join(inputpath_stockreturn, 'stock_return.csv')
        df = gt.readcsv(inputpath_stockreturn, dtype=str)
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
            score_name=df_config['score_name'].tolist()[i]
            index_type=self.portfolio_index_finding(score_name)
            bt.back_testing_main_history(index_type, score_name, start_date, end_date)




