import pandas as pd
import os
import sys
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import datetime
import global_tools as gt
import global_setting.global_dic as glv
import numpy as np
from data.data_prepared import weight_withdraw,mkt_data,prod_info
def sql_path():
    yaml_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'project_config', 'trackingrealtime_sql.yaml')
    return yaml_path
global inputpath_sql
inputpath_sql=sql_path()
class portfolio_tracking:
    def __init__(self, realtime_data_stock, realtime_data_future, realtime_data_etf, realtime_data_option,
                 realtime_data_cb, realtime_data_adj):
        self.realtime_data_stock, self.realtime_data_future, self.realtime_data_etf, self.realtime_data_option, self.realtime_data_cb, self.realtime_data_adj = \
            realtime_data_stock, realtime_data_future, realtime_data_etf, realtime_data_option, realtime_data_cb, realtime_data_adj
        today = datetime.date.today()
        self.ww=weight_withdraw()
        self.date = gt.strdate_transfer(today)
        self.now = datetime.datetime.now().replace(tzinfo=None)
    def index_type_decision(self,x):
        if 'hs300' in x:
            return '沪深300'
        elif 'sz50' in x:
            return '上证50'
        elif 'zz500' in x:
            return '中证500'
        elif 'zzA500' in x:
            return '中证A500'
        elif 'zz1000' in x:
            return '中证1000'
        elif 'zz2000' in x:
            return '中证2000'
        elif 'top' in x:
            return '中证500'
        else:
            return None
    def paperportfolio_calculation(self):
        portfolio_list=self.ww.portfolio_list_getting()
        df_por=pd.DataFrame()
        return_list=[]
        excess_list=[]
        for portfolio_name in portfolio_list:
            index_type=self.index_type_decision(portfolio_name)
            df=self.ww.portfolio_withdraw(portfolio_name)
            df_yes=self.ww.portfolio_withdraw(portfolio_name,yes=False)
            if len(df)>0:
                df_final= gt.portfolio_analyse_manual(self.date, self.date, df_yes,
                                                           df, False,
                                                           self.realtime_data_stock, self.realtime_data_future,
                                                           self.realtime_data_etf, self.realtime_data_option,
                                                           self.realtime_data_cb, self.realtime_data_adj, realtime=True,index_type=index_type)
                paper_return=df_final['paper_return'].tolist()[0]
                excess_return=df_final['excess_return'].tolist()[0]
            else:
                paper_return = 0
                excess_return = 0
            return_list.append(paper_return)
            excess_list.append(excess_return)
        df_por['portfolio_name'] = portfolio_list
        df_por['valuation_date']=self.date
        df_por['Excess_Return_bp']=excess_list
        df_por['Portfolio_Return_bp']=return_list
        df_por['update_time']=self.now
        df_por['Excess_Return_bp']=round(df_por['Excess_Return_bp']*10000,2)
        df_por['Portfolio_Return_bp'] = round(df_por['Portfolio_Return_bp'] * 10000, 2)
        return df_por
    def productportfolio_calculation(self):
        product_list=self.ww.product_list_getting()
        df_por=pd.DataFrame()
        return_list=[]
        excess_list=[]
        for product_code in product_list:
            pi=prod_info(product_code)
            index_type=pi.get_product_detail('index')
            df=self.ww.product_withdraw(product_code)
            df_yes=self.ww.product_withdraw(product_code,yes=False)
            if len(df)>0:
                df_final= gt.portfolio_analyse_manual(self.date, self.date, df_yes,
                                                           df, False,
                                                           self.realtime_data_stock, self.realtime_data_future,
                                                           self.realtime_data_etf, self.realtime_data_option,
                                                           self.realtime_data_cb, self.realtime_data_adj, realtime=True,index_type=index_type)
                paper_return=df_final['paper_return'].tolist()[0]
                excess_return=df_final['excess_return'].tolist()[0]
            else:
                paper_return = 0
                excess_return = 0
            return_list.append(paper_return)
            excess_list.append(excess_return)
        df_por['product_code'] = product_list
        df_por['valuation_date']=self.date
        df_por['Excess_Return_bp']=excess_list
        df_por['Portfolio_Return_bp']=return_list
        df_por['update_time']=self.now
        df_por['Excess_Return_bp']=round(df_por['Excess_Return_bp']*10000,2)
        df_por['Portfolio_Return_bp'] = round(df_por['Portfolio_Return_bp'] * 10000, 2)
        return df_por
    def portfolioTracking_main(self):
        df_port=self.paperportfolio_calculation()
        df_prod=self.productportfolio_calculation()
        if len(df_port)>0:
            sm = gt.sqlSaving_main(inputpath_sql, 'portfolioreturn', delete=True)
            sm.df_to_sql(df_port, 'product_code')
        if len(df_prod)>0:
            sm = gt.sqlSaving_main(inputpath_sql, 'productreturn', delete=True)
            sm.df_to_sql(df_prod, 'product_code')


if __name__ == '__main__':
    rd = mkt_data()
    realtime_data_stock, realtime_data_future, realtime_data_etf, realtime_data_option, realtime_data_cb, realtime_data_adj = rd.realtimeData_withdraw()
    pt = portfolio_tracking( realtime_data_stock, realtime_data_future, realtime_data_etf,
                            realtime_data_option, realtime_data_cb, realtime_data_adj)
    # print(pt.product_info_processing())
    print(pt.portfolioTracking_main())
