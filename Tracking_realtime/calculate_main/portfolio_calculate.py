import pandas as pd
import os
import sys
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import datetime
import global_tools as gt
import global_setting.global_dic as glv
import numpy as np
from data.data_prepared import weight_withdraw,prod_info
def sql_path():
    yaml_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'project_config', 'trackingrealtime_sql.yaml')
    return yaml_path
global inputpath_sql
inputpath_sql=sql_path()
class portfolio_tracking:
    def __init__(self):
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
    def paperportfolio_withdraw(self):
        df_final=pd.DataFrame()
        portfolio_list=self.ww.portfolio_list_getting()
        for portfolio_name in portfolio_list:
            index_type=self.index_type_decision(portfolio_name)
            df=self.ww.portfolio_withdraw(portfolio_name)
            df['valuation_date']=self.date
            df['portfolio_name']=portfolio_name
            df['index_type']=index_type
            df_final=pd.concat([df_final,df])
        return df_final
    def productportfolio_withdraw(self):
        product_list=self.ww.product_list_getting()
        df_final=pd.DataFrame()
        for product_code in product_list:
            pi=prod_info(product_code)
            index_type=pi.get_product_detail('index')
            df=self.ww.product_withdraw(product_code)
            df['valuation_date']=self.date
            df['portfolio_name']=product_code
            df['index_type']=index_type
            df_final=pd.concat([df_final,df])
        return df_final
    def portfolioTracking_main(self):
        df_port_weight=self.paperportfolio_withdraw()
        portfolio_name_list=df_port_weight['portfolio_name'].unique().tolist()
        df_prod_weight=self.productportfolio_withdraw()
        df_cal=pd.concat([df_port_weight,df_prod_weight])
        df_info,df_detail=gt.portfolio_analyse(df_cal,cost_stock=0,realtime=True)
        df_info=df_info[['valuation_date','portfolio_name','paper_return','excess_paper_return']]
        df_info['update_time'] = self.now
        df_info['Excess_Return_bp'] = round(df_info['excess_paper_return'] * 10000, 2)
        df_info['Portfolio_Return_bp'] = round(df_info['paper_return'] * 10000, 2)
        df_info=df_info[['valuation_date','portfolio_name','Excess_Return_bp','Portfolio_Return_bp','update_time']]
        df_port=df_info[df_info['portfolio_name'].isin(portfolio_name_list)]
        df_prod = df_info[~(df_info['portfolio_name'].isin(portfolio_name_list))]
        df_prod.rename(columns={'portfolio_name':'product_code'},inplace=True)
        if len(df_port)>0:
            sm = gt.sqlSaving_main(inputpath_sql, 'portfolioreturn', delete=True)
            sm.df_to_sql(df_port)
        if len(df_prod)>0:
            sm = gt.sqlSaving_main(inputpath_sql, 'productreturn', delete=True)
            sm.df_to_sql(df_prod)


if __name__ == '__main__':
    config_path=glv.get('config_path')
    gt.table_manager(config_path, 'tracking_realtime', 'realtime_futureoptionholding')
    # pt=portfolio_tracking()
    # pt.portfolioTracking_main()
