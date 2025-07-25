import pandas as pd
import os
import sys
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import datetime
import global_tools as gt
import global_setting.global_dic as glv
import numpy as np
from data.data_prepared import futureoption_position,security_position,prod_info,mkt_data
def sql_path():
    yaml_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'project_config', 'trackingrealtime_sql.yaml')
    return yaml_path
global inputpath_sql
inputpath_sql=sql_path()
class product_tracking:
    def __init__(self,product_code,realtime_data_stock, realtime_data_future,realtime_data_etf,realtime_data_option,realtime_data_cb,realtime_data_adj):
        self.product_code=product_code
        fp=futureoption_position(product_code)
        self.df_future_ori, self.df_option_ori=fp.futureoption_withdraw()
        df_future,df_future_yes=self.df_processing(self.df_future_ori)
        self.df_indexFuture,self.df_commFuture,self.df_bond=self.future_split(df_future)
        self.df_indexFuture_yes,self.df_commFuture_yes,self.df_bond_yes=self.future_split(df_future_yes)
        self.df_option,self.df_option_yes=self.df_processing(self.df_option_ori)
        sp=security_position(product_code)
        self.df_stock_ori, self.df_etf_ori, self.df_cb_ori=sp.security_withdraw()
        self.df_stock,self.df_stock_yes=self.df_processing(self.df_stock_ori)
        self.df_etf,self.df_etf_yes=self.df_processing(self.df_etf_ori)
        self.df_cb,self.df_cb_yes=self.df_processing(self.df_cb_ori)
        pi=prod_info(product_code)
        self.asset_value=pi.assetvalue_withdraw()
        self.realtime_data_stock, self.realtime_data_future,self.realtime_data_etf,self.realtime_data_option,self.realtime_data_cb,self.realtime_data_adj=\
        realtime_data_stock, realtime_data_future, realtime_data_etf, realtime_data_option, realtime_data_cb, realtime_data_adj
        today = datetime.date.today()
        self.date = gt.strdate_transfer(today)
        self.now=datetime.datetime.now().replace(tzinfo=None)
    def direction_prossing(self, x):
        if '空' in x:
            return -1
        else:
            return 1
    def futureOption_processing(self,df):
        def direction_processing2(x):
            if x>0:
                return 'long'
            else:
                return 'short'
        df['direction']=df['quantity'].apply(lambda x: direction_processing2(x))
        df['quantity']=abs(df['quantity'])
        return df
    def df_processing(self,df):
        if len(df)>0:
            if 'direction' in df.columns:
                df['direction'] = df['direction'].apply(lambda x: self.direction_prossing(x))
            else:
                df['direction'] = 1
            df['quantity'] = df['quantity'] * df['direction']
            df['pre_quantity'] = df['pre_quantity'] * df['direction']
            df_today = df[['code', 'quantity']]
            df_yes = df[['code', 'pre_quantity']]
            df_yes.columns = ['code', 'quantity']
        else:
            df_today=pd.DataFrame()
            df_yes=pd.DataFrame()
        return df_today,df_yes
    def future_split(self,df):
        df['new_code'] = df['code'].apply(lambda x: str(x)[:1])
        df_future = df[~(df['new_code'] == 'T')]
        df_indexFuture=df_future[df_future['new_code']=='I']
        df_commFuture=df_future[~(df_future['new_code']=='I')]
        df_bond = df[df['new_code'] == 'T']
        df_indexFuture.drop(columns='new_code',inplace=True)
        df_commFuture.drop(columns='new_code', inplace=True)
        df_bond.drop(columns='new_code',inplace=True)
        return df_indexFuture,df_commFuture,df_bond
    def indexfuture_analysis(self):
        df = pd.DataFrame()
        if len(self.df_indexFuture)>0:
            df_final,df = gt.portfolio_analyse_manual(self.date, self.date,self.df_indexFuture_yes, self.df_indexFuture,True,
                                                   self.realtime_data_stock, self.realtime_data_future,
                                                   self.realtime_data_etf, self.realtime_data_option,
                                                   self.realtime_data_cb, self.realtime_data_adj,realtime=True)
            future_profit = df_final['portfolio_profit'].tolist()[0]
            future_mktvalue = df_final['portfolio_mktvalue'].tolist()[0]
        else:
            future_profit=0
            future_mktvalue=0
        if len(df)>0:
           df=df[['code','quantity','delta','mkt_value','profit']]
           df=self.futureOption_processing(df)
        return future_profit,future_mktvalue,df
    def commfuture_analysis(self):
        df=pd.DataFrame()
        if len(self.df_commFuture)>0:
            df_final,df = gt.portfolio_analyse_manual(self.date, self.date,self.df_commFuture_yes,self.df_commFuture,True,
                                                   self.realtime_data_stock, self.realtime_data_future,
                                                   self.realtime_data_etf, self.realtime_data_option,
                                                   self.realtime_data_cb, self.realtime_data_adj,realtime=True)
            future_profit = df_final['portfolio_profit'].tolist()[0]
            future_mktvalue = df_final['portfolio_mktvalue'].tolist()[0]
        else:
            future_profit=0
            future_mktvalue=0
        if len(df)>0:
           df=df[['code','quantity','delta','mkt_value','profit']]
           df=self.futureOption_processing(df)
        return future_profit,future_mktvalue,df
    def option_analysis(self):
        df = pd.DataFrame()
        if len(self.df_option)>0:
            df_final,df = gt.portfolio_analyse_manual(self.date, self.date, self.df_option_yes, self.df_option,True,
                                                   self.realtime_data_stock, self.realtime_data_future,
                                                   self.realtime_data_etf, self.realtime_data_option,
                                                   self.realtime_data_cb, self.realtime_data_adj,realtime=True)
            option_profit = df_final['portfolio_profit'].tolist()[0]
            option_mktvalue = df_final['portfolio_mktvalue'].tolist()[0]
        else:
            option_profit=0
            option_mktvalue=0
        if len(df)>0:
           df=df[['code','quantity','delta','mkt_value','profit']]
           df=self.futureOption_processing(df)
        return option_profit,option_mktvalue,df
    def stock_analysis(self):
        if len(self.df_stock)>0:
            df_final = gt.portfolio_analyse_manual(self.date, self.date, self.df_stock_yes, self.df_stock,False,
                                                   self.realtime_data_stock, self.realtime_data_future,
                                                   self.realtime_data_etf, self.realtime_data_option,
                                                   self.realtime_data_cb, self.realtime_data_adj,realtime=True)
            stock_profit = df_final['portfolio_profit'].tolist()[0]
            stock_mktvalue = df_final['portfolio_mktvalue'].tolist()[0]
        else:
            stock_profit=0
            stock_mktvalue=0
        return stock_profit,stock_mktvalue
    def etf_analysis(self):
        if len(self.df_etf)>0:
            df_final = gt.portfolio_analyse_manual(self.date, self.date, self.df_etf_yes, self.df_etf,False,
                                                   self.realtime_data_stock, self.realtime_data_future,
                                                   self.realtime_data_etf, self.realtime_data_option,
                                                   self.realtime_data_cb, self.realtime_data_adj,realtime=True)
            etf_profit = df_final['portfolio_profit'].tolist()[0]
            etf_mktvalue = df_final['portfolio_mktvalue'].tolist()[0]
        else:
            etf_profit=0
            etf_mktvalue=0
        return etf_profit,etf_mktvalue
    def cb_analysis(self):
        if len(self.df_cb)>0:
            df_final = gt.portfolio_analyse_manual(self.date, self.date, self.df_cb_yes, self.df_cb,False,
                                                   self.realtime_data_stock, self.realtime_data_future,
                                                   self.realtime_data_etf, self.realtime_data_option,
                                                   self.realtime_data_cb, self.realtime_data_adj,realtime=True)
            cb_profit = df_final['portfolio_profit'].tolist()[0]
            cb_mktvalue = df_final['portfolio_mktvalue'].tolist()[0]
        else:
            cb_profit=0
            cb_mktvalue=0
        return cb_profit,cb_mktvalue
    def bond_analysis(self):
        if len(self.df_cb)>0:
            df_final = gt.portfolio_analyse_manual(self.date, self.date, self.df_bond_yes, self.df_bond,False,
                                                   self.realtime_data_stock, self.realtime_data_future,
                                                   self.realtime_data_etf, self.realtime_data_option,
                                                   self.realtime_data_cb, self.realtime_data_adj,realtime=True)
            bond_profit = df_final['portfolio_profit'].tolist()[0]
            bond_mktvalue = df_final['portfolio_mktvalue'].tolist()[0]
        else:
            bond_profit=0
            bond_mktvalue=0
        return bond_profit,bond_mktvalue
    def trading_action_processing(self):
        df_stock=self.df_stock_ori.copy()
        df_stock['type']='stock'
        df_future=self.df_future_ori.copy()
        df_future['type']='future'
        df_option=self.df_option_ori.copy()
        df_option['type']='option'
        df_etf=self.df_etf_ori.copy()
        df_etf['type']='etf'
        df_final=pd.concat([df_stock,df_future,df_option,df_etf])
        df_final=df_final[['code','quantity','pre_quantity','direction','type']]
        df_final['difference'] = df_final['quantity'] - df_final['pre_quantity']
        def action_decision(x):
            if x>0:
                return '开仓'
            elif x==0:
                return '不变'
            else:
                return '平仓'
        def direction_changing(x):
            if x==1:
                return 'long'
            else:
                return 'short'
        df_final['action']=df_final['difference'].apply(lambda x: action_decision(x))
        df_final['direction']=df_final['direction'].apply(lambda x: direction_changing(x))
        df_final=df_final[~(df_final['action']=='不变')]
        df_final['product_code']=self.product_code
        df_final['valuation_date']=self.date
        df_final['update_time']=self.now
        df_final['simulation']=False
        df_final.rename(columns={'quantity':'HoldingQty','pre_quantity':'HoldingQty_yes'},inplace=True)
        return df_final
    def futureoption_holding_processing(self,df_future,df_option):
        df_fo = pd.concat([df_future, df_option])
        df_fo['valuation_date']=self.date
        df_fo['product_code']=self.product_code
        df_fo['simulation']=False
        df_fo.rename(columns={'quantity':'HoldingQty','profit':'daily_profit'},inplace=True)
        df_fo['update_time']=self.now
        return df_fo
    def product_info_processing(self):
        df_info=pd.DataFrame()
        indexfuture_profit, indexfuture_mktvalue, df_indexfuture=self.indexfuture_analysis()
        commfuture_profit, commfuture_mktvalue, df_commfuture = self.commfuture_analysis()
        option_profit, option_mktvalue, df_option=self.option_analysis()
        stock_profit, stock_mktvalue=self.stock_analysis()
        etf_profit, etf_mktvalue=self.etf_analysis()
        cb_profit, cb_mktvalue=self.cb_analysis()
        bond_profit, bond_mktvalue=self.bond_analysis()
        indexfuture_proportion=indexfuture_mktvalue/self.asset_value
        option_proportion=option_mktvalue/self.asset_value
        stock_proportion=stock_mktvalue/self.asset_value
        etf_proportion=etf_mktvalue/self.asset_value
        cb_proportion=cb_mktvalue/self.asset_value
        leverage_ratio=indexfuture_proportion+option_proportion+stock_proportion+etf_proportion+cb_proportion
        profit_sum=stock_profit+etf_profit+option_profit+indexfuture_profit+commfuture_profit+bond_profit+cb_profit
        product_return=profit_sum/self.asset_value
        product_return=round(product_return*10000,2)
        profit_name_list=['股票盈亏','ETF_盈亏','期权盈亏','股指期货盈亏','商品期货盈亏','国债盈亏','可转债盈亏']
        profit_value_list=[stock_profit,etf_profit,option_profit,indexfuture_profit,commfuture_profit,bond_profit,cb_profit]
        proportion_name_list=['股票占比','期货占比','期权占比','ETF占比','可转债占比']
        proportion_value_list=[stock_proportion,indexfuture_proportion,option_proportion,etf_proportion,cb_proportion]
        other_name_list=['杠杆率','股票市值','资产净值','总资产预估收益率(bp)']
        other_value_list=[leverage_ratio,stock_mktvalue,self.asset_value,product_return]
        df_info['type']=other_name_list+profit_name_list+proportion_name_list
        df_info['value']=other_value_list+profit_value_list+proportion_value_list
        df_info['value']=round(df_info['value'],3)
        df_info['product_code']=self.product_code
        df_info['simulation']=False
        df_info['update_time']=self.now
        df_info['valuation_date']=self.date
        df_fo=self.futureoption_holding_processing(df_indexfuture,df_option)
        return df_info,df_fo
    def productTracking_main(self):
        df_info,df_fo=self.product_info_processing()
        df_action=self.trading_action_processing()
        if len(df_info)>0:
            sm = gt.sqlSaving_main(inputpath_sql, 'proinfo',delete=True)
            sm.df_to_sql(df_info,'product_code',self.product_code)
        if len(df_fo)>0:
            sm2 = gt.sqlSaving_main(inputpath_sql, 'optionfuture_holding',delete=True)
            sm2.df_to_sql(df_fo,'product_code',self.product_code)
        if len(df_action)>0:
            sm3 = gt.sqlSaving_main(inputpath_sql, 'holding_changing', delete=True)
            sm3.df_to_sql(df_action, 'product_code', self.product_code)









if __name__ == '__main__':
    rd=mkt_data()
    realtime_data_stock, realtime_data_future, realtime_data_etf, realtime_data_option, realtime_data_cb, realtime_data_adj=rd.realtimeData_withdraw()
    pt=product_tracking('SNY426',realtime_data_stock, realtime_data_future, realtime_data_etf, realtime_data_option, realtime_data_cb, realtime_data_adj)
    #print(pt.product_info_processing())
    print(pt.productTracking_main())















