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
class portfolio_tracking:
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
    pt=portfolio_tracking('SNY426',realtime_data_stock, realtime_data_future, realtime_data_etf, realtime_data_option, realtime_data_cb, realtime_data_adj)
    #print(pt.product_info_processing())
    print(pt.productTracking_main())












    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    # #辅助运算function
    # def future_option_mapping(self,x):
    #     if str(x)[:2] == 'HO':
    #         return 'IH' + str(x)[2:]
    #     elif str(x)[:2] == 'IO':
    #         return 'IF' + str(x)[2:]
    #     elif str(x)[:2] == 'MO':
    #         return 'IM' + str(x)[2:]
    #     else:
    #         print(x,'没有找到匹配规则')
    #         return None
    #         # print('qnmd')
    #         # raise ValueError
    # def index_finding(self,x):
    #     if str(x)[:2]=='IH':
    #         return '上证50'
    #     elif str(x)[:2]=='IF':
    #         return '沪深300'
    #     elif str(x)[:2]=='IC':
    #         return '中证500'
    #     elif str(x)[:2]=='IM':
    #         return '中证1000'
    #     else:
    #         print(x,'没有找到匹配规则')
    #         return None
    # def option_direction(self,x):
    #     if '卖' in x:
    #         return -1
    #     else:
    #         return 1
    # def product_index_withdraw(self,product_name):
    #     inputpath_product = glv.get('product_detail')
    #     df_proindex = pd.read_excel(inputpath_product,sheet_name='product_detail')
    #     if product_name=='惠盈一号':
    #         product_name2='宣夜惠盈1号'
    #     elif product_name == '瑞锐精选':
    #         product_name2 = '瑞锐精选'
    #     elif product_name == '瑞锐500':
    #         product_name2 = '瑞锐500指增'
    #     elif product_name=='高益振英一号':
    #         product_name2='高毅振英1号'
    #     elif product_name=='念觉沪深300':
    #         product_name2='念空瑞景39号'
    #     elif product_name=='念觉知行4号':
    #         product_name2='念空知行4号'
    #     elif product_name=='仁睿价值精选1号':
    #         product_name2='仁睿价值精选1号'
    #     else:
    #         raise ValueError
    #     index_type = df_proindex[df_proindex['product_name'] == product_name2]['index_type'].tolist()[0]
    #     return index_type
    # def portfolio_index_exposure_withdraw(self,product_name):
    #     index=self.product_index_withdraw(product_name)
    #     if index == '上证50':
    #         df_index = self.df_sz50_exposure
    #     elif index == '沪深300':
    #         df_index = self.df_hs300_exposure
    #     elif index == '中证500':
    #         df_index = self.df_zz500_exposure
    #     elif index == '中证1000':
    #         df_index = self.df_zz1000_exposure
    #     else:
    #         df_index = self.df_zzA500_exposure
    #     df_index=df_index.astype(float)
    #     df_index=df_index.T
    #     df_index.reset_index(inplace=True)
    #     df_index.rename(columns={'index': 'factor_name'}, inplace=True)
    #     df_index.columns = ['factor_name', 'index_exposure']
    #     return df_index
    #
    # def option_analysis(self,position_df, realtime_data_df, realtime_data_future):
    #     realtime_data_df['代码'] = realtime_data_df['代码'].str.split('.').str[0]
    #     merged_df = pd.merge(position_df, realtime_data_df, left_on='合约', right_on='代码')
    #     merged_df['option_code'] = merged_df['合约'].apply(lambda x: str(x)[:6])
    #     merged_df['future'] = merged_df['option_code'].apply(lambda x: self.future_option_mapping(x))
    #     realtime_data_future2=realtime_data_future.copy()
    #     realtime_data_future2.drop(columns='前结算价',inplace=True)
    #     merged_df = merged_df.merge(realtime_data_future2, on='future', how='left')
    #     merged_df = merged_df[
    #         ['合约', '买卖', '总持仓', '前结算价', '现价', 'Delta', '中价隐含波动率', 'future', 'future_price',
    #          'ratio']]
    #     merged_df['market_value'] = merged_df['future_price'] * merged_df['Delta'] * merged_df['总持仓'] * merged_df[
    #         'ratio'] / (merged_df['ratio'] / 100)
    #     merged_df['direction'] = merged_df['买卖'].apply(lambda x: self.option_direction(x))
    #     merged_df['market_value'] = merged_df['market_value'] * merged_df['direction']
    #     merged_df['proportion'] = abs(merged_df['Delta'] * merged_df['总持仓']* merged_df['direction'] / (merged_df['ratio'] / 100))
    #     merged_df['daily_profit'] = (merged_df['现价'] - merged_df['前结算价']) * merged_df['总持仓'] * merged_df[
    #         'direction'] * 100
    #     merged_df = merged_df[['合约', '买卖', '总持仓', 'Delta', 'market_value', 'daily_profit','proportion']]
    #     return merged_df
    # def future_analysis(self,position_df, realtime_data_future):
    #     position_df2=position_df.copy()
    #     position_df2['new_code']=position_df['合约'].apply(lambda x: str(x)[:1])
    #     position_df3=position_df2[~(position_df2['new_code']=='T')]
    #     position_df3=position_df3[(position_df2['new_code']=='I')]
    #     position_df4=position_df2[(position_df2['new_code']=='T')]
    #     merged_df = pd.merge(position_df3, realtime_data_future, left_on='合约', right_on='future')
    #     merged_df['Delta'] = 1
    #     merged_df['direction'] = merged_df['买卖'].apply(lambda x: self.option_direction(x))
    #     merged_df['market_value'] = merged_df['future_price'] * merged_df['ratio'] * merged_df['direction'] * merged_df[
    #         '总持仓']
    #     merged_df['proportion']=1
    #     merged_df['daily_profit'] = (merged_df['future_price'] - merged_df['前结算价']) * merged_df['ratio'] * \
    #                                 merged_df['direction'] * merged_df['总持仓']
    #     merged_df = merged_df[['合约', '买卖', '总持仓', 'Delta', 'market_value', 'daily_profit','proportion']]
    #     position_df4.reset_index(inplace=True,drop=True)
    #     realtime_data_future2=realtime_data_future.copy()
    #     realtime_data_future2.rename(columns={'future':'合约'},inplace=True)
    #     merged_df2 = realtime_data_future2.merge(position_df4, on='合约')
    #     merged_df2.dropna(inplace=True)
    #     merged_df2['direction'] = merged_df2['买卖'].apply(lambda x: self.option_direction(x))
    #     merged_df2['daily_profit'] = (merged_df2['future_price'] - merged_df2['前结算价']) * merged_df2['ratio'] * \
    #                                 merged_df2['direction'] * merged_df2['总持仓']
    #     merged_df2=merged_df2[['合约', '买卖', '总持仓', 'daily_profit']]
    #     bond_profit=merged_df2['daily_profit'].sum()
    #     return merged_df,merged_df2,bond_profit
    # def cta_analysis(self,position_df,realtime_data_future):
    #     position_df2 = position_df.copy()
    #     position_df2['new_code'] = position_df['合约'].apply(lambda x: str(x)[:1])
    #     position_df3 = position_df2[~(position_df2['new_code'] == 'T')]
    #     position_df3 = position_df3[~(position_df2['new_code'] == 'I')]
    #     position_df3['合约']=position_df3['合约'].str.upper()
    #     merged_df = pd.merge(position_df3, realtime_data_future, left_on='合约', right_on='future')
    #     merged_df['Delta'] = 1
    #     merged_df['direction'] = merged_df['买卖'].apply(lambda x: self.option_direction(x))
    #     merged_df['market_value'] = merged_df['future_price'] * merged_df['ratio'] * merged_df['direction'] * merged_df[
    #         '总持仓']
    #     merged_df['proportion'] = 1
    #     merged_df['daily_profit'] = (merged_df['future_price'] - merged_df['前结算价']) * merged_df['ratio'] * \
    #                                 merged_df['direction'] * merged_df['总持仓']
    #     merged_df = merged_df[['合约', '买卖', '总持仓', 'Delta', 'market_value', 'daily_profit', 'proportion']]
    #     return merged_df
    # def FO_main(self,df_holding):
    #     df_info=pd.DataFrame()
    #     df1 = self.option_analysis(df_holding, self.realtime_data_df, self.realtime_data_future)
    #     df2,df_bond,bond_profit = self.future_analysis(df_holding, self.realtime_data_future)
    #     df_final = pd.concat([df1, df2])
    #     mkt_value_option = df1['market_value'].sum()
    #     profit_option = df1['daily_profit'].sum()
    #     mkt_value_future = df2['market_value'].sum()
    #     profit_future = df2['daily_profit'].sum()
    #     mkt_value_total = df_final['market_value'].sum()
    #     profit_total = df_final['daily_profit'].sum()
    #     profit_total=profit_total+bond_profit
    #     df_info['info_name']=['期权期货总市值','期权期货国债总盈亏','期权市值','期货市值','期权盈亏','期货盈亏','国债盈亏']
    #     df_info['money']=[mkt_value_total,profit_total,mkt_value_option,mkt_value_future,profit_option,profit_future,bond_profit]
    #     return df_info,df_final,df_bond
    # def portfolio_info_processing(self,stock_money,asset_value,etf_money,cb_money,df_info,stock_profit, etf_profit,cta_profit,cb_profit):
    #     df_porinfo=pd.DataFrame()
    #     mkt_value_total=df_info[df_info['info_name']=='期权期货总市值']['money'].tolist()[0]
    #     mkt_value_option=df_info[df_info['info_name']=='期权市值']['money'].tolist()[0]
    #     mkt_value_future = df_info[df_info['info_name'] == '期货市值']['money'].tolist()[0]
    #     leverage_ratio = round((stock_money + mkt_value_total+cb_money) / asset_value,4)
    #     ratio_stock = round(stock_money/ asset_value,4)
    #     ratio_option=round(mkt_value_option/asset_value,4)
    #     ratio_etf=round(etf_money/asset_value,4)
    #     ratio_future=round(mkt_value_future/asset_value,4)
    #     ratio_cb=round(cb_money/asset_value,4)
    #     df_porinfo['info_name']=['杠杆率','股票占比','期货占比','期权占比','ETF占比','可转债占比','股票市值','资产净值']
    #     df_porinfo['money']=[leverage_ratio,ratio_stock,ratio_future,ratio_option,ratio_etf,ratio_cb,stock_money,asset_value]
    #     df_profit=df_info[df_info['info_name'].isin(['期权盈亏','期货盈亏','国债盈亏'])]
    #     profit_list=df_profit['money'].tolist()
    #     profit_fo=profit_list[0]+profit_list[1]
    #     profit_fos=profit_fo+stock_profit
    #     profit_list_final=['股票盈亏','ETF_盈亏']+['期权盈亏','股指期货盈亏','国债盈亏']+['商品期货盈亏','可转债盈亏','总资产预估收益率(bp)']
    #     product_return=(cb_profit+profit_fos+etf_profit+profit_list[-1]+cta_profit)/asset_value
    #     profit_list_final2=[stock_profit,etf_profit]+profit_list+[cta_profit,cb_profit,round(product_return*10000,2)]
    #     df_info_add=pd.DataFrame()
    #     df_info_add['profit_name']=profit_list_final
    #     df_info_add['profit']=profit_list_final2
    #     df_proinfo2=pd.concat([df_porinfo,df_info_add],axis=1)
    #     # print(df_porinfo)
    #     return df_proinfo2
    #
    # def stock_exposure_calculate(self,df_portfolio,asset_value,stock_money):
    #     proportion=stock_money/asset_value
    #     selecting_code_list = df_portfolio['code'].tolist()
    #     df_factor_exposure = self.df_factor[self.df_factor['code'].isin(selecting_code_list)]
    #     df_factor_exposure.fillna(0, inplace=True)
    #     code_list=df_factor_exposure['code'].tolist()
    #     df_factor_exposure.drop(columns='code', inplace=True)
    #     df_portfolio=df_portfolio[df_portfolio['code'].isin(code_list)]
    #     weight = df_portfolio['weight'].astype(float).tolist()
    #     index_factor_exposure = list(
    #         np.array(np.dot(np.mat(df_factor_exposure.values).T, np.mat(weight).T)).flatten())
    #     index_factor_exposure = [index_factor_exposure]
    #     index_factor_exposure=np.multiply(np.array(index_factor_exposure),proportion)
    #     df_final = pd.DataFrame(index_factor_exposure, columns=df_factor_exposure.columns.tolist())
    #     df_final=df_final.T
    #     df_final.reset_index(inplace=True)
    #     df_final.rename(columns={'index':'factor_name'},inplace=True)
    #     df_final.columns=['factor_name','stock_exposure']
    #     return df_final
    # def index_exposure_sum(self,df):
    #     heyue_list = df['合约'].tolist()
    #     exposure_final = []
    #     for heyue in heyue_list:
    #         proportion = df[df['合约'] == heyue]['proportion'].tolist()[0]
    #         weight = df[df['合约'] == heyue]['weight'].tolist()[0]
    #         index = df[df['合约'] == heyue]['index_type'].tolist()[0]
    #         if index == '上证50':
    #             df_index = self.df_sz50_exposure
    #         elif index == '沪深300':
    #             df_index = self.df_hs300_exposure
    #         elif index == '中证500':
    #             df_index = self.df_zz500_exposure
    #         elif index == '中证1000':
    #             df_index = self.df_zz1000_exposure
    #         else:
    #             df_index = self.df_zzA500_exposure
    #         df_index.fillna(0, inplace=True)
    #         df_index = df_index.astype(float)
    #         slice_exposure = np.multiply(np.array(df_index.values), (proportion * weight))
    #         exposure_final.append(slice_exposure.tolist()[0])
    #     df_final = pd.DataFrame(exposure_final, columns=self.df_sz50_exposure.columns.tolist())
    #     df_final['合约'] = heyue_list
    #     df_final = df_final[['合约'] + self.df_sz50_exposure.columns.tolist()]
    #     return df_final
    # def option_future_exposure_calculate(self,df_detail,asset_value):
    #     df_detail['len'] = df_detail['合约'].apply(lambda x: len(x))
    #     df_future = df_detail[df_detail['len'] == 6]
    #     df_option = df_detail[~(df_detail['len'] == 6)]
    #     df_option['index'] = df_option['合约'].apply(lambda x: self.future_option_mapping(x))
    #     df_option['index_type'] = df_option['index'].apply(lambda x: self.index_finding(x))
    #     df_future['index_type'] = df_future['合约'].apply(lambda x: self.index_finding(x))
    #     df_option['weight'] = df_option['market_value'] / asset_value
    #     df_future['weight'] = df_future['market_value'] / asset_value
    #     option_exposure = self.index_exposure_sum(df_option)
    #     future_exposure = self.index_exposure_sum(df_future)
    #     option_future_exposure = pd.concat([option_exposure, future_exposure])
    #     option_exposure.set_index('合约', inplace=True, drop=True)
    #     future_exposure.set_index('合约', inplace=True, drop=True)
    #     option_exposure = option_exposure.apply(lambda x: x.sum(), axis=0)
    #     future_exposure = future_exposure.apply(lambda x: x.sum(), axis=0)
    #     dict_option_exposure = {'factor_name': option_exposure.index, 'option_exposure': option_exposure.values}
    #     dict_future_exposure = {'factor_name': future_exposure.index, 'future_exposure': future_exposure.values}
    #     option_final = pd.DataFrame(dict_option_exposure)
    #     future_final= pd.DataFrame(dict_future_exposure)
    #     exposure=option_final.merge(future_final,on='factor_name',how='left')
    #     return option_future_exposure, exposure
    # def final_portfolio_exposure_processing(self,df_stock_factor,future_option_exposure,index_exposure):
    #     df_portfolio_exposure = df_stock_factor.merge(future_option_exposure, on='factor_name', how='left')
    #     df_portfolio_exposure.set_index('factor_name', inplace=True)
    #     df_portfolio_exposure['portfolio_exposure'] = df_portfolio_exposure.apply(lambda x: x.sum(), axis=1)
    #     df_portfolio_exposure.reset_index(inplace=True)
    #     df_portfolio_exposure=df_portfolio_exposure.merge(index_exposure,on='factor_name', how='left')
    #     df_portfolio_exposure['difference']=df_portfolio_exposure['portfolio_exposure']-df_portfolio_exposure['index_exposure']
    #     df_portfolio_exposure['ratio']=df_portfolio_exposure['difference']/abs(df_portfolio_exposure['index_exposure'])
    #     return df_portfolio_exposure
    # def trading_detail_split(self,df_holding2,df_holding3,df_holding_stock2,etf_code_list2):
    #     df_holding=df_holding2.copy()
    #     df_holding_yes=df_holding3.copy()
    #     # print(df_holding_yes)
    #     df_holding_yes=df_holding_yes[['合约','总持仓','买卖']]
    #     df_holding_yes.columns=['合约','昨日持仓','买卖']
    #     df_holding4 = df_holding.copy()
    #     df_holding4=df_holding4[['合约','总持仓','买卖']]
    #     df_holding4.columns=['合约','总持仓','买卖']
    #     df_holding_stock=df_holding_stock2.copy()
    #     df_stock=df_holding_stock[~(df_holding_stock['code'].isin(etf_code_list2))]
    #     df_etf = df_holding_stock[(df_holding_stock['code'].isin(etf_code_list2))]
    #     df_holding['difference']=df_holding['总持仓']-df_holding['昨仓']
    #     df_stock['difference']=df_stock['HoldingQty']-df_stock['HoldingQty_yes']
    #     df_etf['difference'] = df_etf['HoldingQty'] - df_etf['HoldingQty_yes']
    #     def action_decision(x):
    #         if x>0:
    #             return '开仓'
    #         elif x==0:
    #             return '不变'
    #         else:
    #             return '平仓'
    #     df_holding4=df_holding4.merge(df_holding_yes,on=['合约','买卖'],how='outer')
    #     df_holding4.fillna(0,inplace=True)
    #     df_holding4['difference']=df_holding4['总持仓']-df_holding4['昨日持仓']
    #     df_holding4['action']=df_holding4['difference'].apply(lambda x: action_decision(x))
    #     df_holding['action']=df_holding['difference'].apply(lambda x: action_decision(x))
    #     df_stock['action'] = df_stock['difference'].apply(lambda x: action_decision(x))
    #     df_etf['action'] = df_etf['difference'].apply(lambda x: action_decision(x))
    #     df_holding=df_holding[~(df_holding['action']=='不变')]
    #     df_stock = df_stock[~(df_stock['action'] == '不变')]
    #     df_etf =  df_etf[~(df_etf['action'] == '不变')]
    #     df_holding4=df_holding4[~(df_holding4['action'] == '不变')]
    #     df_holding.sort_values(by='difference',ascending=False,inplace=True)
    #     df_stock.sort_values(by='difference', ascending=False,inplace=True)
    #     df_etf.sort_values(by='difference', ascending=False,inplace=True)
    #     df_holding4.sort_values(by='difference', ascending=False, inplace=True)
    #     return df_holding,df_stock,df_etf,df_holding4
    #
    #
    #
    # def portfolio_tracking_main(self,product_name):
    #     prodata=product_data(product_name, self.realtime_data_index)
    #     df_holding,df_holding_yes=prodata.position_withdraw(self.simulation)
    #     df_cta=self.cta_analysis(df_holding,self.realtime_data_future)
    #     cta_profit=df_cta['daily_profit'].sum()
    #     stock_money_today,asset_value_today=prodata.stock_info_dicesion()
    #     if product_name=='念觉沪深300':
    #         hs300_return=self.realtime_data_index['沪深300'].tolist()[0]
    #         asset_value_today=(1+hs300_return)*asset_value_today
    #         stock_money_today=(1+hs300_return)*stock_money_today
    #     if product_name=='念觉知行4号':
    #         hs300_return=self.realtime_data_index['中证A500'].tolist()[0]
    #         asset_value_today=(1+hs300_return)*asset_value_today
    #         stock_money_today=(1+hs300_return)*stock_money_today
    #     if product_name=='仁睿价值精选1号':
    #         hs300_return=self.realtime_data_index['中证500'].tolist()[0]
    #         asset_value_today=(1+hs300_return)*asset_value_today
    #         stock_money_today=(1+hs300_return)*stock_money_today
    #     df_portfolio=prodata.portfolio_weight_withdraw()
    #     etf_code_list=prodata.etf_pool_withdraw()
    #     stock_money, etf_money_today,cb_money_today, stock_profit, etf_profit,cb_profit,df_stockholding=prodata.stockinfo_analyse(self.realtime_data_etf,self.realtime_data_stock)
    #     index_exposure=self.portfolio_index_exposure_withdraw(product_name)
    #     df_info, df_final,df_bond = self.FO_main(df_holding)
    #     df_porinfo = self.portfolio_info_processing(stock_money_today, asset_value_today, etf_money_today,cb_money_today,df_info,stock_profit, etf_profit,cta_profit,cb_profit)
    #     df_stock_factor=self.stock_exposure_calculate(df_portfolio,asset_value_today,stock_money_today)
    #     option_future_exposure, exposure_final=self.option_future_exposure_calculate(df_final, asset_value_today)
    #     df_portfolio_exposure=self.final_portfolio_exposure_processing(df_stock_factor, exposure_final, index_exposure)
    #     df_fo_difference, df_stock_difference, df_etf_difference,df_fo_difference2=self.trading_detail_split(df_holding,df_holding_yes,df_stockholding,etf_code_list)
    #     df_final=pd.concat([df_final,df_bond])
    #     return df_info,df_final,df_porinfo,option_future_exposure,df_portfolio_exposure,df_fo_difference,df_fo_difference2, df_stock_difference, df_etf_difference,df_cta
    # def saving_main(self,product_name):
    #     df_info, df_final, df_porinfo, option_future_exposure, df_portfolio_exposure,df_fo_difference,df_fo_difference2, df_stock_difference, df_etf_difference,df_cta=self.portfolio_tracking_main(product_name)
    #     outputpath = glv.get('realtime_output')
    #     if self.simulation==True:
    #         outputpath=os.path.join(outputpath,'simulation')
    #     gt.folder_creator2(outputpath)
    #     #存储sql
    #     sp=sql_processing(product_name,self.simulation,df_porinfo,df_final,df_cta,df_fo_difference,df_fo_difference2,df_etf_difference,df_stock_difference)
    #     sp.productTracking_sqlmain()
    #     outputpath = os.path.join(outputpath, str(product_name)+'_product_tracking.xlsx')
    #     with pd.ExcelWriter(outputpath, engine='openpyxl') as writer:
    #         df_porinfo.to_excel(writer, sheet_name='产品杠杆率', index=False)
    #         df_fo_difference.to_excel(writer, sheet_name='期权期货当日变动', index=False)
    #         df_fo_difference2.to_excel(writer, sheet_name='期权期货隔夜变动', index=False)
    #         df_etf_difference.to_excel(writer, sheet_name='etf变动', index=False)
    #         df_stock_difference.to_excel(writer, sheet_name='股票变动', index=False)
    #         df_final.to_excel(writer, sheet_name='期货期权数据', index=False)
    #         df_cta.to_excel(writer,sheet_name='商品期货数据',index=False)
    #         df_info.to_excel(writer, sheet_name='期权期货信息', index=False)
    #         df_portfolio_exposure.to_excel(writer,sheet_name='产品风险因子暴露', index=False)
    #         option_future_exposure.to_excel(writer,sheet_name='期权期货因子暴露',index=False)





