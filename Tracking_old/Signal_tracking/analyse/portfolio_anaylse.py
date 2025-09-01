import os
import pandas as pd
import Signal_tracking.global_setting.global_dic as glv
import global_tools_func.global_tools as gt
class portfolio_analyse:
    def __init__(self,df_score,df_stock_return,df_index_return,df_hs300,df_zz500,df_zzA500,df_zz1000):
        self.df_stock_return=df_stock_return
        self.df_index_return=df_index_return
        self.df_score=df_score
        self.df_hs300=df_hs300
        self.df_zz500=df_zz500
        self.df_zzA500=df_zzA500
        self.df_zz1000=df_zz1000
    def index_decision(self,index_type):
        if index_type=='沪深300':
            return self.df_hs300
        elif index_type=='中证500':
            return self.df_zz500
        elif index_type=='中证A500':
            return self.df_zzA500
        elif index_type=='中证1000':
            return self.df_zz1000
        else:
            raise ValueError
    def score_withdraw(self,score_type,target_date):
        available_date = gt.last_workday_calculate(target_date)
        available_date = gt.intdate_transfer(available_date)
        inputpath = glv.get('score')
        inputpath = os.path.join(inputpath,score_type)
        inputpath = gt.file_withdraw(inputpath, available_date)
        df = gt.readcsv(inputpath)
        return df
    def portfolo_info_withdraw(self,portfolio_name):
        inputpath=glv.get('valid_score')
        df_config=pd.read_excel(inputpath)
        index_type=df_config[df_config['score_name']==portfolio_name]['index_type'].tolist()[0]
        base_score = df_config[df_config['score_name'] == portfolio_name]['base_score'].tolist()[0]
        return index_type,base_score
    def portfolio_performance_calculate(self,df, index_return):
        df = df.merge(self.df_stock_return, on='code', how='left')
        df.fillna(0, inplace=True)
        df['excess_return'] = df['return'] - float(index_return)
        df['weight_difference'] = df['weight'].astype(float) - df['component_weight'].astype(float)
        df['contribution'] = df['excess_return'] * df['weight_difference']
        contribution = df['contribution'].sum()
        weight = df['weight'].sum()
        contribution=round(contribution*10000,4)
        return contribution, weight
    def portfolio_analyse(self,portfolio_name,target_date):
        index_type, base_score=self.portfolo_info_withdraw(portfolio_name)
        df_base_score=self.score_withdraw(base_score,target_date)
        df_weight=self.df_score.copy()
        df_weight=df_weight.merge(df_base_score,on='code',how='left')
        index_return=self.df_index_return[index_type].tolist()[0]
        df_index=self.index_decision(index_type)
        df_index.rename(columns={'weight': 'component_weight'}, inplace=True)
        df_weight = df_weight.merge(df_index, on='code', how='outer')
        df_weight2 = df_weight.copy()
        df_missing = df_weight2[~(df_weight2['component_weight'].isna())]
        df_missing = df_missing[(df_missing['final_score'].isna())]
        df_missing = df_missing[['code', 'component_weight','weight']]
        df_missing.fillna(0,inplace=True)
        df_weight = df_weight[~(df_weight['final_score'].isna())]
        df_weight.sort_values(by='final_score', ascending=False, inplace=True)
        df_weight.reset_index(inplace=True, drop=True)
        df_top=df_weight[df_weight['component_weight'].isna()]
        df_top.sort_values(by='final_score', ascending=False, inplace=True)
        df_top.reset_index(inplace=True, drop=True)
        df_top=df_top.loc[:199]
        df_weight = df_weight[~(df_weight['component_weight'].isna())]
        excess_missing,weight_missing= self.portfolio_performance_calculate(df_missing,index_return)
        excess_top,weight_top = self.portfolio_performance_calculate(df_top,index_return)
        contribution_list = [excess_missing,excess_top]
        weight_list=[weight_missing,weight_top]
        analyse_list = ['missing', 'top']
        for i in range(0, 10):
            j = i / 10
            k = 0.1 + 0.1 * i
            quantile_lower = df_weight['final_score'].quantile(1 - k)
            quantile_upper = df_weight['final_score'].quantile(1 - j)
            slice_df_weight = df_weight[
                (df_weight['final_score'] < quantile_upper) & (df_weight['final_score'] > quantile_lower)]
            slice_df_weight = slice_df_weight[['code','weight','component_weight']]
            excess_com,weight_com = self.portfolio_performance_calculate(slice_df_weight,index_return)
            contribution_list.append(excess_com)
            analyse_list.append('component_' + str(round(1 - j, 2)) + '_' + str(round(1 - k, 2)))
            weight_list.append(weight_com)
        df_final = pd.DataFrame()
        df_final['analyse_name'] = analyse_list
        df_final['excess_return'] = contribution_list
        df_final.set_index('analyse_name', inplace=True, drop=True)
        df_final = df_final.T
        df_final.reset_index(inplace=True, drop=True)
        df_final2 = pd.DataFrame()
        df_final2['analyse_name'] = analyse_list
        df_final2['weight'] = weight_list
        df_final2.set_index('analyse_name', inplace=True, drop=True)
        df_final2=df_final2.T
        df_final2.reset_index(inplace=True, drop=True)
        return df_final,df_final2

