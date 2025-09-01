import os
import pandas as pd
import Signal_tracking.global_setting.global_dic as glv
import global_tools_func.global_tools as gt


class score_analyse:
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
    def portfolio_performance_calculate(self,df,index_return):
        df = df.merge(self.df_stock_return, on='code', how='left')
        df.fillna(0, inplace=True)
        df['excess_return'] = df['return'] - float(index_return)
        if len(df)==0:
            portfolio_excess=0
        else:
            df['weight']=1/len(df)
            df['portfolio_excess'] = df['excess_return'] * df['weight']
            portfolio_excess = df['portfolio_excess'].sum()
        portfolio_excess=round(portfolio_excess*10000,4)
        return portfolio_excess
    def portfolio_analyse(self,index_type):
        df_weight=self.df_score.copy()
        index_return=self.df_index_return[index_type].tolist()[0]
        df_index=self.index_decision(index_type)
        df_index.rename(columns={'weight': 'component_weight'}, inplace=True)
        df_weight = df_weight.merge(df_index, on='code', how='outer')
        df_weight2=df_weight.copy()
        df_missing = df_weight2[~(df_weight2['component_weight'].isna())]
        df_missing=df_missing[(df_missing['final_score'].isna())]
        df_missing = df_missing[['code', 'component_weight']]
        df_weight = df_weight[~(df_weight['final_score'].isna())]
        df_weight.sort_values(by='final_score', ascending=False, inplace=True)
        df_weight.reset_index(inplace=True, drop=True)
        df_top=df_weight[df_weight['component_weight'].isna()]
        df_top.sort_values(by='final_score', ascending=False, inplace=True)
        df_top.reset_index(inplace=True, drop=True)
        df_top=df_top.loc[:199]
        df_weight = df_weight[~(df_weight['component_weight'].isna())]
        excess_missing= self.portfolio_performance_calculate(df_missing,index_return)
        excess_top = self.portfolio_performance_calculate(df_top,index_return)
        contribution_list = [excess_missing,excess_top]
        analyse_list = ['missing', 'top']
        for i in range(0, 10):
            j = i / 10
            k = 0.1 + 0.1 * i
            quantile_lower = df_weight['final_score'].quantile(1 - k)
            quantile_upper = df_weight['final_score'].quantile(1 - j)
            slice_df_weight = df_weight[
                (df_weight['final_score'] < quantile_upper) & (df_weight['final_score'] > quantile_lower)]
            slice_df_weight = slice_df_weight[['code','component_weight']]
            excess_com = self.portfolio_performance_calculate(slice_df_weight,index_return)
            contribution_list.append(excess_com)
            analyse_list.append('component_' + str(round(1 - j, 2)) + '_' + str(round(1 - k, 2)))
        df_final = pd.DataFrame()
        df_final['analyse_name'] = analyse_list
        df_final['excess_return'] = contribution_list
        df_final.set_index('analyse_name', inplace=True, drop=True)
        df_final = df_final.T
        df_final.reset_index(inplace=True, drop=True)
        return df_final
