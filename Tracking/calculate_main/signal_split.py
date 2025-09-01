import pandas as pd
import os
import sys
# 添加全局工具函数路径到系统路径
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import datetime
import global_tools as gt
import global_setting.global_dic as glv
import numpy as np
class score_split_calculate:
    def __init__(self,df_score,df_indexcomponent,df_mkt,df_index,df_portinfo):
        self.df_score=df_score
        self.df_indexcomponent=df_indexcomponent
        self.df_mkt=df_mkt
        self.df_index=df_index
        self.df_portfinfo = df_portinfo
    def score_performance_calculate(self,df):
        df.fillna(0, inplace=True)
        if len(df)>0:
           df['weight']=1/len(df)
        else:
            df['weight']=None
        df['weight2']=df['weight']/df['weight'].sum()
        df['excess_return'] = df['pct_chg'] -df['index_return']
        df['weight_difference'] = df['weight'].astype(float) - df['component_weight'].astype(float)
        df['contribution'] = df['excess_return'] * df['weight_difference']
        df['portfolio_excess_return'] = df['excess_return'] * df['weight2']
        # 按valuation_date分组计算contribution和weight的总和
        df_grouped = df.groupby('valuation_date').agg({
            'contribution': 'sum',
            'weight': 'sum',
            'weight_difference':'sum',
            'portfolio_excess_return':'sum'
        }).reset_index()
        return df_grouped
    def score_split(self,index_type,base_score):
        index_code=gt.index_mapping(index_type,'code')
        df_index=self.df_index[self.df_index['code']==index_code]
        df_index=df_index[['valuation_date','pct_chg']]
        df_index.columns=['valuation_date','index_return']
        df_score=self.df_score[self.df_score['score_name']==base_score]
        df_indexcomponent=self.df_indexcomponent[self.df_indexcomponent['index_type']==index_type]
        df_indexcomponent.rename(columns={'weight':'component_weight'},inplace=True)
        df_weight=df_score.copy()
        df_weight=df_weight.merge(df_indexcomponent,on=['valuation_date','code'],how='outer')
        df_weight=df_weight.merge(self.df_mkt,on=['valuation_date','code'],how='left')
        df_weight=df_weight.merge(df_index,on='valuation_date',how='left')
        df_missing=df_weight[~(df_weight['component_weight'].isna())]
        df_missing = df_missing[(df_missing['final_score'].isna())]
        df_missing.fillna(0, inplace=True)
        df_info_missing=self.score_performance_calculate(df_missing)
        df_info_missing['split_name']='component_missing'
        df_top=df_weight[df_weight['component_weight'].isna()]
        df_top.sort_values(by='final_score', ascending=False, inplace=True)
        df_top.reset_index(inplace=True, drop=True)
        df_top = df_top.loc[:199]
        df_top.fillna(0, inplace=True)
        df_info_top = self.score_performance_calculate(df_top)
        df_info_top['split_name'] = 'top'
        df_weight2 = df_weight[~(df_weight['component_weight'].isna())]
        df_weight2 = df_weight2[~(df_weight2['final_score'].isna())]
        df_info_comp=pd.DataFrame()
        for i in range(0, 10):
            j = i / 10
            k = 0.1 + 0.1 * i
            quantile_lower = df_weight2['final_score'].quantile(1 - k)
            quantile_upper = df_weight2['final_score'].quantile(1 - j)
            slice_df_weight = df_weight2[
                (df_weight2['final_score'] < quantile_upper) & (df_weight2['final_score'] > quantile_lower)]
            slice_info= self.score_performance_calculate(slice_df_weight)
            slice_info['split_name']='component_' + str(round(1 - j, 2)) + '_' + str(round(1 - k, 2))
            df_info_comp=pd.concat([df_info_comp,slice_info])
        df_output=pd.concat([df_info_comp,df_info_top,df_info_missing])
        df_output['score_name']=base_score
        df_output['index_type']=index_type
        return df_output
    def score_split_main(self):
        df_output2=pd.DataFrame()
        portfolio_name_list=self.df_portfinfo['base_score'].unique().tolist()
        for score_name in portfolio_name_list:
            for index_type in ['上证50','沪深300','中证500','中证1000','中证A500']:
                df_output=self.score_split(index_type,score_name)
                df_output2=pd.concat([df_output2,df_output])
        return df_output2