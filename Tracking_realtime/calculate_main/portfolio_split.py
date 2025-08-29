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

class portfolio_split_calculate:
    def __init__(self,df_score,df_indexcomponent,df_mkt,df_index,df_weight,df_portinfo):
        self.df_score=df_score
        self.df_indexcomponent=df_indexcomponent
        self.df_weight=df_weight
        self.df_portfinfo=df_portinfo
        self.df_mkt=df_mkt
        self.df_index=df_index
        working_days_list=self.df_mkt['valuation_date'].unique().tolist()
        self.df_weight=self.df_weight[self.df_weight['valuation_date'].isin(working_days_list)]
    def portfolio_performance_calculate(self,df):
        df.fillna(0, inplace=True)
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
    def portfolio_split(self,portfolio_name):
        df_weight=self.df_weight[self.df_weight['portfolio_name']==portfolio_name]
        base_score=self.df_portfinfo[self.df_portfinfo['score_name']==portfolio_name]['base_score'].tolist()[0]
        index_type = self.df_portfinfo[self.df_portfinfo['score_name'] == portfolio_name]['index_type'].tolist()[0]
        index_code=gt.index_mapping(index_type,'code')
        df_index=self.df_index[self.df_index['code']==index_code]
        df_index=df_index[['valuation_date','pct_chg']]
        df_index.columns=['valuation_date','index_return']
        df_score=self.df_score[self.df_score['score_name']==base_score]
        df_indexcomponent=self.df_indexcomponent[self.df_indexcomponent['index_type']==index_type]
        df_indexcomponent.rename(columns={'weight':'component_weight'},inplace=True)
        df_weight=df_weight.merge(df_indexcomponent,on=['valuation_date','code'],how='outer')
        df_weight=df_weight.merge(df_score,on=['valuation_date','code'],how='left')
        df_weight=df_weight.merge(self.df_mkt,on=['valuation_date','code'],how='left')
        df_weight=df_weight.merge(df_index,on='valuation_date',how='left')
        df_missing=df_weight[~(df_weight['component_weight'].isna())]
        df_missing = df_missing[(df_missing['final_score'].isna())]
        df_missing.fillna(0, inplace=True)
        df_info_missing=self.portfolio_performance_calculate(df_missing)
        df_info_missing['split_name']='component_missing'
        df_top=df_weight[df_weight['component_weight'].isna()]
        df_top.fillna(0, inplace=True)
        df_info_top = self.portfolio_performance_calculate(df_top)
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
            slice_info= self.portfolio_performance_calculate(slice_df_weight)
            slice_info['split_name']='component_' + str(round(1 - j, 2)) + '_' + str(round(1 - k, 2))
            df_info_comp=pd.concat([df_info_comp,slice_info])
        df_output=pd.concat([df_info_comp,df_info_top,df_info_missing])
        df_output['portfolio_name']=portfolio_name
        return df_output
    def portfolio_split_main(self):
        df_output2=pd.DataFrame()
        portfolio_name_list=self.df_portfinfo['score_name'].tolist()
        for portfolio_name in portfolio_name_list:
            df_output=self.portfolio_split(portfolio_name)
            df_output2=pd.concat([df_output2,df_output])
        return df_output2


