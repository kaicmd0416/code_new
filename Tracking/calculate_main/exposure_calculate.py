"""
产品计算模块
功能：处理单个产品的持仓数据计算，包括期货期权、股票ETF可转债等各类资产的分析
主要类：
- product_tracking: 产品跟踪计算类，负责单个产品的完整计算流程
作者：[作者名]
创建时间：[创建时间]
"""

import pandas as pd
import os
import sys

# 添加全局工具函数路径到系统路径
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
pd.set_option('display.max_columns', None)
import datetime
import global_tools as gt
import global_setting.global_dic as glv
import numpy as np
from data.data_prepared import futureoption_position, security_position, prod_info, get_product_detail


def sql_path():
    """
    获取SQL配置文件路径
    功能：构建SQL配置文件的完整路径

    Returns:
        str: SQL配置文件的完整路径
    """
    yaml_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'project_config', 'trackingrealtime_sql.yaml')
    return yaml_path


# 全局变量定义
global inputpath_sql
inputpath_sql = sql_path()
class exposure_tracking:
    """
    产品跟踪计算类
    功能：处理单个产品的完整计算流程，包括数据获取、处理、分析和结果输出
    """

    def __init__(self,df_detail,df_info,df_stockexposure,df_indexexposure,df_factorreturn,index_type):
        """
        初始化方法
        功能：初始化产品跟踪对象，获取所有必要的持仓数据和产品信息

        参数：
            product_code (str): 产品代码

        """
        self.index_type=index_type
        self.working_days_list=df_info['valuation_date'].unique().tolist()
        self.df_detail=df_detail
        self.df_info=df_info
        self.df_stockexposure=df_stockexposure
        self.df_indexexposure=df_indexexposure
        self.df_factorreturn=df_factorreturn
        # 设置当前日期和时间
        today = datetime.date.today()
        self.date = gt.strdate_transfer(today)
        self.now = datetime.datetime.now().replace(tzinfo=None)

    def stock_exposure_calculate(self):
        df_output=pd.DataFrame()
        df_portfolio_ori = self.df_detail[self.df_detail['portfolio_name'] == 'stock']
        df_portfolio_ori = df_portfolio_ori[['valuation_date', 'code', 'weight']]
        for date in self.working_days_list:
            proportion=self.df_info[self.df_info['valuation_date']==date]['proportion_stock'].tolist()
            df_portfolio=df_portfolio_ori[df_portfolio_ori['valuation_date']==date]
            selecting_code_list = df_portfolio['code'].tolist()
            df_factor_exposure=self.df_stockexposure[self.df_stockexposure['valuation_date']==date]
            df_factor_exposure = df_factor_exposure[df_factor_exposure['code'].isin(selecting_code_list)]
            df_factor_exposure.fillna(0, inplace=True)
            code_list = df_factor_exposure['code'].tolist()
            df_factor_exposure.drop(columns=['code','valuation_date'], inplace=True)
            df_portfolio = df_portfolio[df_portfolio['code'].isin(code_list)]
            weight = df_portfolio['weight'].astype(float).tolist()
            index_factor_exposure = list(
                np.array(np.dot(np.mat(df_factor_exposure.values).T, np.mat(weight).T)).flatten())
            index_factor_exposure = [index_factor_exposure]
            index_factor_exposure = np.multiply(np.array(index_factor_exposure), proportion)
            df_final = pd.DataFrame(index_factor_exposure, columns=df_factor_exposure.columns.tolist())
            df_final = df_final.T
            df_final.reset_index(inplace=True)
            df_final.rename(columns={'index': 'factor_name'}, inplace=True)
            df_final.columns = ['factor_name', 'stock_exposure']
            df_final['valuation_date']=date
            df_output=pd.concat([df_output,df_final])
        return df_output
    def index_finding(self,x):
        if str(x)[:2]=='IH' or str(x)[:2] == 'HO':
            return '上证50'
        elif str(x)[:2]=='IF' or str(x)[:2] == 'IO':
            return '沪深300'
        elif str(x)[:2]=='IC':
            return '中证500'
        elif str(x)[:2]=='IM' or str(x)[:2] == 'MO':
            return '中证1000'
        else:
            return None
    def index_exposure_sum(self,df,df_exposure):
        heyue_list = df['code'].tolist()
        exposure_final = []
        for heyue in heyue_list:
            proportion = df[df['code'] == heyue]['proportion'].tolist()[0]
            weight = df[df['code'] == heyue]['weight'].tolist()[0]
            index = df[df['code'] == heyue]['index_type'].tolist()[0]
            df_index=df_exposure[df_exposure['index_type']==index]
            df_index.drop(columns=['valuation_date','index_type'],inplace=True)
            df_index.fillna(0, inplace=True)
            df_index = df_index.astype(float)
            slice_exposure = np.multiply(np.array(df_index.values), (proportion * weight))
            exposure_final.append(slice_exposure.tolist()[0])
        df_exposure2=df_exposure.drop(columns=['valuation_date','index_type'])
        df_final = pd.DataFrame(exposure_final, columns=df_exposure2.columns.tolist())
        df_final['code'] = heyue_list
        df_final = df_final[['code'] + df_exposure2.columns.tolist()]
        return df_final
    def future_exposure_calculate(self):
        df_output = pd.DataFrame()
        df_future_ori = self.df_detail[self.df_detail['portfolio_name'] == 'indexfuture']
        df_future_ori = df_future_ori[['valuation_date', 'code', 'weight','proportion']]
        df_option_ori = self.df_detail[self.df_detail['portfolio_name'] == 'option']
        df_option_ori = df_option_ori [['valuation_date', 'code', 'weight','proportion']]
        for date in self.working_days_list:
            df_exposure=self.df_indexexposure[self.df_indexexposure['valuation_date']==date]
            proportion_future = self.df_info[self.df_info['valuation_date'] == date]['proportion_indexfuture'].tolist()
            proportion_option = self.df_info[self.df_info['valuation_date'] == date]['proportion_option'].tolist()
            df_future = df_future_ori[df_future_ori['valuation_date'] == date]
            df_option = df_option_ori[df_option_ori['valuation_date'] == date]
            df_future['index_type']=df_future['code'].apply(lambda x: self.index_finding(x))
            df_option['index_type'] = df_option['code'].apply(lambda x: self.index_finding(x))
            option_exposure = self.index_exposure_sum(df_option,df_exposure)
            future_exposure = self.index_exposure_sum(df_future,df_exposure)
            option_exposure.set_index('code', inplace=True, drop=True)
            future_exposure.set_index('code', inplace=True, drop=True)
            option_exposure = option_exposure.apply(lambda x: x.sum(), axis=0)
            future_exposure = future_exposure.apply(lambda x: x.sum(), axis=0)
            dict_option_exposure = {'factor_name': option_exposure.index, 'option_exposure': option_exposure.values}
            dict_future_exposure = {'factor_name': future_exposure.index, 'future_exposure': future_exposure.values}
            option_final = pd.DataFrame(dict_option_exposure)
            future_final= pd.DataFrame(dict_future_exposure)
            option_final['option_exposure']=option_final['option_exposure']*proportion_option
            future_final['future_exposure'] = future_final['future_exposure'] * proportion_future
            exposure=option_final.merge(future_final,on='factor_name',how='left')
            exposure['valuation_date']=date
            df_output=pd.concat([df_output,exposure])
        return df_output

    def final_portfolio_exposure_processing(self):
        df_factorreturn=self.df_factorreturn.copy()
        
        # 将df_factorreturn从宽格式转换为长格式
        # 保留valuation_date列，将其他列作为factor_name
        id_vars = ['valuation_date']
        value_vars = [col for col in df_factorreturn.columns if col not in id_vars]
        
        df_factorreturn_long = df_factorreturn.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name='factor_name',
            value_name='factor_return'
        )
        
        # 重新排列列顺序
        df_factorreturn_long = df_factorreturn_long[['factor_name', 'factor_return', 'valuation_date']]
        df_stock_factor=self.stock_exposure_calculate()
        future_option_exposure=self.future_exposure_calculate()
        if self.index_type!=None:
             index_exposure=self.df_indexexposure[self.df_indexexposure['index_type']==self.index_type]
             # 将index_exposure从宽格式转换为长格式
             # 保留valuation_date和index_type列，将其他列作为factor_name
             id_vars = ['valuation_date', 'index_type']
             value_vars = [col for col in index_exposure.columns if col not in id_vars]

             index_exposure_long = index_exposure.melt(
                 id_vars=id_vars,
                 value_vars=value_vars,
                 var_name='factor_name',
                 value_name='index_exposure'
             )

             # 重新排列列顺序
             index_exposure_long = index_exposure_long[['factor_name', 'index_exposure', 'valuation_date']]
        df_portfolio_exposure = df_stock_factor.merge(future_option_exposure, on=['factor_name','valuation_date'], how='left')
        df_portfolio_exposure.set_index(['factor_name','valuation_date'], inplace=True)
        df_portfolio_exposure['portfolio_exposure'] = df_portfolio_exposure.apply(lambda x: x.sum(), axis=1)
        df_portfolio_exposure.reset_index(inplace=True)
        if self.index_type!=None:
            df_portfolio_exposure = df_portfolio_exposure.merge(index_exposure_long, on=['factor_name','valuation_date'], how='left')
        else:
            df_portfolio_exposure['index_exposure']=0
        df_portfolio_exposure['difference'] = df_portfolio_exposure['portfolio_exposure'] - df_portfolio_exposure[
            'index_exposure']
        df_portfolio_exposure['ratio'] = df_portfolio_exposure['difference'] / abs(
            df_portfolio_exposure['index_exposure'])
        df_portfolio_exposure=df_portfolio_exposure.merge(df_factorreturn_long, on=['factor_name','valuation_date'], how='left')
        df_portfolio_exposure['product_factor_return']=df_portfolio_exposure['difference']*df_portfolio_exposure['factor_return']
        # 将inf值替换为None
        df_portfolio_exposure = df_portfolio_exposure.replace([np.inf, -np.inf], None)
        
        return df_portfolio_exposure
