"""
投资组合计算模块
功能：处理投资组合级别的计算，包括纸面投资组合和产品投资组合的分析
主要类：
- portfolio_tracking: 投资组合跟踪计算类，负责投资组合级别的完整计算流程
作者：[作者名]
创建时间：[创建时间]
"""
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
from data.data_prepared import weight_withdraw,scoredata_withdraw,product_info_withdraw,mktdata_withdraw
from calculate_main.portfolio_split import portfolio_split_calculate
from calculate_main.signal_split import score_split_calculate
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

class portfolio_tracking:
    """
    投资组合跟踪计算类
    功能：处理投资组合级别的完整计算流程，包括纸面投资组合和产品投资组合的分析
    """
    
    def __init__(self,start_date,end_date,realtime,split):
        """
        初始化方法
        功能：初始化投资组合跟踪对象，设置日期和权重获取器
        """
        self.start_date=start_date
        self.end_date=end_date
        self.split=split
        if realtime==True:
             today = datetime.date.today()
             date = gt.strdate_transfer(today)  # 当前日期字符串
             self.start_date=date
             self.end_date=date
        self.realtime=realtime
        self.ww = weight_withdraw(self.start_date,self.end_date,realtime)  # 权重数据获取器
        if split==True:
            sw=scoredata_withdraw(self.start_date,self.end_date,realtime)
            self.df_score=sw.score_withdraw()
            mw=mktdata_withdraw(self.start_date,self.end_date,realtime)
            self.df_indexcomp=mw.indexweight_withdraw()
            self.df_stockreturn=mw.stockdata_withdraw()
            self.df_portinfo=sw.portfolio_info_withdraw()
            self.df_indexreturn=mw.indexdata_withdraw()
        else:
            self.df_score=pd.DataFrame()
            self.df_indexcomp=pd.DataFrame()
            self.df_portinfo=pd.DataFrame()
            self.df_stockreturn=pd.DataFrame()
            self.df_indexreturn = pd.DataFrame()
        self.now = datetime.datetime.now().replace(tzinfo=None)  # 当前时间

    
    def paperportfolio_withdraw(self):
        """
        获取纸面投资组合数据
        功能：获取所有纸面投资组合的权重数据
        
        返回：
            DataFrame: 包含所有纸面投资组合权重数据的DataFrame
        """
        df = self.ww.portfolio_withdraw(None)
        inputpath_info=glv.get('portfolio_info')
        config_path = glv.get('path_config')
        df_info=gt.data_getting(inputpath_info,config_path)
        df_info=df_info[['score_name','index_type']]
        df_info.rename(columns={'score_name':'portfolio_name'},inplace=True)
        df=df.merge(df_info,on='portfolio_name',how='left')
        df.fillna('中证500',inplace=True)
        df = df[['valuation_date', 'code', 'weight', 'portfolio_name', 'index_type']]
        return df
    
    def productportfolio_withdraw(self):
        """
        获取产品投资组合数据
        功能：获取所有产品投资组合的权重数据
        
        返回：
            DataFrame: 包含所有产品投资组合权重数据的DataFrame
        """
        df = self.ww.product_withdraw(None)
        df_prod=product_info_withdraw()
        df_prod=df_prod[['valuation_date','product_code','index_type']]
        df_prod.rename(columns={'product_code':'portfolio_name'},inplace=True)
        df.rename(columns={'product_code':'portfolio_name'},inplace=True)
        df=df.merge(df_prod,on=['valuation_date','portfolio_name'],how='left')
        df = df[['valuation_date', 'code', 'weight', 'portfolio_name', 'index_type']]
        return df
    def Split_main(self):
        df_portinfo=self.df_portinfo.copy()
        df_weight=self.paperportfolio_withdraw()
        df_weight['portfolio_name2']=df_weight['portfolio_name'].apply(lambda x: x[-2:])
        df_weight=df_weight[df_weight['portfolio_name2'].isin(['HB','LB'])]
        df_portinfo['portfolio_name2'] = df_portinfo['score_name'].apply(lambda x: x[-2:])
        df_portinfo = df_portinfo[df_portinfo['portfolio_name2'].isin(['HB', 'LB'])]
        df_weight.drop(columns='portfolio_name2',inplace=True)
        df_portinfo.drop(columns='portfolio_name2', inplace=True)
        psc=portfolio_split_calculate(self.df_score,self.df_indexcomp,self.df_stockreturn,self.df_indexreturn,df_weight,df_portinfo)
        df_portsplit=psc.portfolio_split_main()
        df_portsplit['update_time']=self.now
        ssc=score_split_calculate(self.df_score,self.df_indexcomp,self.df_stockreturn,self.df_indexreturn,self.df_portinfo)
        df_signalsplit=ssc.score_split_main()
        df_signalsplit['update_time'] = self.now
        return df_portsplit,df_signalsplit

    def portfolioTracking_main(self):
        """
        投资组合跟踪主函数
        功能：执行投资组合级别的完整计算流程，包括数据获取、分析、处理和保存
        """
        # 获取纸面投资组合权重数据
        working_days_list=gt.working_days_list(self.start_date,self.end_date)
        df_port_weight = self.paperportfolio_withdraw()
        portfolio_name_list = df_port_weight['portfolio_name'].unique().tolist()
        # 获取产品投资组合权重数据
        df_prod_weight = self.productportfolio_withdraw()
        # 合并所有投资组合数据
        df_cal = pd.concat([df_port_weight, df_prod_weight])
        df_cal=df_cal[~(df_cal['portfolio_name']=='ubp500')]
        # 进行投资组合分析
        if self.realtime==True:
            df_info, df_detail = gt.portfolio_analyse(df_cal,cost_stock=0,realtime=self.realtime)
        else:
            df_info, df_detail = gt.portfolio_analyse(df_cal, realtime=self.realtime)
        if self.split==True:
            df_portsplit,df_signalsplit=self.Split_main()
        else:
            df_portsplit,df_signalsplit=pd.DataFrame(),pd.DataFrame()
        df_info=df_info[df_info['valuation_date'].isin(working_days_list)]
        df_detail=df_detail[df_detail['valuation_date'].isin(working_days_list)]
        # 处理投资组合信息数据
        df_info = df_info[['valuation_date', 'portfolio_name', 'paper_return', 'excess_paper_return']]
        df_info['update_time'] = self.now
        # 将收益率转换为基点单位
        df_info['Excess_Return_bp'] = round(df_info['excess_paper_return'] * 10000, 2)
        df_info['Portfolio_Return_bp'] = round(df_info['paper_return'] * 10000, 2)
        df_info = df_info[['valuation_date', 'portfolio_name', 'Excess_Return_bp', 'Portfolio_Return_bp', 'update_time']]
        
        # 分离纸面投资组合和产品投资组合数据
        df_port = df_info[df_info['portfolio_name'].isin(portfolio_name_list)]
        df_prod = df_info[~(df_info['portfolio_name'].isin(portfolio_name_list))]
        df_prod.rename(columns={'portfolio_name': 'product_code'}, inplace=True)
        #保存纸面投资组合数据到数据库
        if len(df_port) > 0:
            if self.realtime==True:
                sm = gt.sqlSaving_main(inputpath_sql, 'portfolioreturn', delete=True)
            else:
                sm = gt.sqlSaving_main(inputpath_sql, 'portfolioreturn_daily', delete=True)
            sm.df_to_sql(df_port)
        # 保存产品投资组合数据到数据库
        if len(df_prod) > 0:
            if self.realtime == True:
                sm = gt.sqlSaving_main(inputpath_sql, 'productreturn', delete=True)
            else:
                sm = gt.sqlSaving_main(inputpath_sql, 'productreturn_daily', delete=True)
            sm.df_to_sql(df_prod)
        if len(df_portsplit)>0:
            if self.realtime==True:
                sm = gt.sqlSaving_main(inputpath_sql, 'portfolio_split_realtime', delete=True)
            else:
                sm = gt.sqlSaving_main(inputpath_sql, 'portfolio_split_daily', delete=True)
            sm.df_to_sql(df_portsplit)
        if len(df_signalsplit)>0:
            if self.realtime==True:
                sm = gt.sqlSaving_main(inputpath_sql, 'score_split_realtime', delete=True)
            else:
                sm = gt.sqlSaving_main(inputpath_sql, 'score_split_daily', delete=True)
            sm.df_to_sql(df_signalsplit)

if __name__ == '__main__':
    pt=portfolio_tracking('2025-08-22','2025-08-25',realtime=False,split=True)
    print(pt.paperportfolio_withdraw())

