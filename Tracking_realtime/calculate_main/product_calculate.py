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

import datetime
import global_tools as gt
import global_setting.global_dic as glv
import numpy as np
from data.data_prepared import futureoption_position, security_position, prod_info,get_product_detail,factorexposure_withdraw
from calculate_main.exposure_calculate import exposure_tracking
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

class product_tracking:
    """
    产品跟踪计算类
    功能：处理单个产品的完整计算流程，包括数据获取、处理、分析和结果输出
    """
    
    def __init__(self, start_date,end_date,product_code,realtime):
        """
        初始化方法
        功能：初始化产品跟踪对象，获取所有必要的持仓数据和产品信息
        
        参数：
            product_code (str): 产品代码
        """
        self.product_code = product_code
        self.start_date=start_date
        self.end_date=end_date
        self.realtime=realtime
        if self.realtime==True:
            today = datetime.date.today()
            date = gt.strdate_transfer(today)
            self.start_date=self.end_date=date
        # 获取期货期权持仓数据
        fp = futureoption_position(start_date,end_date,product_code,realtime)
        self.df_future, self.df_option = fp.futureoption_withdraw_main()
        # 处理期货数据，分离今日和昨日数据
        self.df_indexFuture, self.df_commFuture, self.df_bond = self.future_split(self.df_future)
        # 获取股票ETF可转债持仓数据
        sp = security_position(start_date,end_date,product_code,realtime)
        self.df_stock, self.df_etf, self.df_cb = sp.security_withdraw_main()
        self.asset_type=get_product_detail(self.product_code,'type')
        self.index_type = get_product_detail(self.product_code, 'index')
        # 获取产品资产价值
        pi = prod_info(start_date,end_date,product_code,realtime)
        self.df_asset = pi.assetvalue_withdraw()
        fw=factorexposure_withdraw(start_date,end_date,realtime)
        self.df_stockexposure=fw.stock_exposure_withdraw()
        self.df_indexexposure=fw.index_exposure_withdraw()
        self.df_factorreturn=fw.factorreturn_withdraw()
        # 设置当前日期和时间
        today = datetime.date.today()
        self.date = gt.strdate_transfer(today)
        self.now = datetime.datetime.now().replace(tzinfo=None)
    
    def direction_prossing(self, x):
        """
        处理持仓方向
        功能：将中文方向标识转换为数值
        
        参数：
            x (str): 中文方向标识
            
        返回：
            int: 方向数值（1表示多头，-1表示空头）
        """
        if x>0:
            return '多'
        else:
            return '空'
    
    def futureOption_processing(self, df):
        """
        处理期货期权数据
        功能：将数值方向转换为英文标识，并取绝对值
        
        参数：
            df (DataFrame): 期货期权数据框
            
        返回：
            DataFrame: 处理后的数据框
        """
        def direction_processing2(x):
            if x > 0:
                return 'long'
            else:
                return 'short'
        
        df['direction'] = df['quantity'].apply(lambda x: direction_processing2(x))
        df['quantity'] = abs(df['quantity'])
        return df
    def future_split(self, df):
        """
        分离期货类型
        功能：根据代码前缀分离指数期货、商品期货和债券期货
        
        参数：
            df (DataFrame): 期货数据框
            
        返回：
            DataFrame: 指数期货数据框
            DataFrame: 商品期货数据框
            DataFrame: 债券期货数据框
        """
        # 根据代码前缀分类
        df['new_code'] = df['code'].apply(lambda x: str(x)[:1])
        df_future = df[~(df['new_code'] == 'T')]  # 非债券期货
        df_indexFuture = df_future[df_future['new_code'] == 'I']  # 指数期货
        df_commFuture = df_future[~(df_future['new_code'] == 'I')]  # 商品期货
        df_bond = df[df['new_code'] == 'T']  # 债券期货
        
        # 删除临时列
        df_indexFuture.drop(columns='new_code', inplace=True)
        df_commFuture.drop(columns='new_code', inplace=True)
        df_bond.drop(columns='new_code', inplace=True)
        return df_indexFuture, df_commFuture, df_bond
    def partial_analysis(self):
        """
        部分分析
        功能：对各类资产进行部分分析，计算风险指标和收益
        
        返回：
            DataFrame: 投资组合信息
            DataFrame: 详细持仓信息
        """
        # 为各类资产添加投资组合名称
        df_indexFuture = self.df_indexFuture.copy()
        df_indexFuture['portfolio_name'] = 'indexfuture'
        df_commFuture = self.df_commFuture.copy()
        df_commFuture['portfolio_name'] = 'commfuture'
        df_bond = self.df_bond.copy()
        df_bond['portfolio_name'] = 'bond'
        df_stock = self.df_stock.copy()
        df_stock['portfolio_name'] = 'stock'
        df_option = self.df_option.copy()
        df_option['portfolio_name'] = 'option'
        df_etf = self.df_etf.copy()
        df_etf['portfolio_name'] = 'etf'
        df_cb = self.df_cb.copy()
        df_cb['portfolio_name'] = 'cb'
        # 合并所有资产数据
        df_port = pd.concat([df_indexFuture, df_commFuture, df_bond, df_stock, df_option, df_etf, df_cb])
        df_port=df_port[['valuation_date','code','quantity','portfolio_name']]
        # # 进行投资组合分析
        if self.realtime==True:
            df_info, df_detail = gt.portfolio_analyse(df_port, cost_stock=0, cost_etf=0, cost_future=0, cost_option=0, cost_convertiblebond=0, realtime=True, weight_standardize=True)
        else:
            df_info, df_detail = gt.portfolio_analyse(df_port, weight_standardize=True)
        return df_info, df_detail
    def df_transform(self,df):
        df2 = pd.DataFrame()
        if len(df)>0:
            portfolio_name = df['portfolio_name'].tolist()[0]
            df2['valuation_date'] = df['valuation_date'].tolist()
            df2[str(portfolio_name) + '_profit'] = df['portfolio_profit'].tolist()
            df2[str(portfolio_name) + '_return'] = df['portfolio_return'].tolist()
            df2[str(portfolio_name) + '_excess_return'] = df['excess_portfolio_return'].tolist()
            df2[str(portfolio_name) + '_mktvalue'] = df['portfolio_mktvalue'].tolist()
            df2[str(portfolio_name) + '_index_return'] = df['index_return'].tolist()
        return df2
    def info_split(self, df_info, df_detail):
        """
        信息分离
        功能：将分析结果按资产类型分离
        
        参数：
            df_info (DataFrame): 投资组合信息
            df_detail (DataFrame): 详细持仓信息
            
        返回：
            dict: 各类资产的收益和市值信息
        """

        portfolio_name_list=['indexfuture','commfuture','bond','stock','option','etf','cb']
        df_info_output = pd.DataFrame()
        df_info_output['valuation_date']=df_info['valuation_date'].unique().tolist()
        for portfolio_name in portfolio_name_list:
            slice_df_info=df_info[df_info['portfolio_name'] == portfolio_name]
            slice_df_info=self.df_transform(slice_df_info)
            if len(slice_df_info)!=0:
                 df_info_output=df_info_output.merge(slice_df_info,on='valuation_date',how='left')
            else:
                 df_info_output[str(portfolio_name) + '_profit']=0
                 df_info_output[str(portfolio_name) + '_mktvalue'] = 0
        return df_info_output,df_detail
    
    def trading_action_processing(self):
        """
        交易行为处理
        功能：分析持仓变化，识别交易行为（买入、卖出、持仓不变）
        
        返回：
            DataFrame: 包含交易行为分析的数据框
        """
        # 为各类资产添加类型标识
        df_stock = self.df_stock.copy()
        df_stock['type'] = 'stock'
        df_future = self.df_future.copy()
        df_future['type'] = 'future'
        df_option = self.df_option.copy()
        df_option['type'] = 'option'
        df_etf = self.df_etf.copy()
        df_etf['type'] = 'etf'
        
        # 合并所有资产数据
        df_final = pd.concat([df_stock, df_future, df_option, df_etf])
        df_final['direction']=df_final['quantity'].apply(lambda x: self.direction_prossing(x))
        df_final = df_final[['valuation_date','code', 'quantity', 'pre_quantity', 'direction', 'type']]
        # 计算持仓变化
        df_final['difference'] = df_final['quantity'] - df_final['pre_quantity']
        def action_decision(x):
            """
            交易行为判断
            功能：根据持仓变化判断交易行为
            
            参数：
                x (float): 持仓变化量
                
            返回：
                str: 交易行为类型
            """
            if x > 0:
                return '开仓'
            elif x < 0:
                return '平仓'
            else:
                return '不变'
        
        # 添加交易行为列
        df_final['action'] = df_final['difference'].apply(action_decision)
        df_final=df_final[~(df_final['action']=='不变')]
        df_final['product_code']=self.product_code
        df_final.rename(columns={'quantity':'HoldingQty','pre_quantity':'HoldingQty_yes'},inplace=True)
        return df_final
    def product_info_processing(self):
        """
        产品信息处理
        功能：处理产品的基本信息，包括资产价值、各类资产收益等
        
        返回：
            DataFrame: 产品信息数据框
        """
        # 获取各类资产分析结果
        df_info, df_detail=self.partial_analysis()
        df_info,df_detail= self.info_split(df_info, df_detail)
        df_info=df_info.merge(self.df_asset,on='valuation_date',how='left')
        df_info['NetAssetValue_yes']=df_info['NetAssetValue'].shift(1)
        working_days_list=gt.working_days_list(self.start_date,self.end_date)
        df_info=df_info[df_info['valuation_date'].isin(working_days_list)]
        df_detail = df_detail[df_detail['valuation_date'].isin(working_days_list)]
        df_info['tracking_profit']=df_info['stock_profit']+df_info['indexfuture_profit']+df_info['commfuture_profit']+df_info['option_profit']+df_info['etf_profit']+df_info['cb_profit']+df_info['bond_profit']
        df_info['tracking_mktvalue'] = df_info['stock_mktvalue'] + df_info['indexfuture_mktvalue'] + df_info[
            'commfuture_mktvalue'] + df_info['option_mktvalue'] + df_info['etf_mktvalue'] + df_info['cb_mktvalue'] + df_info[
                                      'bond_mktvalue']
        for asset_type in ['stock','option','indexfuture','commfuture','cb','etf']:
            df_info['proportion_'+str(asset_type)] = df_info[str(asset_type)+'_mktvalue'] / df_info['NetAssetValue']
        if self.asset_type=='中性':
            df_info['indexfuture_excess_return']=df_info['indexfuture_excess_return']+2*df_info['indexfuture_index_return']
        df_info['leverage_ratio']=df_info['tracking_mktvalue']/df_info['NetAssetValue']
        df_info['tracking_product_return']=df_info['tracking_profit']/df_info['NetAssetValue_yes']
        df_info['NetAssetValue']=df_info['NetAssetValue']+df_info['RedemptionAmount']-df_info['SubscriptionAmount']
        df_info['product_profit']=df_info['NetAssetValue']-df_info['NetAssetValue_yes']
        df_info['product_return'] = df_info['NetAssetValue']/df_info['NetAssetValue_yes']-1
        df_info['tracking_profit_error']=df_info['product_profit']-df_info['tracking_profit']
        df_info['tracking_return_error'] = df_info['product_return'] - df_info['tracking_product_return']
        df_info.drop(columns=['NetAssetValue_yes'],inplace=True)
        days_list=df_info['valuation_date'].tolist()
        df_output=pd.DataFrame()
        for days in days_list:
            slice_df_info=df_info[df_info['valuation_date']==days]
            slice_df_info.drop(columns='valuation_date',inplace=True)
            slice_df_info=slice_df_info.T
            slice_df_info.reset_index(inplace=True)
            slice_df_info.columns = ['type', 'value']
            slice_df_info['valuation_date'] = days
            df_output=pd.concat([df_output,slice_df_info])
        df_output=df_output[['valuation_date','type','value']]
        df_output['product_code'] = self.product_code
        df_output['update_time']=self.now
        df_output2=df_detail[df_detail['portfolio_name'].isin(['indexfuture','option'])]
        df_output2 = df_output2[['valuation_date','code', 'quantity', 'delta', 'risk_mkt_value', 'profit', 'portfolio_name']]
        df_output2.rename(columns={'risk_mkt_value': 'mkt_value'}, inplace=True)
        df_output2 = self.futureOption_processing(df_output2)
        df_output2 = df_output2[['valuation_date','code', 'direction', 'quantity', 'delta', 'mkt_value', 'profit']]
        df_output2['product_code'] = self.product_code
        df_output2.rename(columns={'quantity': 'HoldingQty', 'profit': 'daily_profit'}, inplace=True)
        df_output2['update_time'] = self.now
        return df_info,df_detail,df_output,df_output2

    def productTracking_main(self):
        """
        产品跟踪主函数
        功能：执行产品级别的完整计算流程，包括数据获取、分析、处理和保存
        """
        # 获取产品信息
        if self.realtime==False:
            df_info, df_detail, df_output, df_output2 = self.product_info_processing()
            et = exposure_tracking(df_detail, df_info, self.df_stockexposure, self.df_indexexposure,
                                   self.df_factorreturn, self.index_type)
            df_exposure = et.final_portfolio_exposure_processing()
            df_exposure['product_code']=self.product_code
            df_exposure['update_time']=self.now
            sm = gt.sqlSaving_main(inputpath_sql, 'product_factor', delete=True)
            sm.df_to_sql(df_exposure, 'product_code', self.product_code)
        # 获取交易行为数据
        df_action = self.trading_action_processing()

        # 保存产品信息到数据库
        if len(df_output) > 0:
            if self.realtime==True:
                  sm = gt.sqlSaving_main(inputpath_sql, 'proinfo', delete=True)
            else:
                sm = gt.sqlSaving_main(inputpath_sql, 'proinfo_daily', delete=True)
            sm.df_to_sql(df_output,'product_code',self.product_code)
        if len(df_action)>0:
            if self.realtime==True:
                 sm3 = gt.sqlSaving_main(inputpath_sql, 'holding_changing', delete=True)
            else:
                sm3 = gt.sqlSaving_main(inputpath_sql, 'holding_changing_daily', delete=True)
            sm3.df_to_sql(df_action, 'product_code', self.product_code)
        if len(df_output2) > 0:
            if self.realtime==True:
                sm = gt.sqlSaving_main(inputpath_sql, 'optionfuture_holding', delete=True)
            else:
                sm = gt.sqlSaving_main(inputpath_sql, 'optionfuture_holding_daily', delete=True)
            sm.df_to_sql(df_output2,'product_code',self.product_code)

        return df_output, df_action, df_output2
if __name__ == '__main__':
    pt=product_tracking('2025-08-22','2025-08-25','SST132',False)
    print(pt.productTracking_main())
    # for product_code in ['SGS958', 'SVU353', 'SNY426', 'SSS044', 'STH580', 'SST132', 'SLA626']:
    #     print(product_code)
    #     for realtime in [False]:
    #         print(realtime)
    #         if realtime==True:
    #             start_date=end_date='2025-08-27'
    #         else:
    #             start_date=end_date='2025-08-22'
    #         fp = prod_info(start_date,end_date, product_code, realtime)
    #         df= fp.assetvalue_withdraw()
    #         print(df)














