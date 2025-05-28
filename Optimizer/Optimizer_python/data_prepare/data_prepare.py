import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt
import json
import pandas as pd
import os
import warnings
import global_setting.global_dic as glv
warnings.filterwarnings("ignore")
global source,config_path

def source_getting():
    """
    获取数据源配置

    Returns:
        str: 数据源模式（'local' 或 'sql'）
    """
    try:
        current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_path = os.path.join(current_dir, 'global_setting\\optimizer_path_config.json')
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        source = config_data['components']['data_source']['mode']
    except Exception as e:
        print(f"获取配置出错: {str(e)}")
        source = 'local'
    return source,config_path
source,config_path= source_getting()
class cross_section_data_preparing:
    def __init__(self,available_date):
        self.available_date=available_date
    def index_component_withdraw(self):
        df_hs300 = gt.index_weight_withdraw(index_type='沪深300', available_date=self.available_date)
        df_zz500 = gt.index_weight_withdraw(index_type='中证500', available_date=self.available_date)
        df_zz1000 = gt.index_weight_withdraw(index_type='中证1000', available_date=self.available_date)
        df_zz2000 = gt.index_weight_withdraw(index_type='中证2000', available_date=self.available_date)
        df_zzA500 = gt.index_weight_withdraw(index_type='中证A500', available_date=self.available_date)
        return df_hs300, df_zz500, df_zz1000, df_zz2000, df_zzA500

    def index_exposure_withdraw(self):
        df_hs300 = gt.crossSection_index_factorexposure_withdraw(index_type='沪深300', available_date=self.available_date)
        df_zz500 = gt.crossSection_index_factorexposure_withdraw(index_type='中证500', available_date=self.available_date)
        df_zz1000 = gt.crossSection_index_factorexposure_withdraw(index_type='中证1000', available_date=self.available_date)
        df_zz2000 = gt.crossSection_index_factorexposure_withdraw(index_type='中证2000', available_date=self.available_date)
        df_zzA500=gt.crossSection_index_factorexposure_withdraw(index_type='中证A500', available_date=self.available_date)
        return df_hs300, df_zz500, df_zz1000, df_zz2000,df_zzA500

    def stock_pool_withdraw(self):
        available_date2 = gt.intdate_transfer(self.available_date)
        inputpath_stockpool = glv.get('input_factorstockpool')
        if '\\' in inputpath_stockpool:
              inputpath_stockpool = gt.file_withdraw(inputpath_stockpool, available_date2)
        else:
            inputpath_stockpool=str(inputpath_stockpool)+f" WHERE valuation_date = '{self.available_date}'"
        df_stockpool = gt.data_getting(inputpath_stockpool,config_path)
        return df_stockpool

    def stock_factor_exposure_withdraw(self):
        available_date2 = gt.intdate_transfer(self.available_date)
        inputpath_factor = glv.get('input_factorexposure')
        if source=='local':
             inputpath_factor = gt.file_withdraw(inputpath_factor, available_date2)
        else:
             inputpath_factor=str(inputpath_factor)+f" WHERE valuation_date = '{self.available_date}'"
        df_factor = gt.data_getting(inputpath_factor,config_path)
        df_factor.sort_values(by='code',inplace=True)
        return df_factor
    def factor_cov_withdraw(self):
        available_date2=gt.intdate_transfer(self.available_date)
        inputpath_cov=glv.get('input_factorcov')
        if source == 'local':
              inputpath_cov=gt.file_withdraw(inputpath_cov,available_date2)
        else:
              inputpath_cov=str(inputpath_cov)+f" WHERE valuation_date = '{self.available_date}'"
        df= gt.data_getting(inputpath_cov,config_path)
        df.rename(columns={'factor_name':'covariance'},inplace=True)
        return df
    def factor_risk_withdraw(self):
        available_date2=gt.intdate_transfer(self.available_date)
        inputpath_cov=glv.get('input_factorrisk')
        if source == 'local':
               inputpath_cov=gt.file_withdraw(inputpath_cov,available_date2)
        else:
              inputpath_cov=str(inputpath_cov)+f" WHERE valuation_date = '{self.available_date}'"
        df=gt.data_getting(inputpath_cov,config_path)
        df.set_index('code',inplace=True,drop=True)
        df=df[['specificrisk']]
        df=df.T
        df.reset_index(inplace=True,drop=True)
        return df
class stable_data_preparing:
    def stable_data_preparing(self):  # only need to run one time
        # 静态文件
        inputpath_st = glv.get('st_stock')
        df_st = gt.data_getting(inputpath_st,config_path)
        df_st.columns=['code']
        inputpath_stockuniverse = glv.get('stock_universe_new')
        if source=='sql':
            inputpath_stockuniverse=str(inputpath_stockuniverse)+" WHERE type = 'stockuni_new'"
        df_stockuniverse = gt.data_getting(inputpath_stockuniverse,config_path)
        return  df_st, df_stockuniverse
if __name__ == '__main__':
    dp=cross_section_data_preparing('2025-05-22')
    print(dp.weight_yes_withdraw('combine_zz500_HB','2025-05-24'))