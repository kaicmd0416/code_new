import sys
import global_tools_func.global_tools as gt
import pandas as pd
import os
import warnings
import Optimizer_python.global_setting.global_dic as glv
warnings.filterwarnings("ignore")
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
        df_hs300 = gt.crossSection_index_factorexposure_withdraw_new(index_type='沪深300', available_date=self.available_date)
        df_zz500 = gt.crossSection_index_factorexposure_withdraw_new(index_type='中证500', available_date=self.available_date)
        df_zz1000 = gt.crossSection_index_factorexposure_withdraw_new(index_type='中证1000', available_date=self.available_date)
        df_zz2000 = gt.crossSection_index_factorexposure_withdraw_new(index_type='中证2000', available_date=self.available_date)
        df_zzA500=gt.crossSection_index_factorexposure_withdraw_new(index_type='中证A500', available_date=self.available_date)
        return df_hs300, df_zz500, df_zz1000, df_zz2000,df_zzA500

    def stock_pool_withdraw(self):
        available_date2 = gt.intdate_transfer(self.available_date)
        inputpath_stockpool = glv.get('input_factorstockpool')
        inputpath_stockpool = gt.file_withdraw(inputpath_stockpool, available_date2)
        df_stockpool = gt.readcsv(inputpath_stockpool)
        return df_stockpool

    def stock_factor_exposure_withdraw(self):
        available_date2 = gt.intdate_transfer(self.available_date)
        inputpath_factor = glv.get('input_factorexposure')
        inputpath_other = glv.get('input_other')
        inputpath_other = os.path.join(inputpath_other, 'StockUniverse_new.csv')
        inputpath_factor = gt.file_withdraw(inputpath_factor, available_date2)
        df_factor = gt.readcsv(inputpath_factor)
        df_stockuniverse = gt.readcsv(inputpath_other)
        stock_list = df_stockuniverse['S_INFO_WINDCODE'].tolist()
        df_factor['code'] = stock_list
        return df_factor
    def factor_cov_withdraw(self):
        available_date2=gt.intdate_transfer(self.available_date)
        inputpath_cov=glv.get('input_factorcov')
        inputpath_cov=gt.file_withdraw(inputpath_cov,available_date2)
        df=gt.readcsv(inputpath_cov)
        return df
    def factor_risk_withdraw(self):
        available_date2=gt.intdate_transfer(self.available_date)
        inputpath_cov=glv.get('input_factorrisk')
        inputpath_cov=gt.file_withdraw(inputpath_cov,available_date2)
        df=gt.readcsv(inputpath_cov)
        return df

class stable_data_preparing:
    def stable_data_preparing(self):  # only need to run one time
        # 静态文件
        inputpath_st = glv.get('input_other')
        inputpath_st = os.path.join(inputpath_st, 'st_stock.xlsx')
        df_st = pd.read_excel(inputpath_st)
        df_st.columns=['code']
        inputpath_stockuniverse = glv.get('input_other')
        inputpath_stockuniverse = os.path.join(inputpath_stockuniverse, 'StockUniverse_new.csv')
        df_stockuniverse = gt.readcsv(inputpath_stockuniverse)
        return  df_st, df_stockuniverse