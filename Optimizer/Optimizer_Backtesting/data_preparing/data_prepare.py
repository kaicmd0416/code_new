import sys
import global_tools_func.global_tools as gt
import pandas as pd
import os
import warnings
import Optimizer_Backtesting.global_setting.global_dic as glv
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
class timeseries_section_data_preparing:
    def time_series_data_preparing(self):  # only need to run one time
        # index_return
        df_indexreturn = gt.timeSeries_index_return_withdraw()
        # stock_return
        inputpath_stock = glv.get('input_timeseries')
        inputpath_stock = os.path.join(inputpath_stock, 'stock_return.csv')
        df_stock = gt.readcsv(inputpath_stock)
        df_stock['valuation_date'] = pd.to_datetime(df_stock['valuation_date'])
        df_stock['valuation_date'] = df_stock['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        # 静态文件
        inputpath_st = glv.get('input_other')
        inputpath_st = os.path.join(inputpath_st, 'st_stock.xlsx')
        df_st = pd.read_excel(inputpath_st)
        inputpath_stockuniverse = glv.get('input_other')
        inputpath_stockuniverse = os.path.join(inputpath_stockuniverse, 'StockUniverse_new.csv')
        df_stockuniverse = gt.readcsv(inputpath_stockuniverse)
        return df_indexreturn, df_stock, df_st, df_stockuniverse