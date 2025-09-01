import os
import pandas as pd
import Signal_tracking.global_setting.global_dic as glv
import global_tools_func.global_tools as gt
class data_prepare:
    def __init__(self,target_date):
        self.target_date=target_date

    def crossSection_stock_return_withdraw(self):
        target_date2 = gt.intdate_transfer(self.target_date)
        inputpath_stockreturn = glv.get('stock_return')
        input_name = gt.file_withdraw(inputpath_stockreturn, target_date2)
        inputpath_stockreturn = os.path.join(inputpath_stockreturn, input_name)
        df_stock = gt.readcsv(inputpath_stockreturn)
        df_stock.set_index('valuation_date', inplace=True)
        df_stock = df_stock.T
        df_stock.reset_index(inplace=True)
        df_stock.columns = ['code', 'return']
        return df_stock
    def crossSection_index_return_withdraw(self):
        target_date2 = gt.intdate_transfer(self.target_date)
        inputpath_indexreturn = glv.get('index_return')
        file_name = gt.file_withdraw(inputpath_indexreturn, target_date2)
        inputpath_index = os.path.join(inputpath_indexreturn, file_name)
        df = gt.readcsv(inputpath_index, dtype=str)
        return df
    def index_component_withdraw(self):
        df_hs300=gt.index_weight_withdraw('沪深300',self.target_date)
        df_zzA500 = gt.index_weight_withdraw('中证A500', self.target_date)
        df_zz500 = gt.index_weight_withdraw('中证500', self.target_date)
        df_zz1000 = gt.index_weight_withdraw('中证1000',self.target_date)
        return df_hs300,df_zzA500,df_zz500,df_zz1000




