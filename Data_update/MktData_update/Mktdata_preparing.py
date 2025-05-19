import os
import pandas as pd
import global_setting.global_dic as glv
import sys
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt
import yaml
import pymysql
from setup_logger.logger_setup import setup_logger
class indexdata_prepare:
    def __init__(self,available_date):
        self.available_date=available_date
    def raw_wind_index_data_withdraw(self):  # available_date这里是YYYYMMDD格式
        inputpath_wind = glv.get('input_wind_indexdata')
        inputpath_wind = gt.file_withdraw(inputpath_wind, self.available_date)
        df_wind = gt.readcsv(inputpath_wind)
        return df_wind
    def raw_tushare_index_data_withdraw(self):  # available_date这里是YYYYMMDD格式
        inputpath_wind = glv.get('input_tushare_indexdata')
        inputpath_wind = gt.file_withdraw(inputpath_wind, self.available_date)
        df_wind = gt.readcsv(inputpath_wind)
        try:
           df_wind['vol']=df_wind['vol']*100
           df_wind['amount']=df_wind['amount']*1000
        except:
            pass
        return df_wind
    def raw_jy_index_data_withdraw(self):  # available_date这里是YYYYMMDD格式
        inputpath_jy = glv.get('input_jy_indexdata')
        inputpath_jy = gt.file_withdraw(inputpath_jy, self.available_date)
        df_jy = gt.readcsv(inputpath_jy)
        return df_jy
class indexComponent_prepare:
    def __init__(self,available_date):
        self.available_date=available_date
    def file_name_withdraw(self,index_type,source_name):
        if index_type == '上证50':
            if source_name=='jy':
                return 'sz50Monthly'
            else:
                return'sz50'
        elif index_type == '沪深300':
            if source_name == 'jy':
                 return 'csi300Monthly'
            else:
                return 'hs300'
        elif index_type == '中证500':
            if source_name == 'jy':
                 return 'zz500Monthly'
            else:
                return'zz500'
        elif index_type == '中证1000':
            if source_name=='jy':
                 return 'zz1000Monthly'
            else:
                return 'zz1000'
        elif index_type == '中证2000':
            if source_name == 'jy':
                return 'zz2000Monthly'
            else:
                return 'zz2000'
        elif index_type=='国证2000':
            if source_name=='jy':
                return 'gz2000Monthly'
            else:
                return 'gz2000'
        else:
            if source_name == 'jy':
                return 'zzA500Monthly'
            else:
                return 'zzA500'

    def raw_jy_index_component_preparing(self,index_type):
        inputpath_component = glv.get('input_jy_indexcomponent')
        file_name = self.file_name_withdraw(index_type,'jy')
        inputpath_component_update = os.path.join(inputpath_component, file_name)
        inputpath_component_update = gt.file_withdraw(inputpath_component_update, self.available_date)
        df_daily = gt.readcsv(inputpath_component_update)
        if len(df_daily) != 0:
            df_daily.columns = ['code', 'weight', 'status']
            df_daily = df_daily[df_daily['status'] == 1]
            df_daily['weight'] = df_daily['weight'] / 100
        return df_daily
    def raw_wind_index_component_preparing(self,index_type):
        inputpath_component = glv.get('input_wind_indexcomponent')
        file_name = self.file_name_withdraw(index_type,'wind')
        inputpath_component_update = os.path.join(inputpath_component, file_name)
        inputpath_component_update = gt.file_withdraw(inputpath_component_update, self.available_date)
        df_daily = gt.readcsv(inputpath_component_update)
        if len(df_daily) != 0:
            df_daily.columns = ['date','code','weight']
            df_daily['status']=1
            df_daily=df_daily[['code', 'weight', 'status']]
            df_daily = df_daily[df_daily['status'] == 1]
            df_daily['weight'] = df_daily['weight'] / 100
        return df_daily
class stockData_preparing:
    def __init__(self,available_date):
        self.available_date =available_date

    def raw_wind_stockdata_withdraw(self):
        inputpath_stock = glv.get('input_wind_stock')
        inputpath_stock = gt.file_withdraw(inputpath_stock, self.available_date)
        df_stock = gt.readcsv(inputpath_stock)
        available_date2 = pd.to_datetime(self.available_date)
        available_date2 = available_date2.strftime('%Y-%m-%d')
        df_stock['valuation_date'] = available_date2
        def trade_status(x):
            if x=='交易':
                return 1
            else:
                return 0
        try:
            df_stock['tarde_status']= df_stock['tarde_status'].apply(lambda x:trade_status(x))
        except:
            try:
                df_stock['trade_status'] = df_stock['trade_status'].apply(lambda x: trade_status(x))
            except:
                pass
        return df_stock

    def raw_jy_stockdata_withdraw(self):
        inputpath_stock = glv.get('input_jy_stock')
        inputpath_stock = gt.file_withdraw(inputpath_stock, self.available_date)
        df_stock = gt.readcsv(inputpath_stock)
        available_date2 = pd.to_datetime(self.available_date)
        available_date2 = available_date2.strftime('%Y-%m-%d')
        try:
            df_stock['valuation_date'] = available_date2
            df_stock.drop(columns=['adjFactor','adjConst'],inplace=True)
        except:
            pass
        return df_stock
class futureData_preparing:
    def __init__(self,available_date):
        self.available_date =available_date

    def raw_wind_futuredata_withdraw(self):
        inputpath_stock = glv.get('input_futuredata_wind')
        inputpath_stock = gt.file_withdraw(inputpath_stock, self.available_date)
        df_stock = gt.readcsv(inputpath_stock)
        available_date2 = pd.to_datetime(self.available_date)
        available_date2 = available_date2.strftime('%Y-%m-%d')
        df_stock['valuation_date'] = available_date2
        return df_stock

    def raw_tushare_futuredata_withdraw(self):
        inputpath_stock = glv.get('input_futuredata_tushare')
        inputpath_stock = gt.file_withdraw(inputpath_stock, self.available_date)
        df_stock = gt.readcsv(inputpath_stock)
        available_date2 = pd.to_datetime(self.available_date)
        available_date2 = available_date2.strftime('%Y-%m-%d')
        df_stock['valuation_date'] = available_date2
        try:
            df_stock['amount']=df_stock['amount']*10000
        except:
            pass
        return df_stock

    def raw_jy_futuredata_withdraw(self):
        inputpath_stock = glv.get('input_futuredata_jy')
        inputpath_stock = gt.file_withdraw(inputpath_stock, self.available_date)
        df_stock = gt.readcsv(inputpath_stock)
        available_date2 = pd.to_datetime(self.available_date)
        available_date2 = available_date2.strftime('%Y-%m-%d')
        df_stock['valuation_date'] = available_date2
        return df_stock
class optionData_preparing:
    def __init__(self,available_date):
        self.available_date =available_date

    def raw_wind_optiondata_withdraw(self):
        inputpath_stock = glv.get('input_optiondata_wind')
        inputpath_stock = gt.file_withdraw(inputpath_stock, self.available_date)
        df_stock = gt.readcsv(inputpath_stock)
        available_date2 = pd.to_datetime(self.available_date)
        available_date2 = available_date2.strftime('%Y-%m-%d')
        df_stock['valuation_date'] = available_date2
        return df_stock

    def raw_tushare_optiondata_withdraw(self):
        inputpath_stock = glv.get('input_optiondata_tushare')
        inputpath_stock = gt.file_withdraw(inputpath_stock, self.available_date)
        df_stock = gt.readcsv(inputpath_stock)
        available_date2 = pd.to_datetime(self.available_date)
        available_date2 = available_date2.strftime('%Y-%m-%d')
        df_stock['valuation_date'] = available_date2
        try:
           df_stock['amount']=df_stock['amount']*10000
        except:
            pass
        return df_stock
class etfData_preparing:
    def __init__(self,available_date):
        self.available_date =available_date

    def raw_wind_etfdata_withdraw(self):
        inputpath_stock = glv.get('input_etfdata_wind')
        inputpath_stock = gt.file_withdraw(inputpath_stock, self.available_date)
        df_stock = gt.readcsv(inputpath_stock)
        available_date2 = pd.to_datetime(self.available_date)
        available_date2 = available_date2.strftime('%Y-%m-%d')
        df_stock['valuation_date'] = available_date2
        return df_stock

    def raw_tushare_etfdata_withdraw(self):
        inputpath_stock = glv.get('input_etfdata_tushare')
        inputpath_stock = gt.file_withdraw(inputpath_stock, self.available_date)
        df_stock = gt.readcsv(inputpath_stock)
        available_date2 = pd.to_datetime(self.available_date)
        available_date2 = available_date2.strftime('%Y-%m-%d')
        df_stock['valuation_date'] = available_date2
        try:
           df_stock['amount']=df_stock['amount']*1000
        except:
            pass
        return df_stock
class CBData_preparing:
    def __init__(self,available_date):
        self.available_date =available_date
    def raw_wind_cbdata_withdraw(self):
        inputpath_stock = glv.get('input_cbdata_wind')
        inputpath_stock = gt.file_withdraw(inputpath_stock, self.available_date)
        df_stock = gt.readcsv(inputpath_stock)
        available_date2 = pd.to_datetime(self.available_date)
        available_date2 = available_date2.strftime('%Y-%m-%d')
        df_stock['valuation_date'] = available_date2
        return df_stock

    def raw_tushare_cbdata_withdraw(self):
        inputpath_stock = glv.get('input_cbdata_tushare')
        inputpath_stock = gt.file_withdraw(inputpath_stock, self.available_date)
        df_stock = gt.readcsv(inputpath_stock)
        available_date2 = pd.to_datetime(self.available_date)
        available_date2 = available_date2.strftime('%Y-%m-%d')
        df_stock['valuation_date'] = available_date2
        inputpath = glv.get('input_cbdata_info_tushare')
        inputpath2=glv.get('input_cbdata_conp_tushare')
        if int(self.available_date)<20250325:
            inputpath=gt.file_withdraw(inputpath,'20250325')
            inputpath2=gt.file_withdraw(inputpath2,'20250325')
        else:
            inputpath = gt.file_withdraw(inputpath, self.available_date)
            inputpath2 = gt.file_withdraw(inputpath2, self.available_date)
        df_info = gt.readcsv(inputpath)
        if df_info.empty:
            print('没有找到tushare'+self.available_date+'的文件')
            df_stock=pd.DataFrame()
        else:
            df_info = df_info[['ts_code', 'stk_code', 'maturity_date', 'conv_price']]
            df_conv = gt.readcsv(inputpath2)
            df_conv = df_conv[df_conv['change_date'] < int(self.available_date)]
            df_conv.drop_duplicates(subset='ts_code', keep='last', inplace=True)
            df_conv = df_conv[['ts_code', 'change_date', 'convertprice_aft']]
            df_stock = df_stock.merge(df_info, on='ts_code', how='left')
            df_stock = df_stock.merge(df_conv, on='ts_code', how='left')
            df_stock = df_stock[
                ['ts_code', 'stk_code', 'close', 'pre_close', 'maturity_date', 'convertprice_aft', 'conv_price']]
            df_stock.loc[df_stock['convertprice_aft'].isna(), ['convertprice_aft']] = \
            df_stock[df_stock['convertprice_aft'].isna()]['conv_price']
            df_stock.drop(columns='conv_price', inplace=True)
            df_stock.rename(columns={'convertprice_aft': 'conv_price'}, inplace=True)
            df_stock['maturity_date'] = df_stock['maturity_date'].astype(int).astype(str)
            df_stock['maturity_date'] = pd.to_datetime(df_stock['maturity_date'])
            df_stock['maturity'] = (df_stock['maturity_date'] - pd.to_datetime(self.available_date)).dt.days / 365
            df_stock.drop(columns='maturity_date', inplace=True)
        return df_stock
class LHB_amt_prepare:
    def __init__(self, available_date):
        self.available_date = available_date
    def raw_LHB_tushare_withdraw(self):
        inputpath=glv.get('input_lhb_tushare')
        inputpath2=glv.get('output_indexdata')
        available_date2=gt.intdate_transfer(self.available_date)
        inputpath=gt.file_withdraw(inputpath,available_date2)
        inputpath2=gt.file_withdraw(inputpath2,available_date2)
        df_lhb=gt.readcsv(inputpath)
        df_index=gt.readcsv(inputpath2)
        return df_lhb,df_index
class NLB_amt_prepare:
    def __init__(self, available_date):
        self.available_date = available_date
    def raw_NLB_wind_withdraw(self):
        inputpath=glv.get('input_nlb_wind')
        inputpath2=glv.get('output_indexdata')
        available_date2=gt.intdate_transfer(self.available_date)
        inputpath=gt.file_withdraw(inputpath,available_date2)
        inputpath2=gt.file_withdraw(inputpath2,available_date2)
        df_nlb=gt.readcsv(inputpath)
        df_index=gt.readcsv(inputpath2)
        return df_nlb,df_index
class FutureDifference_prepare:
    def __init__(self, available_date):
        self.available_date = available_date
    def raw_futureDifference_withdraw(self):
        inputpath=glv.get('output_futuredata')
        inputpath2=glv.get('output_indexdata')
        available_date2=gt.intdate_transfer(self.available_date)
        inputpath=gt.file_withdraw(inputpath,available_date2)
        inputpath2=gt.file_withdraw(inputpath2,available_date2)
        df_future=gt.readcsv(inputpath)
        df_index=gt.readcsv(inputpath2)
        return df_future,df_index
if __name__ == '__main__':
    cbp=BankMomentum_prepare('2025-05-06')
    cbp.raw_BankMomentum_prepare()