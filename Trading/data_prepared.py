import os
import pandas as pd
import global_setting.global_dic as glv
import sys
from datetime import datetime
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt
global source,config_path
source=glv.get('source')
config_path=glv.get('config_path')
class data_prepared:
    def __init__(self,target_date,realtime=False):
        self.target_date=target_date
        self.realtime=realtime
        if realtime==True:
            self.target_date=gt.strdate_transfer(datetime.today())
    def indexType_getting(self,product_code):
        inputpath_config = glv.get('config_product')
        excel_file = pd.ExcelFile(inputpath_config)
        sheet_names = excel_file.sheet_names
        detail_name = sheet_names[0]
        df_info = pd.read_excel(inputpath_config, detail_name)
        index_type=df_info[df_info['product_code']==product_code]['index_type'].tolist()[0]
        return index_type
    def mktData_withdraw(self):
        if self.realtime==True:
            available_date=self.target_date
        else:
            available_date=gt.last_workday_calculate(self.target_date)
        df_stock=gt.stockdata_withdraw(available_date,self.realtime)
        df_etf=gt.etfdata_withdraw(available_date,self.realtime)
        df_stock=df_stock[['code','close']]
        df_etf=df_etf[['code','close']]
        etf_pool=df_etf['code'].tolist()
        df_mkt=pd.concat([df_stock,df_etf])
        df_mkt['close']=df_mkt['close'].astype(float)
        return df_mkt,etf_pool
    def portfolioList_withdraw(self):
        inputpath = glv.get('portfolio_weight')
        inputpath = str(inputpath) + f" Where valuation_date='{self.target_date}'"
        df = gt.data_getting(inputpath, config_path)
        portfolio_list = df['portfolio_name'].unique().tolist()
        return portfolio_list
    def portfolioWeight_withdraw(self,portfolio_name):
        inputpath = glv.get('portfolio_weight')
        inputpath = str(inputpath) + f" Where portfolio_name='{portfolio_name}' And valuation_date='{self.target_date}'"
        df = gt.data_getting(inputpath, config_path)
        return df
    def productCode_withdraw(self):
        inputpath_config = glv.get('config_trading')
        df = pd.read_excel(inputpath_config, sheet_name='info_sheet')
        productCode_list = df['product_code'].unique().tolist()
        return productCode_list
    def tradingTime_withdraw(self,product_code):
        inputpath_config = glv.get('config_trading')
        df = pd.read_excel(inputpath_config, sheet_name='info_sheet')
        trading_time = df[df['product_code']==product_code]['trading_time'].tolist()[0]
        return trading_time
    def productInfo_withdraw(self, product_code):
        inputpath_config = glv.get('config_trading')
        df = pd.read_excel(inputpath_config, sheet_name='info_sheet')
        slice_df = df[df['product_code'] == product_code]
        account_money = slice_df['account_money'].tolist()[0]
        t0_money = slice_df['t0_money'].tolist()[0]
        if account_money == 'auto':
            if product_code in ['SGS958', 'SNY426', 'SSS044', 'SLA626']:
                if self.realtime == False:
                    inputpath_info = glv.get('input_info')
                    available_date = gt.last_workday_calculate(self.target_date)
                else:
                    inputpath_info = glv.get('input_info_temp')
                    available_date = self.target_date
                account_name = 'asset_value'
            elif product_code in ['SVU353', 'STH580', 'SST132']:
                available_date2 = gt.last_workday_calculate(self.target_date)
                available_date = gt.last_workday_calculate(available_date2)
                inputpath_info = glv.get('input_info_l4')
                account_name = 'StockValue'
            else:
                print('product_code不在list当中')
                raise ValueError
            inputpath_info = str(
                inputpath_info) + f" Where product_code='{product_code}' And valuation_date='{available_date}'"
            df_info = gt.data_getting(inputpath_info, config_path)
            account_money = df_info[account_name].tolist()[0]
            if account_name == 'StockValue':
                index_type=self.indexType_getting(product_code)
                index_return=gt.crossSection_index_return_withdraw(index_type, available_date2)
                account_money=(1+index_return)*account_money
        else:
            account_money = account_money
        stock_money = account_money - t0_money
        return stock_money
    def productHolding_withdraw(self,product_code):
        if self.realtime==False:
             inputpath=glv.get('product_Realholding_daily')
             available_date=gt.last_workday_calculate(self.target_date)
        else:
            inputpath=glv.get('product_Realholding_realtime')
            available_date=self.target_date
        inputpath=str(inputpath)+f" Where product_code='{product_code}' And valuation_date='{available_date}'"
        df=gt.data_getting(inputpath,config_path)
        update_time_list=df['update_time'].unique().tolist()
        update_time_list.sort()
        lastest_time=update_time_list[-1]
        df=df[df['update_time']==lastest_time]
        df=df[['code','quantity']]
        df['code']=df['code'].astype(float)
        df=gt.code_transfer(df)
        df.columns=['code','holding']
        df['holding']=df['holding'].astype(float)
        df['new_code']=df['code'].apply(lambda x: str(x)[:2])
        df=df[~(df['new_code']=='20')]
        df=df[['code','holding']]
        return df
    def productTargetWeight_withdraw(self,product_code):
        inputpath=glv.get('productTarget_weight')
        inputpath = str(inputpath) + f" Where product_code='{product_code}' And valuation_date='{self.target_date}'"
        df = gt.data_getting(inputpath, config_path)
        update_time_list = df['update_time'].unique().tolist()
        update_time_list.sort()
        lastest_time = update_time_list[-1]
        df = df[df['update_time'] == lastest_time]
        return df

if __name__ == '__main__':
    print(gt.crossSection_index_return_withdraw('中证A500', '2025-06-11'))
    # dp=data_prepared('2025-06-09',realtime=True)
    # dp.productTargetHolding_withdraw('SGS958')