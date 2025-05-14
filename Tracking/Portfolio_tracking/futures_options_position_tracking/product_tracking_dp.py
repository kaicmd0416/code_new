import datetime
import os
import pandas as pd
import yaml
import datetime as datetime
import Portfolio_tracking.global_setting.global_dic as glv
import global_tools_func.global_tools as gt
import numpy as np
class data_prepared:
    def realtime_data_withdraw(self):
        inputpath_realtime=glv.get('realtime_data')
        realtime_data_df = pd.read_excel(inputpath_realtime, sheet_name='option_info')
        realtime_data_future = pd.read_excel(inputpath_realtime, sheet_name='future_info')
        realtime_data_index=pd.read_excel(inputpath_realtime,sheet_name='indexreturn')
        realtime_data_etf=pd.read_excel(inputpath_realtime,sheet_name='etf_info')
        columns_list = realtime_data_index.columns.tolist()
        if '000510.SH' in columns_list:
            realtime_data_index.rename(columns={'000510.SH': '000510.CSI'}, inplace=True)
        realtime_data_index=realtime_data_index[['valuation_date','000016.SH','000300.SH','000905.SH','000852.SH','932000.CSI','999004.SSI','000510.CSI']]
        realtime_data_index.columns=['valuation_date','上证50','沪深300','中证500','中证1000','中证2000','华证微盘','中证A500']
<<<<<<< Updated upstream
<<<<<<< Updated upstream
        realtime_data_future = realtime_data_future[['简称', '日期', '时间', '现价', '合约系数', '前结算价']]
=======
        realtime_data_future = realtime_data_future[['代码', '日期', '时间', '现价', '合约系数', '前结算价']]
>>>>>>> Stashed changes
=======
        realtime_data_future = realtime_data_future[['代码', '日期', '时间', '现价', '合约系数', '前结算价']]
>>>>>>> Stashed changes
        realtime_data_future.columns = ['future', 'date', 'time', 'future_price', 'ratio', '前结算价']
        realtime_data_future.dropna(axis=0, inplace=True)
        return realtime_data_df, realtime_data_future,realtime_data_index,realtime_data_etf
    def stock_factor_exposure_withdraw(self):
        available_date = gt.last_workday_calculate(datetime.date.today())
        available_date2 = gt.intdate_transfer(available_date)
        inputpath_factor = glv.get('input_factorexposure')
        inputpath_other = glv.get('input_other')
        inputpath_other = os.path.join(inputpath_other, 'StockUniverse_new.csv')
        inputpath_factor = gt.file_withdraw(inputpath_factor, available_date2)
        df_factor = gt.readcsv(inputpath_factor)
        df_stockuniverse = gt.readcsv(inputpath_other)
        stock_list = df_stockuniverse['S_INFO_WINDCODE'].tolist()
        df_factor['code'] = stock_list
        return df_factor
    def index_exposure_withdraw(self):
        available_date = gt.last_workday_calculate(datetime.date.today())
        df_sz50 = gt.crossSection_index_factorexposure_withdraw_new(index_type='上证50',
                                                                     available_date=available_date)
        df_hs300 = gt.crossSection_index_factorexposure_withdraw_new(index_type='沪深300', available_date=available_date)
        df_zz500 = gt.crossSection_index_factorexposure_withdraw_new(index_type='中证500', available_date=available_date)
        df_zz1000 = gt.crossSection_index_factorexposure_withdraw_new(index_type='中证1000', available_date=available_date)
        df_zz2000 = gt.crossSection_index_factorexposure_withdraw_new(index_type='中证2000', available_date=available_date)
        df_zzA500=gt.crossSection_index_factorexposure_withdraw_new(index_type='中证A500', available_date=available_date)
        df_sz50.drop(columns='valuation_date', inplace=True)
        df_hs300.drop(columns='valuation_date',inplace=True)
        df_zz500.drop(columns='valuation_date',inplace=True)
        df_zz1000.drop(columns='valuation_date',inplace=True)
        df_zz2000.drop(columns='valuation_date', inplace=True)
        df_zzA500.drop(columns='valuation_date', inplace=True)
        return df_sz50,df_hs300, df_zz500, df_zz1000, df_zz2000,df_zzA500
class product_data:
    def __init__(self,product_name):
        self.product_name=product_name

    def product_index_withdraw(self, product_name):
        inputpath_product = glv.get('product_detail')
        df_proindex = pd.read_excel(inputpath_product,sheet_name='product_detail')
        if product_name == '惠盈一号':
            product_name2 = '宣夜惠盈1号'
        elif product_name == '盛元8号':
            product_name2 = '盛丰500指增8号'
        else:
            raise ValueError
        index_type = df_proindex[df_proindex['product_name'] == product_name2]['index_type'].tolist()[0]
        return index_type
    def position_withdraw(self):
        inputpath_holding=glv.get('future_info')
        inputpath_holding=os.path.join(inputpath_holding,self.product_name)
        input_list1=os.listdir(inputpath_holding)
        input_list1=[i for i in input_list1 if '持仓_' in i]
        input_list1.sort()
        input_name=input_list1[-1]
        date1=str('20')+input_name[-10:-4]
        date1=gt.strdate_transfer(date1)
        inputpath_holding=os.path.join(inputpath_holding,input_name)
        df_holding=gt.readcsv(inputpath_holding)
        today=datetime.date.today()
        today=gt.strdate_transfer(today)
        if date1!=today:
            print('holding最新更新日期为:'+str(date1))
        if self.product_name=='惠盈一号':
             df_holding.rename(columns={'方向':'买卖','持仓':'总持仓'},inplace=True)
        return df_holding

    def HY_info_withdraw(self):
<<<<<<< Updated upstream
<<<<<<< Updated upstream
        index_type=self.product_index_withdraw(self.product_name)
=======
=======
>>>>>>> Stashed changes
        inputpath_stock=glv.get('product_holding')
        inputlist_stock=os.listdir(inputpath_stock)
        inputlist_stock=[i for i in inputlist_stock if 'Account' in i]
        inputlist_stock.sort()
        input_name=inputlist_stock[-1]
        inputpath_stock=os.path.join(inputpath_stock,input_name)
        df_account=gt.readcsv(inputpath_stock)
        stock_money=df_account['股票总市值'].tolist()[0]
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
        today = datetime.date.today()
        yes=gt.last_workday_calculate(today)
        yes2=gt.last_workday_calculate(yes)
        inputpath=glv.get('data_l4')
        inputpath=os.path.join(inputpath,'宣夜惠盈一号')
        input_list=os.listdir(inputpath)
        input_name=[i for i in input_list if gt.intdate_transfer(yes2) in i][0]
        inputpath=os.path.join(inputpath,input_name)
        df=pd.read_excel(inputpath)
<<<<<<< Updated upstream
<<<<<<< Updated upstream
        stock_money=df['股票市值'].tolist()[0]
        asset_value=df['资产净值'].tolist()[0]
        index_return_yes=float(gt.crossSection_index_return_withdraw(index_type,available_date=yes))
        stock_money=stock_money*(1+index_return_yes)
        asset_value=asset_value*(1+index_return_yes)
=======
        asset_value=df['资产净值'].tolist()[0]
        asset_value=asset_value
>>>>>>> Stashed changes
=======
        asset_value=df['资产净值'].tolist()[0]
        asset_value=asset_value
>>>>>>> Stashed changes
        return stock_money,asset_value
    def SY_info_withdraw(self):
        index_type = self.product_index_withdraw(self.product_name)
        today = datetime.date.today()
        yes = gt.last_workday_calculate(today)
        yes2 = gt.last_workday_calculate(yes)
        inputpath = glv.get('data_l4')
        inputpath = os.path.join(inputpath, '盛元8号')
        input_list = os.listdir(inputpath)
        input_name = [i for i in input_list if gt.intdate_transfer(yes2) in i][0]
        inputpath = os.path.join(inputpath, input_name)
        df = pd.read_excel(inputpath)
        stock_money = df['股票市值'].tolist()[0]
        asset_value = df['资产净值'].tolist()[0]
        index_return_yes = float(gt.crossSection_index_return_withdraw(index_type, available_date=yes))
        stock_money = stock_money * (1 + index_return_yes)
        asset_value = asset_value * (1 + index_return_yes)
        return stock_money, asset_value
    def money_changing(self):
        inputpath = os.path.split(os.path.realpath(__file__))[0]
        inputpath=os.path.join(inputpath,'product_money_changing.xlsx')
        df=pd.read_excel(inputpath)
        product_name2 = None
        if self.product_name=='惠盈一号':
            product_name2='HY'
        elif self.product_name=='盛元8号':
            product_name2='SY'
        else:
            raise ValueError
        stock_money_change=df[df['product_name']==product_name2]['stock_money_change'].tolist()[0]
        asset_money_change = df[df['product_name'] == product_name2]['asset_money_change'].tolist()[0]
        return  stock_money_change,asset_money_change

    def etf_pool_withdraw(self):
        inputpath = glv.get('input_other')
        inputpath = os.path.join(inputpath, 'ETF_pool.csv')
        df = gt.readcsv(inputpath)
        df['code'] = df['wind_code'].apply(lambda x: str(x)[:6])
        code_list = df['code'].tolist()
        return code_list
    def portfolio_weight_withdraw(self):
        etf_code=self.etf_pool_withdraw()
        target_date=datetime.date.today()
        target_date=gt.intdate_transfer(target_date)
        inputpath=glv.get('product_weight')
        if self.product_name=='惠盈一号':
            product_name2='宣夜惠盈1号'
        elif self.product_name=='盛元8号':
            product_name2='盛丰500指增8号'
        else:
            raise ValueError
        inputpath=os.path.join(inputpath,product_name2)
        inputpath=gt.file_withdraw(inputpath,target_date)
        df=gt.readcsv(inputpath)
        df['new_code']=df['code'].apply(lambda x: str(x)[:6])
        df=df[~(df['new_code'].isin(etf_code))]
        return df
    def stock_info_dicesion(self):
        if self.product_name=='惠盈一号':
            stock_money, asset_value=self.HY_info_withdraw()
        elif self.product_name=='盛元8号':
            stock_money, asset_value = self.SY_info_withdraw()
        else:
            raise ValueError
        stock_change_money,asset_change_money=self.money_changing()
        stock_money=stock_money-stock_change_money
        asset_value=asset_value-asset_change_money
        return stock_money,asset_value
    def etf_money_calculate(self,df_etf):
        df2=df_etf.copy()
        df2.rename(columns={'代码':'code'},inplace=True)
        today = datetime.date.today()
        yes = gt.last_workday_calculate(today)
        inputpath_holding=glv.get('product_holding')
        input_list = os.listdir(inputpath_holding)
<<<<<<< Updated upstream
<<<<<<< Updated upstream
        input_name = [i for i in input_list if gt.intdate_transfer(yes) in i][0]
        inputpath_holding = os.path.join(inputpath_holding, input_name)
        df=gt.readcsv(inputpath_holding)
=======
=======
>>>>>>> Stashed changes
        input_name = [i for i in input_list if gt.intdate_transfer(yes) in i and 'PositionDetail' in i][0]
        inputpath_holding = os.path.join(inputpath_holding, input_name)
        df=gt.readcsv(inputpath_holding)
        df=df[['证券代码','当前拥股']]
        df.columns=['StockCode','HoldingQty']
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
        df['code']=df['StockCode'].apply(lambda x: str(x)[:6])
        etf_code=self.etf_pool_withdraw()
        df=df[df['code'].isin(etf_code)]
        df=gt.code_transfer(df)
        df=df.merge(df2,on='code',how='left')
        df['money']=df['现价']*df['HoldingQty']
        etf_money=df['money'].sum()
        return etf_money


