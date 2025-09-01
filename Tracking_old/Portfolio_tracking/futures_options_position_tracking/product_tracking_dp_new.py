import datetime
import os
import pandas as pd
import yaml
import re
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
        realtime_data_stock=pd.read_excel(inputpath_realtime,sheet_name='stockprice')
        columns_list = realtime_data_index.columns.tolist()
        if '000510.SH' in columns_list:
            realtime_data_index.rename(columns={'000510.SH': '000510.CSI'}, inplace=True)
        realtime_data_index=realtime_data_index[['valuation_date','000016.SH','000300.SH','000905.SH','000852.SH','932000.CSI','999004.SSI','000510.CSI']]
        realtime_data_index.columns=['valuation_date','上证50','沪深300','中证500','中证1000','中证2000','华证微盘','中证A500']
        realtime_data_future = realtime_data_future[['代码', '日期', '时间', '现价', '合约系数', '前结算价']]
        realtime_data_future.columns = ['future', 'date', 'time', 'future_price', 'ratio', '前结算价']
        realtime_data_future['future']=realtime_data_future['future'].str.split('.').str[0]
        realtime_data_future.dropna(axis=0, inplace=True)
        return realtime_data_stock,realtime_data_df, realtime_data_future,realtime_data_index,realtime_data_etf
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
    def __init__(self,product_name,realtime_index):
        self.product_name=product_name
        self.realtime_index=realtime_index
    def position_withdraw_hy(self,simulation=False):
        inputpath_holding = glv.get('future_info_xy')
        if simulation==False:
            inputpath_holding=os.path.join(inputpath_holding,'future')
        else:
            inputpath_holding=os.path.join(inputpath_holding,'future_simulation')
        input_list1 = os.listdir(inputpath_holding)
        input_list1 = [i for i in input_list1 if '持仓_' in i]
        input_list1.sort()
        input_name = input_list1[-1]
        input_name2=input_list1[-2]
        date1 = str('20') + input_name[-10:-4]
        date1 = gt.strdate_transfer(date1)
        inputpath_holding2=os.path.join(inputpath_holding,input_name2)
        inputpath_holding = os.path.join(inputpath_holding, input_name)
        df_holding = gt.readcsv(inputpath_holding)
        df_holding2 = gt.readcsv(inputpath_holding2)
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        if date1 != today:
            print('holding最新更新日期为:' + str(date1))
        if self.product_name == '惠盈一号':
            df_holding.rename(columns={'方向': '买卖', '持仓': '总持仓'}, inplace=True)
            df_holding2.rename(columns={'方向': '买卖', '持仓': '总持仓'}, inplace=True)
        df_holding['昨仓'] = df_holding['总持仓']
        df_holding['今仓'] = df_holding['昨仓']
        df_holding2['昨仓'] = df_holding2['总持仓']
        df_holding2['今仓'] = df_holding2['昨仓']
        return df_holding,df_holding2
    def position_withdraw_rr(self):
        inputpath_holding = glv.get('future_info_rr')
        input_list1 = os.listdir(inputpath_holding)
        if self.product_name=='瑞锐精选':
            input_list1 = [i for i in input_list1 if 'PositionDetail(金砖1号)' in i]
        elif self.product_name=='瑞锐500':
            input_list1 = [i for i in input_list1 if 'PositionDetail(量化500增强2)' in i]
        input_list1.sort()
        input_name = input_list1[-1]
        input_name2=input_list1[-2]
        date1 = input_name[-13:-5]
        date1 = gt.strdate_transfer(date1)
        inputpath_holding2 = os.path.join(inputpath_holding, input_name2)
        inputpath_holding = os.path.join(inputpath_holding, input_name)
        df_holding = gt.readcsv(inputpath_holding)
        df_holding2=gt.readcsv(inputpath_holding2)
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        if date1 != today:
            print('holding最新更新日期为:' + str(date1))
        df_holding=df_holding[['合约代码','多空','总持仓','昨仓']]
        df_holding2 = df_holding2[['合约代码', '多空', '总持仓', '昨仓']]
        def direction_transfer(x):
            if x =='多':
                return '买'
            else:
                return '卖'
        df_holding['买卖']=df_holding['多空'].apply(lambda x: direction_transfer(x))
        df_holding2['买卖'] = df_holding2['多空'].apply(lambda x: direction_transfer(x))
        df_holding=df_holding[['合约代码','买卖','总持仓','昨仓']]
        df_holding.columns=['合约','买卖','总持仓','昨仓']
        df_holding2 = df_holding2[['合约代码', '买卖', '总持仓', '昨仓']]
        df_holding2.columns = ['合约', '买卖', '总持仓', '昨仓']
        return df_holding,df_holding2
    def position_withdraw_renr(self):
        inputpath_holding = glv.get('future_info_rr')
        input_list1 = os.listdir(inputpath_holding)
        if self.product_name=='瑞锐精选':
            input_list1 = [i for i in input_list1 if 'PositionDetail(金砖1号)' in i]
        input_list1.sort()
        input_name = input_list1[-1]
        input_name2=input_list1[-2]
        date1 = input_name[-13:-5]
        date1 = gt.strdate_transfer(date1)
        inputpath_holding2 = os.path.join(inputpath_holding, input_name2)
        inputpath_holding = os.path.join(inputpath_holding, input_name)
        df_holding = gt.readcsv(inputpath_holding)
        df_holding2=gt.readcsv(inputpath_holding2)
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        if date1 != today:
            print('holding最新更新日期为:' + str(date1))
        df_holding=df_holding[['合约代码','多空','总持仓','昨仓']]
        df_holding2 = df_holding2[['合约代码', '多空', '总持仓', '昨仓']]
        def direction_transfer(x):
            if x =='多':
                return '买'
            else:
                return '卖'
        df_holding['买卖']=df_holding['多空'].apply(lambda x: direction_transfer(x))
        df_holding2['买卖'] = df_holding2['多空'].apply(lambda x: direction_transfer(x))
        df_holding=df_holding[['合约代码','买卖','总持仓','昨仓']]
        df_holding.columns=['合约','买卖','总持仓','昨仓']
        df_holding2 = df_holding2[['合约代码', '买卖', '总持仓', '昨仓']]
        df_holding2.columns = ['合约', '买卖', '总持仓', '昨仓']
        return df_holding,df_holding2
    def position_withdraw_other(self,simulation=False):
        if self.product_name=='念觉沪深300':
            inputpath=glv.get('future_info_rj')
        elif self.product_name=='念觉知行4号':
            inputpath=glv.get('future_info_zx')
        elif self.product_name=='高益振英一号':
            inputpath=glv.get('future_info_gy')
        elif self.product_name=='仁睿价值精选1号':
            inputpath = glv.get('future_info_renr')
        else:
            raise ValueError
        if simulation==False:
            inputpath=os.path.join(inputpath,'future')
        else:
            inputpath=os.path.join(inputpath,'future_simulation')
        target_date=datetime.date.today()
        target_date=gt.intdate_transfer(target_date)
        inputpath_daily=gt.file_withdraw(inputpath,target_date)
        df=gt.readcsv(inputpath_daily)
        # print(inputpath_daily)
        # print(df)
        df.columns=['合约','总持仓','方向']
        df['昨仓']=df['总持仓']
        def direction_transfer(x):
            if x =='long':
                return '买'
            else:
                return '卖'
        df['买卖']=df['方向'].apply(lambda x: direction_transfer(x))
        df['总持仓']=df['总持仓'].astype(int)
        df.drop(columns='方向',inplace=True)
        return df,df
    def position_withdraw(self,simulation=False):
        if self.product_name=='惠盈一号':
            df_holding,df_holding2=self.position_withdraw_hy(simulation)
        elif self.product_name=='瑞锐精选' or self.product_name=='瑞锐500':
            df_holding,df_holding2=self.position_withdraw_rr()
        else:
            df_holding,df_holding2=self.position_withdraw_other(simulation)
        return df_holding,df_holding2
    def HY_info_withdraw(self,yes):
        inputpath_stock=glv.get('product_holding')
        inputlist_stock=os.listdir(inputpath_stock)
        inputlist_stock = [i for i in inputlist_stock if 'Account' in i]
        inputlist_stock.sort()
        if yes==False:
             input_name=inputlist_stock[-1]
        else:
             input_name = inputlist_stock[-2]
        inputpath_stock=os.path.join(inputpath_stock,input_name)
        date1 = input_name[-13:-5]
        date1 = gt.strdate_transfer(date1)
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        if date1 != today:
            print('account提取日期为:' + str(date1))
        df_account=gt.readcsv(inputpath_stock)
        stock_money=df_account['股票总市值'].tolist()[0]
        today = datetime.date.today()
        yes=gt.last_workday_calculate(today)
        yes2=gt.last_workday_calculate(yes)
        inputpath=glv.get('data_l4')
        inputpath=os.path.join(inputpath,'宣夜惠盈一号')
        input_list=os.listdir(inputpath)
        input_name=[i for i in input_list if gt.intdate_transfer(yes2) in i][0]
        inputpath=os.path.join(inputpath,input_name)
        df=pd.read_excel(inputpath)
        asset_value=df['资产净值'].tolist()[0]
        asset_value=asset_value
        return stock_money,asset_value
    def renR_info_withdraw(self,yes):
        inputpath_stock=glv.get('product_holding_renr')
        inputlist_stock=os.listdir(inputpath_stock)
        inputlist_stock = [i for i in inputlist_stock if 'Account' in i]
        inputlist_stock.sort()
        if yes==False:
             input_name=inputlist_stock[-1]
        else:
             input_name = inputlist_stock[-2]
        inputpath_stock=os.path.join(inputpath_stock,input_name)
        date1 = input_name[-12:-4]
        date1 = gt.strdate_transfer(date1)
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        if date1 != today:
            print('account提取日期为:' + str(date1))
        df_account=gt.readcsv(inputpath_stock)
        stock_money=df_account['股票总市值'].tolist()
        stock_money.sort()
        stock_money=stock_money[1]
        today = datetime.date.today()
        yes=gt.last_workday_calculate(today)
        yes2=gt.last_workday_calculate(yes)
        inputpath=glv.get('data_l4')
        inputpath=os.path.join(inputpath,'仁睿价值精选1号')
        input_list=os.listdir(inputpath)
        input_name=[i for i in input_list if gt.intdate_transfer(yes2) in i][0]
        inputpath=os.path.join(inputpath,input_name)
        df=pd.read_excel(inputpath)
        asset_value=df['资产净值'].tolist()[0]
        asset_value=asset_value
        index_return = gt.crossSection_index_return_withdraw('中证500', yes)
        asset_value = (1 + float(index_return)) * asset_value
        index_return_realtime = self.realtime_index['中证500'].tolist()[0]
        asset_value = (1 + index_return_realtime) * asset_value
        return stock_money,asset_value
    def RR_info_withdraw(self,yes):
        inputpath_stock = glv.get('product_holding_rr')
        inputlist_stock = os.listdir(inputpath_stock)
        if self.product_name == '瑞锐精选':
            inputlist_stock = [i for i in inputlist_stock if 'Account(金砖1号)' in i]
        elif self.product_name == '瑞锐500':
            inputlist_stock = [i for i in inputlist_stock if 'Account(量化500增强2)' in i]
        inputlist_stock.sort()
        if yes==False:
             input_name = inputlist_stock[-1]
        else:
             input_name=inputlist_stock[-2]
        inputpath_stock = os.path.join(inputpath_stock, input_name)
        file_date_list = re.findall(r'\d{8}',input_name)
        date1 = file_date_list[-1]
        # date1 = input_name[-13:-5]
        date1 = gt.strdate_transfer(date1)
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        if date1 != today:
            print('account提取更新日期为:' + str(date1))
        df_account = gt.readcsv(inputpath_stock)
        stock_money = df_account['股票总市值'].tolist()[0]
        today = datetime.date.today()
        yes = gt.last_workday_calculate(today)
        yes2 = gt.last_workday_calculate(yes)
        inputpath = glv.get('data_l4')
        inputpath=os.path.join(inputpath,self.product_name)
        input_list = os.listdir(inputpath)
        input_name = [i for i in input_list if gt.intdate_transfer(yes2) in i][0]
        inputpath = os.path.join(inputpath, input_name)
        df = pd.read_excel(inputpath)
        asset_value = df['资产净值'].tolist()[0]
        asset_value = asset_value
        if self.product_name=='瑞锐500':
            index_return=gt.crossSection_index_return_withdraw('中证500',yes)
            asset_value=(1+float(index_return))*asset_value
            index_return_realtime=self.realtime_index['中证500'].tolist()[0]
            asset_value = (1 + index_return_realtime) * asset_value
        return stock_money, asset_value
    def other_info_withdraw(self):
        today = datetime.date.today()
        yes=gt.last_workday_calculate(today)
        yes2=gt.last_workday_calculate(yes)
        yes2=gt.intdate_transfer(yes2)
        yes=gt.intdate_transfer(yes)
        inputpath=glv.get('data_l4')
        inputpath=os.path.join(inputpath,self.product_name)
        inputpath=gt.file_withdraw(inputpath,yes2)
        df=pd.read_excel(inputpath)
        asset_value=df['资产净值'].tolist()[0]
        asset_value=asset_value
        stock_money=df['股票市值'].tolist()[0]
        if self.product_name=='念觉沪深300':
            index_return=gt.crossSection_index_return_withdraw('沪深300',yes)
        elif self.product_name=='念觉知行4号':
            index_return=gt.crossSection_index_return_withdraw('中证A500',yes)
        else:
            index_return=0
        stock_money=(1+float(index_return))*stock_money
        asset_value=(1+float(index_return))*asset_value
        return stock_money,asset_value
    def etf_pool_withdraw(self):
        inputpath = glv.get('input_other')
        inputpath = os.path.join(inputpath, 'ETF_pool.csv')
        df = gt.readcsv(inputpath)
        code_list = df['wind_code'].tolist()
        return code_list
    def portfolio_weight_withdraw(self):
        etf_code=self.etf_pool_withdraw()
        target_date=datetime.date.today()
        target_date=gt.intdate_transfer(target_date)
        inputpath=glv.get('product_weight')
        if self.product_name=='惠盈一号':
            product_name2='宣夜惠盈1号'
        elif self.product_name=='瑞锐精选':
            product_name2='瑞锐精选'
        elif self.product_name=='瑞锐500':
            product_name2='瑞锐500指增'
        elif self.product_name=='高益振英一号':
            product_name2='高毅振英1号'
        elif self.product_name=='念觉沪深300':
            product_name2='念空瑞景39号'
        elif self.product_name=='念觉知行4号':
            product_name2='念空知行4号'
        elif self.product_name=='仁睿价值精选1号':
            product_name2='仁睿价值精选1号'
        else:
            product_name2=self.product_name
        inputpath=os.path.join(inputpath,product_name2)
        inputpath=gt.file_withdraw(inputpath,target_date)
        df=gt.readcsv(inputpath)
        df['new_code']=df['code'].apply(lambda x: str(x)[:6])
        df=df[~(df['new_code'].isin(etf_code))]
        return df
    def stock_info_dicesion(self):
        if self.product_name=='惠盈一号':
            stock_money, asset_value=self.HY_info_withdraw(yes=False)
        elif self.product_name=='瑞锐精选' or self.product_name=='瑞锐500':
            stock_money, asset_value = self.RR_info_withdraw(yes=False)
        elif self.product_name=='仁睿价值精选1号':
            stock_money, asset_value = self.renR_info_withdraw(yes=False)
        else:
            stock_money, asset_value = self.other_info_withdraw()
        return stock_money,asset_value
    def cb_data_withdraw(self,available_date):
        inputpath=glv.get('cb_data')
        available_date=gt.intdate_transfer(available_date)
        inputpath=gt.file_withdraw(inputpath,available_date)
        df=gt.readcsv(inputpath)
        return df
    def hy_stockinfo_analyse(self,df_etf,df_stock):
        df2=df_etf.copy()
        df2=df2[['代码','前收','现价']]
        df2.rename(columns={'代码': 'code'}, inplace=True)
        df3=df_stock.copy()
        df3=df3[['代码','close','pre_close']]
        df3.columns=['code','现价','前收']
        df_all=pd.concat([df2,df3])
        inputpath_holding=glv.get('product_holding')
        input_list = os.listdir(inputpath_holding)
        input_list = [i for i in input_list if 'PositionDetail' in i]
        input_list.sort()
        input_name=input_list[-1]
        date1 = input_name[-13:-5]
        date1 = gt.strdate_transfer(date1)
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        if date1 != today:
            print('positionDetail最新更新日期为:' + str(date1))
        inputpath_holding = os.path.join(inputpath_holding, input_name)
        df=gt.readcsv(inputpath_holding)
        df=df[['证券代码','当前拥股','昨夜拥股','市值','当日涨幅']]
        df.columns = ['code', 'HoldingQty','HoldingQty_yes','mkt_value','return']
        df['new_code']=df['code'].apply(lambda x: str(x)[0])
        df=df[~(df['new_code']=='2')]
        df=gt.code_transfer(df)
        df_holding=df.copy()
        df = df.merge(df_all, on='code', how='left')
        df['money']=df['现价']*df['HoldingQty']
        df['profit'] = (df['现价'] - df['前收']) * df['HoldingQty']
        etf_code=self.etf_pool_withdraw()
        df_etf=df[df['code'].isin(etf_code)]
        df_convertible_bond = df[df['现价'].isna()]
        code_list_cb = df_convertible_bond['code'].tolist()
        df_stock = df[~(df['code'].isin(list(set(etf_code) | set(code_list_cb))))]
        date2 = gt.last_workday_calculate(date1)
        df_cb = self.cb_data_withdraw(date2)
        df_cb = df_cb[['code', 'delta']]
        df_convertible_bond=df_convertible_bond.merge(df_cb,on='code',how='left')
        df_convertible_bond['mkt_value']=df_convertible_bond['mkt_value']*df_convertible_bond['delta']
        cb_money = df_convertible_bond['mkt_value'].sum()
        stock_money=df_stock['money'].sum()
        etf_money=df_etf['money'].sum()
        stock_profit = df_stock['profit'].sum()
        etf_profit = df_etf['profit'].sum()
        df_convertible_bond['return']=df_convertible_bond['return'].apply(lambda x: str(x)[:-1])
        df_convertible_bond['return']=df_convertible_bond['return'].astype(float)/100
        df_convertible_bond['profit'] = df_convertible_bond['mkt_value'] - (
                    df_convertible_bond['mkt_value'] / (1 + df_convertible_bond['return']))
        cb_profit = df_convertible_bond['profit'].sum()
        return stock_money, etf_money, cb_money,stock_profit, etf_profit,cb_profit,df_holding
    def renr_stockinfo_analyse(self,df_etf,df_stock):
        df2=df_etf.copy()
        df2=df2[['代码','前收','现价']]
        df2.rename(columns={'代码': 'code'}, inplace=True)
        df3=df_stock.copy()
        df3=df3[['代码','close','pre_close']]
        df3.columns=['code','现价','前收']
        df_all=pd.concat([df2,df3])
        inputpath_holding=glv.get('product_holding_renr')
        input_list = os.listdir(inputpath_holding)
        input_list = [i for i in input_list if 'PositionStatics-' in i]
        input_list.sort()
        input_name=input_list[-1]
        date1 = input_name[-12:-4]
        date1 = gt.strdate_transfer(date1)
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        if date1 != today:
            print('positionDetail最新更新日期为:' + str(date1))
        inputpath_holding = os.path.join(inputpath_holding, input_name)
        df=gt.readcsv(inputpath_holding)
        df=df[['证券代码','当前拥股','昨夜拥股','市值','当日涨幅']]
        df.columns = ['code', 'HoldingQty','HoldingQty_yes','mkt_value','return']
        df['new_code']=df['code'].apply(lambda x: str(x)[0])
        df=gt.code_transfer(df)
        df_holding=df.copy()
        df = df.merge(df_all, on='code', how='left')
        df['money']=df['现价']*df['HoldingQty']
        df['profit'] = (df['现价'] - df['前收']) * df['HoldingQty']
        etf_code=self.etf_pool_withdraw()
        df_etf=df[df['code'].isin(etf_code)]
        df_convertible_bond = df[df['现价'].isna()]
        code_list_cb = df_convertible_bond['code'].tolist()
        df_stock = df[~(df['code'].isin(list(set(etf_code) | set(code_list_cb))))]
        date2 = gt.last_workday_calculate(date1)
        df_cb = self.cb_data_withdraw(date2)
        df_cb = df_cb[['code', 'delta']]
        df_convertible_bond=df_convertible_bond.merge(df_cb,on='code',how='left')
        df_convertible_bond['mkt_value']=df_convertible_bond['mkt_value']*df_convertible_bond['delta']
        cb_money = df_convertible_bond['mkt_value'].sum()
        stock_money=df_stock['money'].sum()
        etf_money=df_etf['money'].sum()
        stock_profit = df_stock['profit'].sum()
        etf_profit = df_etf['profit'].sum()
        df_convertible_bond['return']=df_convertible_bond['return'].apply(lambda x: str(x)[:-1])
        df_convertible_bond['return']=df_convertible_bond['return'].astype(float)/100
        df_convertible_bond['profit'] = df_convertible_bond['mkt_value'] - (
                    df_convertible_bond['mkt_value'] / (1 + df_convertible_bond['return']))
        cb_profit = df_convertible_bond['profit'].sum()
        return stock_money, etf_money, cb_money,stock_profit, etf_profit,cb_profit,df_holding
    def rr_stockinfo_analyse(self,df_etf,df_stock):
        df2 = df_etf.copy()
        df2 = df2[['代码', '前收', '现价']]
        df2.rename(columns={'代码': 'code'}, inplace=True)
        df3 = df_stock.copy()
        df3 = df3[['代码', 'close', 'pre_close']]
        df3.columns = ['code', '现价', '前收']
        df_all = pd.concat([df2, df3])
        inputpath_holding=glv.get('product_holding_rr')
        input_list = os.listdir(inputpath_holding)
        if self.product_name == '瑞锐精选':
            input_list = [i for i in input_list if 'PositionDetail(金砖1号)' in i]
        elif self.product_name == '瑞锐500':
            input_list = [i for i in input_list if 'PositionDetail(量化500增强2)' in i]
        input_list.sort()
        input_name=input_list[-1]
        file_date_list = re.findall(r'\d{8}', input_name)
        date1 = file_date_list[-1]
        # date1 = input_name[-13:-5]
        date1 = gt.strdate_transfer(date1)
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        if date1 != today:
            print('positionDetail最新更新日期为:' + str(date1))
        inputpath_holding = os.path.join(inputpath_holding, input_name)
        df=gt.readcsv(inputpath_holding)
        df=df[['证券代码','当前拥股','昨夜拥股','市值','当日涨幅']]
        df.columns = ['code', 'HoldingQty','HoldingQty_yes','mkt_value','return']
        df['new_code']=df['code'].apply(lambda x: str(x)[0])
        df=df[~(df['new_code']=='2')]
        df=gt.code_transfer(df)
        df_holding=df.copy()
        df = df.merge(df_all, on='code', how='left')
        df['money'] = df['现价'] * df['HoldingQty']
        df['profit'] = (df['现价'] - df['前收']) * df['HoldingQty']
        etf_code = self.etf_pool_withdraw()
        df_etf = df[df['code'].isin(etf_code)]
        df_convertible_bond = df[df['现价'].isna()]
        code_list_cb=df_convertible_bond['code'].tolist()
        df_stock = df[~(df['code'].isin(list(set(etf_code)|set(code_list_cb))))]
        stock_money = df_stock['money'].sum()
        etf_money = df_etf['money'].sum()
        date2 = gt.last_workday_calculate(date1)
        df_cb = self.cb_data_withdraw(date2)
        df_cb = df_cb[['code', 'delta']]
        df_convertible_bond = df_convertible_bond.merge(df_cb, on='code', how='left')
        df_convertible_bond['mkt_value_risk'] = df_convertible_bond['mkt_value'] * df_convertible_bond['delta']
        cb_money=df_convertible_bond['mkt_value_risk'].sum()
        stock_profit = df_stock['profit'].sum()
        etf_profit = df_etf['profit'].sum()
        df_convertible_bond['return']=df_convertible_bond['return'].apply(lambda x: str(x)[:-1])
        df_convertible_bond['return']=df_convertible_bond['return'].astype(float)/100
        df_convertible_bond['profit']=df_convertible_bond['mkt_value']-(df_convertible_bond['mkt_value']/(1+df_convertible_bond['return']))
        cb_profit=df_convertible_bond['profit'].sum()
        return stock_money, etf_money, cb_money,stock_profit, etf_profit,cb_profit,df_holding

    def other_stockinfo_analyse(self,df_etf,df_stock):
        df2 = df_etf.copy()
        df2 = df2[['代码', '前收', '现价']]
        df2.rename(columns={'代码': 'code'}, inplace=True)
        df3 = df_stock.copy()
        df3 = df3[['代码', 'close', 'pre_close']]
        df3.columns = ['code', '现价', '前收']
        df_all = pd.concat([df2, df3])
        inputpath_holding=glv.get('data_l4_holding')
        inputpath_holding=os.path.join(inputpath_holding,self.product_name)
        today = datetime.date.today()
        yes = gt.last_workday_calculate(today)
        yes2 = gt.last_workday_calculate(yes)
        yes2 = gt.intdate_transfer(yes2)
        inputpath_holding=gt.file_withdraw(inputpath_holding,yes2)
        df_stock=pd.read_excel(inputpath_holding,'stock_tracking')
        df_cb=pd.read_excel(inputpath_holding,'c_bond_tracking')
        df_etf=pd.read_excel(inputpath_holding,'etf_tracking')
        df_stock=df_stock[['代码','数量']]
        df_cb = df_cb[['代码', '数量']]
        df_etf= df_etf[['代码', '数量']]
        df_stock.columns=['code','HoldingQty']
        df_cb.columns = ['code', 'HoldingQty']
        df_etf.columns = ['code', 'HoldingQty']
        df_holding=pd.concat([df_stock,df_etf,df_cb])
        df_holding['HoldingQty_yes']=df_holding['HoldingQty']
        df_stock=gt.code_transfer(df_stock)
        df_convertible_bond=gt.code_transfer(df_cb)
        df_etf=gt.code_transfer(df_etf)
        df_stock=df_stock.merge(df_all,on='code',how='left')
        df_stock['HoldingQty_yes']=df_stock['HoldingQty']
        df_stock['money'] = df_stock['现价'] * df_stock['HoldingQty']
        df_stock['profit'] = (df_stock['现价'] - df_stock['前收']) * df_stock['HoldingQty']
        df_etf=df_etf.merge(df_all,on='code',how='left')
        df_etf['HoldingQty_yes']=df_etf['HoldingQty']
        df_etf['money'] = df_etf['现价'] * df_etf['HoldingQty']
        df_etf['profit'] = (df_etf['现价'] - df_etf['前收']) * df_etf['HoldingQty']
        date2 = gt.last_workday_calculate(today)
        df_cb = self.cb_data_withdraw(date2)
        df_cb = df_cb[['code', 'delta','close']]
        df_convertible_bond = df_convertible_bond.merge(df_cb, on='code', how='left')
        df_convertible_bond['mkt_value']=df_convertible_bond['HoldingQty']*df_convertible_bond['close']
        df_convertible_bond['mkt_value_risk'] = df_convertible_bond['mkt_value'] * df_convertible_bond['delta']
        cb_money=df_convertible_bond['mkt_value_risk'].sum()
        stock_profit = df_stock['profit'].sum()
        etf_profit = df_etf['profit'].sum()
        stock_money = df_stock['money'].sum()
        etf_money = df_etf['money'].sum()
        cb_profit=0
        return stock_money, etf_money, cb_money,stock_profit, etf_profit,cb_profit,df_holding
    def stockinfo_analyse(self,df_etf,df_stock):
        if self.product_name=='惠盈一号':
            stock_money, etf_money, cb_money,stock_profit, etf_profit,cb_profit,df_holding=self.hy_stockinfo_analyse(df_etf,df_stock)
        elif self.product_name=='瑞锐精选' or self.product_name=='瑞锐500':
            stock_money, etf_money, cb_money,stock_profit, etf_profit,cb_profit,df_holding=self.rr_stockinfo_analyse(df_etf,df_stock)
        elif self.product_name=='仁睿价值精选1号':
            stock_money, etf_money, cb_money, stock_profit, etf_profit, cb_profit, df_holding = self.renr_stockinfo_analyse(
                df_etf, df_stock)
        else:
            stock_money, etf_money, cb_money, stock_profit, etf_profit, cb_profit, df_holding=self.other_stockinfo_analyse(df_etf,df_stock)
        return stock_money, etf_money, cb_money,stock_profit, etf_profit,cb_profit,df_holding
if __name__ == '__main__':
    dp=data_prepared()
    realtime_data_stock, realtime_data_df, realtime_data_future, realtime_data_index, realtime_data_etf=dp.realtime_data_withdraw()
    pda=product_data('瑞锐精选','xxxx')
    print(pda.other_stockinfo_analyse(realtime_data_etf,realtime_data_stock))


