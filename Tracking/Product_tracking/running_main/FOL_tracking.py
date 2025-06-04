import os
import pandas as pd
import scipy.stats as stats
import Product_tracking.global_setting.global_dic as glv
import global_tools_func.global_tools as gt
import numpy as np
from Product_tracking.tools_func.tools_func import product_type
from Product_tracking.data_prepare.product_tracking_dp import data_prepared,product_data
from Product_tracking.tools_func.tools_func import delta_calculator
class portfolio_tracking:
    def __init__(self,product_name,available_date):
        self.available_date=available_date
        self.product_name=product_name
        dp=data_prepared(available_date)
        prod=product_data(product_name,available_date)
        self.realtime_data_future,self.realtime_data_index=dp.realtime_data_withdraw(yes=False)
        self.realtime_data_future_yes,self.realtime_data_index_yes=dp.realtime_data_withdraw(yes=True)
        self.df_sz50_exposure,self.df_hs300_exposure,self.df_zz500_exposure,self.df_zz1000_exposure,self.df_zz2000_exposure,self.df_zzA500_exposure=dp.index_exposure_withdraw()
        self.df_factor=dp.stock_factor_exposure_withdraw()
        self.product_index=prod.product_index_withdraw()
        self.df_stock,self.df_bond,self.df_cbond,self.df_future,self.df_option,self.df_etf=prod.position_processing()
        self.df_stock=self.df_stock.drop_duplicates(subset=['code'])
        df_info=prod.product_info_withdraw()
        self.stock_money = df_info['股票市值'].tolist()[0]
        if np.isnan(self.stock_money):
            self.stock_money=0
        self.asset_value = df_info['资产净值'].tolist()[0]
        self.net_value=df_info['产品累计净值'].tolist()[0]
        self.shuhui=df_info['赎回金额'].tolist()[0]
        self.shengou=df_info['申购金额'].tolist()[0]
        self.asset_value_yes,self.net_value_yes=prod.asset_yes_withdraw()
        if np.isnan(self.shuhui)==False:
            self.asset_value_yes=self.asset_value_yes-self.shuhui
        if np.isnan(self.shengou)==False:
            self.asset_value_yes=self.asset_value_yes+self.shengou
        self.df_paper=prod.paper_portfolio_withdraw()
        self.df_stock_close=dp.crossSection_stock_close_withdraw(yes=False)
        self.df_stock_close_yes=dp.crossSection_stock_close_withdraw(yes=True)
        self.df_stock_adj=dp.crossSection_stock_adj_withdraw(yes=False)
        self.df_stock_adj_yes=dp.crossSection_stock_adj_withdraw(yes=True)
        self.index_return=dp.crossSection_index_return_withdraw(self.product_index,yes=False)
        self.df_cb=dp.convertible_bond_withdraw(yes=False)
        self.df_cb_yes = dp.convertible_bond_withdraw(yes=True)
        self.df_return=dp.timeseries_stockreturn_withdraw()
    #辅助运算function
    def future_option_mapping(self,x):
        if str(x)[:2] == 'HO':
            return 'IH' + str(x)[2:]
        elif str(x)[:2] == 'IO':
            return 'IF' + str(x)[2:]
        elif str(x)[:2] == 'MO':
            return 'IM' + str(x)[2:]
        else:
            print('qnmd')
            raise ValueError
    def index_finding(self,x):
        if str(x)[:2]=='IH':
            return '上证50'
        elif str(x)[:2]=='IF':
            return '沪深300'
        elif str(x)[:2]=='IC':
            return '中证500'
        elif str(x)[:2]=='IM':
            return '中证1000'
        else:
            return None
    def option_direction(self,x):
        if 'short' in x:
            return -1
        else:
            return 1
    def portfolio_index_exposure_withdraw(self):
        index=self.product_index
        if index == '上证50':
            df_index = self.df_sz50_exposure
        elif index == '沪深300':
            df_index = self.df_hs300_exposure
        elif index == '中证500':
            df_index = self.df_zz500_exposure
        elif index == '中证1000':
            df_index = self.df_zz1000_exposure
        else:
            df_index = self.df_zzA500_exposure
        df_index=df_index.astype(float)
        df_index=df_index.T
        df_index.reset_index(inplace=True)
        df_index.rename(columns={'index': 'factor_name'}, inplace=True)
        df_index.columns = ['factor_name', 'index_exposure']
        return df_index
    def duplicate_holding_processing(self,df):
        a=df.groupby('index_code')['mkt_value'].sum()
        a=pd.DataFrame(a)
        a.reset_index(inplace=True)
        return a
    #etf分析部分
    def etf_analysis(self):
        df_etf = self.df_etf.copy()
        if len(df_etf) > 0:
            df_etf['profit'] = (df_etf['price'] - df_etf['yes_price'])*df_etf['quantity']
            df_etf['mktvalue_yes'] = df_etf['quantity'] * df_etf['yes_price']
        else:
            df_etf['mktvalue_yes']=None
        return df_etf
    #国债分析部分:
    def bond_analysis(self):
        df_bond=self.df_bond.copy()
        if len(df_bond)>0:
            df_bond['profit'] = (df_bond['市价'] - df_bond['昨日市价']) * 10000 * df_bond['数量']
            df_bond['昨日市值'] = df_bond['数量'] * df_bond['昨日市价']*10000
        else:
            df_bond=df_bond
        return df_bond

    #可转债分析部分:
    def convertible_bond_analysis(self):
        df_holding=self.df_cbond.copy()
        df_cb=self.df_cb.copy()
        df_cb_yes=self.df_cb_yes.copy()
        df_close=self.df_stock_close.copy()
        df_close_yes=self.df_stock_close_yes.copy()
        df_cb_yes=df_cb_yes[['code','price','convert_price','duration']]
        df_cb_yes.rename(columns={'price':'price_yes','duration':'duration_yes','convert_price':'convert_price_yes'},inplace=True)
        df_close.rename(columns={'close': 'index_price','code':'index_code'}, inplace=True)
        df_close_yes.rename(columns={'close': 'index_price_yes','code':'index_code'}, inplace=True)
        df_holding=df_holding.merge(df_cb,on='code',how='left')
        df_holding=df_holding.merge(df_cb_yes,on='code',how='left')
        df_holding = df_holding.merge(df_close, on='index_code', how='left')
        df_holding = df_holding.merge(df_close_yes, on='index_code', how='left')
        df_holding['profit']=(df_holding['price']-df_holding['price_yes'])*df_holding['amount']
        return df_holding

    def black_scholes_delta(self,S, K, T, r, sigma):
        # S: 正股价格
        # K: 转股价格·
        # T: 到期时间（以年为单位）
        # r: 无风险利率
        # sigma: 标的股票的波动率
        d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        delta = stats.norm.cdf(d1)
        return delta
    def CB_delta_calculate(self):
        df_holding=self.convertible_bond_analysis()
        df_holding.sort_values(by='index_code',ascending=True,inplace=True)
        code_list=df_holding['index_code'].tolist()
        df_std=gt.stock_volatility_calculate(self.df_return,self.available_date)
        df_std=df_std[code_list]
        df_std.reset_index(inplace=True)
        yes=gt.last_workday_calculate(self.available_date)
        df1=df_std[df_std['valuation_date']==self.available_date]
        df2=df_std[df_std['valuation_date']==yes]
        df1.drop('valuation_date',axis=1,inplace=True)
        df2.drop('valuation_date', axis=1, inplace=True)
        std_list=df1.iloc[0].tolist()
        std_list_yes = df2.iloc[0].tolist()
        df_holding['std']=std_list
        df_holding['std_yes']=std_list_yes
        delta_list=[]
        delta_list_yes=[]
        for i in range(len(df_holding)):
            stock_price=df_holding['index_price'].tolist()[i]
            stock_price_yes = df_holding['index_price_yes'].tolist()[i]
            cb_price = df_holding['convert_price'].tolist()[i]
            cb_price_yes = df_holding['convert_price_yes'].tolist()[i]
            T = df_holding['duration'].tolist()[i]
            T_yes= df_holding['duration_yes'].tolist()[i]
            std = df_holding['std'].tolist()[i]*np.sqrt(252)
            std_yes = df_holding['std_yes'].tolist()[i]*np.sqrt(252)
            delta_today=self.black_scholes_delta(stock_price,cb_price, T, r=0.02, sigma=std)
            delta_yes=self.black_scholes_delta(stock_price_yes,cb_price_yes, T_yes, r=0.02, sigma=std_yes)
            delta_list.append(delta_today)
            delta_list_yes.append(delta_yes)
        df_holding['delta']=delta_list
        df_holding['delta_yes']=delta_list_yes
        return df_holding
    def CB_main(self):
        df_holding=self.CB_delta_calculate()
        profit=df_holding['profit'].sum()
        df_holding['mkt_value1'] = df_holding['amount'] * df_holding['price']
        df_holding['mkt_value']=df_holding['amount']*df_holding['price']*df_holding['delta']
        df_holding['mkt_value_yes'] = df_holding['amount'] * df_holding['price_yes'] * df_holding['delta_yes']
        mkt_value=df_holding['mkt_value'].sum()
        mkt_value_yes=df_holding['mkt_value_yes'].sum()
        return df_holding,profit,mkt_value,mkt_value_yes
    #股票分析部分:
    def stock_analysis(self):
        df_holding=self.df_stock.copy()
        df_close=self.df_stock_close.copy()
        df_close2=self.df_stock_close_yes.copy()
        df_adj=self.df_stock_adj.copy()
        df_adj_yes=self.df_stock_adj_yes.copy()
        df_adj_yes.rename(columns={'adjfactor':'adjfactor_yes'},inplace=True)
        df_close2.rename(columns={'close':'close_yes'},inplace=True)
        df_holding=df_holding.merge(df_close,on='code',how='left')
        df_holding=df_holding.merge(df_close2,on='code',how='left')
        df_holding=df_holding.merge(df_adj,on='code',how='left')
        df_holding=df_holding.merge(df_adj_yes,on='code',how='left')
        df_holding['涨跌']=(df_holding['close']*df_holding['adjfactor']-df_holding['close_yes']*df_holding['adjfactor_yes'])/df_holding['adjfactor_yes']
        df_holding['profit']=df_holding['amount']*df_holding['涨跌']
        df_holding['mkt_value_yes']=df_holding['amount']*df_holding['close_yes']
        profit=df_holding['profit'].sum()
        stock_money=df_holding['mkt_value_yes'].sum()
        return profit,stock_money
    def stock_processing(self):
        df_holding=self.df_stock.copy()
        df_close2=self.df_stock_close_yes.copy()
        df_close2.rename(columns={'close':'close_yes'},inplace=True)
        df_holding=df_holding.merge(df_close2,on='code',how='left')
        df_holding['mkt_value_yes']=df_holding['amount']*df_holding['close_yes']
        df_holding['weight']=df_holding['mkt_value_yes']/df_holding['mkt_value_yes'].sum()
        return df_holding
    #期权期货组合部分
    #计算期权部分
    def ratio_define(self,x):
        x2=str(x)[:2]
        if x2=='IF':
            return 300
        else:
            return 200
    def option_analysis(self,yes):
        position_df=self.df_option
        if len(position_df)>0:
            if yes == False:
                realtime_data_future = self.realtime_data_future
            else:
                realtime_data_future = self.realtime_data_future_yes
            dc = delta_calculator(self.available_date, position_df, realtime_data_future)
            merged_df = dc.delta_calculator_main()
            merged_df['ratio'] = merged_df['future_code'].apply(lambda x: self.ratio_define(x))
            merged_df['market_value'] = merged_df['future_price'] * merged_df['Delta'] * merged_df['数量'] * merged_df[
                'ratio'] / (merged_df['ratio'] / 100)
            merged_df['direction'] = merged_df['方向'].apply(lambda x: self.option_direction(x))
            merged_df['market_value'] = merged_df['market_value'] * merged_df['direction']
            merged_df['proportion'] = abs(
                merged_df['Delta'] * merged_df['数量'] * merged_df['direction'] / (merged_df['ratio'] / 100))
            merged_df['daily_profit'] = (merged_df['市价'] - merged_df['yes_price']) * merged_df['数量'] * merged_df[
                'direction'] * 100
            merged_df = merged_df[['科目名称', '方向', '数量', 'Delta', 'market_value', 'daily_profit', 'proportion']]
            merged_df.rename(columns={'科目名称': '合约'}, inplace=True)
        else:
            merged_df=pd.DataFrame(columns=['科目名称', '方向', '数量', 'Delta', 'market_value', 'daily_profit', 'proportion'])
        return merged_df
    #计算期货部分
    def future_analysis(self,yes):
        merged_df = self.df_future.copy()
        merged_df['Delta'] = 1
        merged_df['direction'] = merged_df['方向'].apply(lambda x: self.option_direction(x))
        merged_df['proportion'] = 1
        merged_df['daily_profit'] = (merged_df['市价'] - merged_df['yes_price']) * merged_df['ratio'] * \
                                    merged_df['direction'] * merged_df['数量']
        if yes==False:
            merged_df['market_value'] = merged_df['市值']
        else:
            merged_df['market_value'] = merged_df['yes_price'] * merged_df['ratio'] * merged_df['direction'] * merged_df[
                '数量']
        merged_df = merged_df[['科目名称', '方向', '数量', 'Delta', 'market_value', 'daily_profit','proportion']]
        merged_df.rename(columns={'科目名称':'合约'},inplace=True)
        return merged_df
    def FOC_main(self,cb_mkt_value,cb_profit):
        if cb_mkt_value==None or cb_profit==None:
            cb_mkt_value=0
            cb_profit=0
        df_info=pd.DataFrame()
        df1= self.option_analysis(yes=False)
        df2= self.future_analysis(yes=False)
        df3=self.bond_analysis()
        df4=self.etf_analysis()
        if len(df3)!=0:
           b_profit = df3['profit'].sum()
           b_mkt=df3['市值'].sum()
        else:
            b_profit=0
            b_mkt=0
        if len(df4)!=0:
           etf_profit = df4['profit'].sum()
           etf_mkt=df4['mktvalue'].sum()
        else:
            etf_profit=0
            etf_mkt=0
        df_final = pd.concat([df1, df2])
        mkt_value_option = df1['market_value'].sum()
        profit_option = df1['daily_profit'].sum()
        mkt_value_future = df2['market_value'].sum()
        profit_future = df2['daily_profit'].sum()
        mkt_value_total = df_final['market_value'].sum()+cb_mkt_value+etf_mkt
        profit_total = df_final['daily_profit'].sum()+cb_profit+b_profit+etf_profit
        df_info['info_name']=['衍生品总市值','衍生品总盈亏','期权市值','期货市值','可转债市值','国债市值','etf市值','期权盈亏','期货盈亏','可转债盈亏','国债盈亏','etf盈亏']
        df_info['money']=[mkt_value_total,profit_total,mkt_value_option,mkt_value_future,cb_mkt_value,b_mkt,etf_mkt,profit_option,profit_future,cb_profit,b_profit,etf_profit]
        return df_info,df_final

    #产品指标处理部分
    def portfolio_info_processing(self,df_info):
        df_porinfo=pd.DataFrame()
        mkt_value_total=df_info[df_info['info_name']=='衍生品总市值']['money'].tolist()[0]
        mkt_value_option=df_info[df_info['info_name']=='期权市值']['money'].tolist()[0]
        mkt_value_future = df_info[df_info['info_name'] == '期货市值']['money'].tolist()[0]
        mkt_value_cb= df_info[df_info['info_name'] == '可转债市值']['money'].tolist()[0]
        mkt_value_b=df_info[df_info['info_name'] == '国债市值']['money'].tolist()[0]
        mkt_value_etf=df_info[df_info['info_name'] == 'etf市值']['money'].tolist()[0]
        leverage_ratio = round((self.stock_money + mkt_value_total) / self.asset_value,4)
        ratio_stock = round(self.stock_money/ self.asset_value,4)
        ratio_option=round(mkt_value_option/self.asset_value,4)
        ratio_future=round(mkt_value_future/self.asset_value,4)
        ratio_cb = round(mkt_value_cb / self.asset_value, 4)
        ratio_etf=round(mkt_value_etf / self.asset_value, 4)
        df_porinfo['info_name']=['杠杆率','股票占比','期货占比','期权占比','可转债占比','etf占比','股票市值','产品累计净值','资产净值']
        df_porinfo['money']=[leverage_ratio,ratio_stock,ratio_future,ratio_option,ratio_cb,ratio_etf,self.stock_money,self.net_value,self.asset_value]
        return df_porinfo
    #仓位对比
    def weight_difference_calculate(self):
        df_paper=self.df_paper
        df_stock=self.stock_processing()
        if len(df_paper)>0:
            df_paper.rename(columns={'weight': 'paper_weight'}, inplace=True)
            df_stock.rename(columns={'weight': 'stock_weight'}, inplace=True)
            df_paper['code'] = df_paper['code'].apply(lambda x: x[:6])
            df_stock['code'] = df_stock['code'].apply(lambda x: x[:6])
            df_paper = df_paper.merge(df_stock, on='code', how='outer')
            df_paper.fillna(0, inplace=True)
            df_paper['difference'] = abs(df_paper['paper_weight'] - df_paper['stock_weight'])
            difference_sum = df_paper['difference'].sum()
            df_paper.sort_values(by='difference', ascending=False, inplace=True)
            df_paper.reset_index(inplace=True, drop=True)
            df_paper = df_paper[df_paper['difference'] > 0.0005]
            df_paper = df_paper[['code', 'paper_weight', 'stock_weight', 'difference']]
            df_paper = gt.code_transfer(df_paper)
            df_info = pd.DataFrame(columns=['code', 'paper_weight', 'stock_weight', 'difference'])
            df_info['code'] = ['仓位差异']
            df_info['paper_weight'] = [difference_sum]
            df_info = pd.concat([df_info, df_paper])
        else:
            df_info=pd.DataFrame()
        return df_info
    #风险因子暴露计算部分
    def stock_exposure_calculate(self):
        df_portfolio=self.df_stock
        df_portfolio.sort_values(by='code',inplace=True,ascending=True)
        proportion=self.stock_money/self.asset_value
        selecting_code_list = df_portfolio['code'].tolist()
        df_factor_exposure = self.df_factor[self.df_factor['code'].isin(selecting_code_list)]
        df_factor_exposure.fillna(0, inplace=True)
        code_list=df_factor_exposure['code'].tolist()
        df_factor_exposure.drop(columns='code', inplace=True)
        df_portfolio=df_portfolio[df_portfolio['code'].isin(code_list)]
        weight = df_portfolio['weight'].astype(float).tolist()
        index_factor_exposure = list(
            np.array(np.dot(np.mat(df_factor_exposure.values).T, np.mat(weight).T)).flatten())
        index_factor_exposure = [index_factor_exposure]
        index_factor_exposure=np.multiply(np.array(index_factor_exposure),proportion)
        df_final = pd.DataFrame(index_factor_exposure, columns=df_factor_exposure.columns.tolist())
        df_final=df_final.T
        df_final.reset_index(inplace=True)
        df_final.rename(columns={'index':'factor_name'},inplace=True)
        df_final.columns=['factor_name','stock_exposure']
        return df_final
    def cb_exposure_calculate(self,df_holding):
        if len(df_holding)>0:
            df_portfolio = df_holding.copy()
            df_portfolio = df_portfolio[['index_code', 'mkt_value']]
            df_portfolio=self.duplicate_holding_processing(df_portfolio)
            df_portfolio['weight'] = df_portfolio['mkt_value'] / df_portfolio['mkt_value'].sum()
            mkt_value = df_portfolio['mkt_value'].sum()
            proportion = mkt_value / self.asset_value
            selecting_code_list = df_portfolio['index_code'].tolist()
            df_factor_exposure = self.df_factor[self.df_factor['code'].isin(selecting_code_list)]
            df_factor_exposure.fillna(0, inplace=True)
            valid_code = df_factor_exposure['code'].tolist()
            df_factor_exposure.drop(columns='code', inplace=True)
            df_portfolio=df_portfolio[df_portfolio['index_code'].isin(valid_code)]
            weight = df_portfolio['weight'].astype(float).tolist()
            index_factor_exposure = list(
                np.array(np.dot(np.mat(df_factor_exposure.values).T, np.mat(weight).T)).flatten())
            index_factor_exposure = [index_factor_exposure]
            index_factor_exposure = np.multiply(np.array(index_factor_exposure), proportion)
            df_final = pd.DataFrame(index_factor_exposure, columns=df_factor_exposure.columns.tolist())
            df_final = df_final.T
            df_final.reset_index(inplace=True)
            df_final.rename(columns={'index': 'factor_name'}, inplace=True)
            df_final.columns = ['factor_name', 'cb_exposure']
        else:
            df_final=pd.DataFrame(columns=['factor_name', 'cb_exposure'])
            factor_name=self.df_factor.columns.tolist()[-1]
            df_final['factor_name']=factor_name
            df_final['cb_exposure']=0
        return df_final

    def index_exposure_sum(self,df):
        heyue_list = df['合约'].tolist()
        exposure_final = []
        for heyue in heyue_list:
            proportion = df[df['合约'] == heyue]['proportion'].tolist()[0]
            weight = df[df['合约'] == heyue]['weight'].tolist()[0]
            index = df[df['合约'] == heyue]['index_type'].tolist()[0]
            if index == '上证50':
                df_index = self.df_sz50_exposure
            elif index == '沪深300':
                df_index = self.df_hs300_exposure
            elif index == '中证500':
                df_index = self.df_zz500_exposure
            elif index == '中证1000':
                df_index = self.df_zz1000_exposure
            else:
                df_index = self.df_zzA500_exposure
            df_index.fillna(0, inplace=True)
            df_index = df_index.astype(float)
            slice_exposure = np.multiply(np.array(df_index.values), (proportion * weight))
            exposure_final.append(slice_exposure.tolist()[0])
        df_final = pd.DataFrame(exposure_final, columns=self.df_sz50_exposure.columns.tolist())
        df_final['合约'] = heyue_list
        df_final = df_final[['合约'] + self.df_sz50_exposure.columns.tolist()]
        return df_final
    def option_future_exposure_calculate(self,df_detail):
        df_detail['len'] = df_detail['合约'].apply(lambda x: len(x))
        df_future = df_detail[df_detail['len'] <= 6]
        df_option = df_detail[(df_detail['len'] > 6)]
        df_option['index'] = df_option['合约'].apply(lambda x: self.future_option_mapping(x))
        df_option['index_type'] = df_option['index'].apply(lambda x: self.index_finding(x))
        df_future['index_type'] = df_future['合约'].apply(lambda x: self.index_finding(x))
        df_future.dropna(inplace=True)
        df_option.dropna(inplace=True)
        df_option['weight'] = df_option['market_value'] / self.asset_value
        df_future['weight'] = df_future['market_value'] / self.asset_value
        option_exposure = self.index_exposure_sum(df_option)
        future_exposure = self.index_exposure_sum(df_future)
        option_future_exposure = pd.concat([option_exposure, future_exposure])
        option_exposure.set_index('合约', inplace=True, drop=True)
        future_exposure.set_index('合约', inplace=True, drop=True)
        option_exposure = option_exposure.apply(lambda x: x.sum(), axis=0)
        future_exposure = future_exposure.apply(lambda x: x.sum(), axis=0)
        dict_option_exposure = {'factor_name': option_exposure.index, 'option_exposure': option_exposure.values}
        dict_future_exposure = {'factor_name': future_exposure.index, 'future_exposure': future_exposure.values}
        option_final = pd.DataFrame(dict_option_exposure)
        future_final= pd.DataFrame(dict_future_exposure)
        exposure=option_final.merge(future_final,on='factor_name',how='left')
        return option_future_exposure, exposure
    def final_portfolio_exposure_processing(self,df_stock_factor,future_option_exposure,index_exposure,df_cb_exposure):
        df_portfolio_exposure = df_stock_factor.merge(future_option_exposure, on='factor_name', how='left')
        df_portfolio_exposure = df_portfolio_exposure.merge(df_cb_exposure, on='factor_name', how='left')
        df_portfolio_exposure.set_index('factor_name', inplace=True)
        df_portfolio_exposure['portfolio_exposure'] = df_portfolio_exposure.apply(lambda x: x.sum(), axis=1)
        df_portfolio_exposure.reset_index(inplace=True)
        df_portfolio_exposure=df_portfolio_exposure.merge(index_exposure,on='factor_name', how='left')
        df_portfolio_exposure['difference']=df_portfolio_exposure['portfolio_exposure']-df_portfolio_exposure['index_exposure']
        df_portfolio_exposure['ratio']=df_portfolio_exposure['difference']/abs(df_portfolio_exposure['index_exposure'])
        return df_portfolio_exposure
    #收益拆解部分
    def product_return_split_zhizeng(self,df_info,stock_profit,stock_money,df_porinfo,cb_profit,cb_mkt_value_yes):
        if cb_profit==None or cb_mkt_value_yes==None or cb_mkt_value_yes==0:
            cb_profit=0
            cb_mkt_value_yes=0
            cb_return=0
            cb_proportion=0
        else:
            cb_return = cb_profit / cb_mkt_value_yes
            cb_proportion = cb_mkt_value_yes / self.asset_value_yes
        if stock_money==0:
            stock_return=None
        else:
            stock_return=stock_profit/stock_money
        df1 = self.option_analysis(yes=True)
        df2 = self.future_analysis(yes=True)
        df3=self.bond_analysis()
        df4 = self.etf_analysis()
        option_yes=df1['market_value'].sum()
        future_yes = df2['market_value'].sum()
        etf_yes = df4['mktvalue_yes'].sum()
        product_profit=self.asset_value-self.asset_value_yes
        product_return=product_profit/self.asset_value_yes
        product_return2=(self.net_value-self.net_value_yes)/self.net_value_yes
        option_pro=df_info[df_info['info_name']=='期权盈亏']['money'].tolist()[0]
        future_pro=df_info[df_info['info_name']=='期货盈亏']['money'].tolist()[0]
        bond_pro=df_info[df_info['info_name']=='国债盈亏']['money'].tolist()[0]
        etf_pro = df_info[df_info['info_name'] == 'etf盈亏']['money'].tolist()[0]
        if len(df3) == 0:
            bond_return = 0
        else:
            bond_mkt_yes=df3['昨日市值'].sum()
            bond_return=bond_pro/bond_mkt_yes
        if len(df4)==0:
            etf_return=0
        else:
            etf_mkt_yes = df4['mktvalue_yes'].sum()
            etf_return = etf_pro / etf_mkt_yes
        try:
           option_return=option_pro/option_yes
        except:
            option_return=0
        try:
            future_return = future_pro / future_yes
        except:
            future_return=0
        option_proportion=option_yes/self.asset_value_yes
        future_proportion=future_yes/self.asset_value_yes
        stock_proportion=stock_money/self.asset_value_yes
        etf_proportion=etf_yes/self.asset_value_yes
        sum_p=option_yes+future_yes+cb_mkt_value_yes+stock_money+etf_yes
        #sum_p2=option_proportion+future_proportion+stock_proportion+cb_proportion+etf_proportion
        df_pro=pd.DataFrame()
        df_pro['product_split']=['产品','股票','期权','期货','转债','国债','ETF']
        error_pro=product_profit-stock_profit-option_pro-future_pro-cb_profit-bond_pro-etf_pro
        error_return=error_pro/self.asset_value_yes
        df_pro['盈亏']=[product_profit,stock_profit,option_pro,future_pro,cb_profit,bond_pro,etf_pro]
        df_pro['仓位占比(昨日)'] = [1, stock_proportion, option_proportion, future_proportion, cb_proportion, 0,
                                    etf_proportion]
        df_pro['收益率(本身)bp']=[product_return2,stock_return,option_return,future_return,cb_return,bond_return,etf_return]
        df_pro.loc[df_pro['仓位占比(昨日)'] <0, ['收益率(本身)bp']] = df_pro[df_pro['仓位占比(昨日)'] <0]['收益率(本身)bp']*-1
        df_pro['超额(本身)bp']=df_pro['收益率(本身)bp']-float(self.index_return)
        df_pro.loc[df_pro['product_split'] == '国债', ['超额(本身)bp']]=df_pro[df_pro['product_split'] == '国债']['收益率(本身)bp']
        df_pro['超额贡献bp'] = df_pro['超额(本身)bp'] * df_pro['仓位占比(昨日)']
        df_pro.loc[df_pro['仓位占比(昨日)'] < 0, ['超额贡献bp']] = df_pro[df_pro['仓位占比(昨日)'] < 0][
                                                                           '超额贡献bp'] * -1
        df_pro.loc[df_pro['product_split'] == '产品', ['超额贡献bp']] = df_pro[df_pro['product_split'] == '产品'][
            '超额(本身)bp']
        df_pro[['收益率(本身)bp','超额(本身)bp','超额贡献bp']]=round(df_pro[['收益率(本身)bp','超额(本身)bp','超额贡献bp']]*10000,4)
        sum_p2=df_porinfo[df_porinfo['info_name']=='杠杆率']['money'].tolist()[0]
        leverage_attribution=float(self.index_return) * (sum_p2-1) * 10000
        error_attribution=df_pro[df_pro['product_split'] == '产品']['超额贡献bp'].tolist()[0]-df_pro[df_pro['product_split'] != '产品']['超额贡献bp'].sum()-leverage_attribution
        df_pro.loc['add2'] = ['误差', error_pro, None, None, None,round(error_attribution, 4)]
        df_pro.loc['add']=['杠杆率',None,0,round(float(self.index_return)*10000,4),round(float(self.index_return)*10000,4),round(leverage_attribution,4)]
        df_pro.reset_index(inplace=True, drop=True)
        df_porinfo.reset_index(inplace=True, drop=True)
        df_info.reset_index(inplace=True,drop=True)
        df_pro=pd.concat([df_pro,df_porinfo],axis=1)
        return df_pro
    def product_return_split_zhongxing(self,df_info,stock_profit,stock_money,df_porinfo,cb_profit,cb_mkt_value_yes):
        if cb_profit==None or cb_mkt_value_yes==None or cb_mkt_value_yes==0:
            cb_profit=0
            cb_mkt_value_yes=0
            cb_return=0
            cb_proportion=0
        else:
            cb_return = cb_profit / cb_mkt_value_yes
            cb_proportion = cb_mkt_value_yes / self.asset_value_yes
        if stock_money==0:
            stock_return=None
        else:
            stock_return=stock_profit/stock_money
        df1 = self.option_analysis(yes=True)
        df2 = self.future_analysis(yes=True)
        df3=self.bond_analysis()
        df4 = self.etf_analysis()
        etf_yes = df4['mktvalue_yes'].sum()
        option_yes=df1['market_value'].sum()
        future_yes = df2['market_value'].sum()
        product_profit=self.asset_value-self.asset_value_yes
        product_return=product_profit/self.asset_value_yes
        product_return2 = (self.net_value - self.net_value_yes) / self.net_value_yes
        option_pro=df_info[df_info['info_name']=='期权盈亏']['money'].tolist()[0]
        future_pro=df_info[df_info['info_name']=='期货盈亏']['money'].tolist()[0]
        bond_pro = df_info[df_info['info_name'] == '国债盈亏']['money'].tolist()[0]
        etf_pro = df_info[df_info['info_name'] == 'etf盈亏']['money'].tolist()[0]
        if len(df3) == 0:
            bond_return = None
        else:
            bond_mkt_yes = df3['昨日市值'].sum()
            bond_return = bond_pro / bond_mkt_yes
        if len(df4)==0:
            etf_return=None
        else:
            etf_mkt_yes = df4['mktvalue_yes'].sum()
            etf_return = etf_pro / etf_mkt_yes
        if len(df1)==0:
            option_return=None
        else:
            option_return=option_pro/option_yes
        if len(df2)==0:
            future_return=None
        else:
            future_return=future_pro/future_yes
        option_proportion=option_yes/self.asset_value_yes
        future_proportion=future_yes/self.asset_value_yes
        stock_proportion=stock_money/self.asset_value_yes
        etf_proportion = etf_yes / self.asset_value_yes
        #sum_p2=option_proportion+future_proportion+stock_proportion+cb_proportion+etf_proportion
        df_pro=pd.DataFrame()
        df_pro['product_split']=['产品','股票','期权','期货','转债','国债','ETF']
        error_pro=product_profit-stock_profit-option_pro-future_pro-cb_profit-bond_pro-etf_pro
        error_return=error_pro/self.asset_value_yes
        df_pro['昨日金额']=[self.asset_value_yes,stock_money,option_yes,future_yes,cb_mkt_value_yes,None,etf_yes]
        df_pro['盈亏']=[product_profit,stock_profit,option_pro,future_pro,cb_profit,bond_pro,etf_pro]
        df_pro['收益率(本身)bp']=[product_return,stock_return,option_return,future_return,cb_return,bond_return,etf_return]
        df_pro['仓位占比(昨日)'] = [1, stock_proportion, option_proportion, future_proportion, cb_proportion, 0,
                                    etf_proportion]
        df_pro.loc[df_pro['仓位占比(昨日)'] < 0, ['收益率(本身)bp']] = df_pro[df_pro['仓位占比(昨日)'] < 0][
                                                                           '收益率(本身)bp'] * -1
        df_pro['超额(本身)bp']=df_pro['收益率(本身)bp']-float(self.index_return)
        df_pro.loc[df_pro['product_split'] == '国债', ['超额(本身)bp']] = df_pro[df_pro['product_split'] == '国债'][
            '收益率(本身)bp']
        df_pro.loc[df_pro['昨日金额']<0, ['超额(本身)bp']] =df_pro[df_pro['昨日金额']<0]['收益率(本身)bp']+float(self.index_return)
        df_pro['超额贡献bp']=df_pro['超额(本身)bp']*df_pro['仓位占比(昨日)']
        df_pro.loc[df_pro['仓位占比(昨日)'] < 0, ['超额贡献bp']] = df_pro[df_pro['仓位占比(昨日)'] < 0][
                                                                       '超额贡献bp'] * -1
        df_pro.loc[df_pro['product_split'] == '产品', ['超额(本身)bp']] = df_pro[df_pro['product_split'] == '产品'][
            '收益率(本身)bp'].tolist()
        df_pro.loc[df_pro['product_split'] == '产品', ['超额贡献bp']] = df_pro[df_pro['product_split'] == '产品'][
            '超额(本身)bp'].tolist()
        df_pro[['收益率(本身)bp','超额(本身)bp','超额贡献bp']]=round(df_pro[['收益率(本身)bp','超额(本身)bp','超额贡献bp']]*10000,4)
        sum_p2 = df_porinfo[df_porinfo['info_name'] == '杠杆率']['money'].tolist()[0]
        leverage_attribution=float(self.index_return) * (sum_p2) * 10000
        error_attribution = df_pro[df_pro['product_split'] == '产品']['超额贡献bp'].tolist()[0] - \
                            df_pro[df_pro['product_split'] != '产品']['超额贡献bp'].sum() - leverage_attribution
        df_pro.loc['add2'] = ['误差',None, error_pro, None, None, None,round(error_attribution, 4)]
        df_pro.loc['add']=['杠杆率',None,None,0,round(float(self.index_return)*10000,4),sum_p2,round(leverage_attribution,4)]
        df_pro.reset_index(inplace=True,drop=True)
        df_porinfo.reset_index(inplace=True,drop=True)
        df_pro=pd.concat([df_pro,df_porinfo],axis=1)
        return df_pro
    def product_return_split_zhongxing_2(self,df_info,stock_profit,stock_money,df_porinfo,cb_profit,cb_mkt_value_yes):
        if cb_profit==None or cb_mkt_value_yes==None or cb_mkt_value_yes==0:
            cb_profit=0
            cb_mkt_value_yes=0
            cb_return=0
            cb_proportion=0
        else:
            cb_return = cb_profit / cb_mkt_value_yes
            cb_proportion = cb_mkt_value_yes / self.asset_value_yes
        if stock_money == 0:
            stock_return = None
        else:
            stock_return = stock_profit / stock_money
        df1 = self.option_analysis(yes=True)
        df2 = self.future_analysis(yes=True)
        df3=self.bond_analysis()
        df4 = self.etf_analysis()
        etf_yes = df4['mktvalue_yes'].sum()
        option_yes=df1['market_value'].sum()
        future_yes = df2['market_value'].sum()
        product_profit=self.asset_value-self.asset_value_yes
        product_return=product_profit/self.asset_value_yes
        product_return2 = (self.net_value - self.net_value_yes) / self.net_value_yes
        option_pro=df_info[df_info['info_name']=='期权盈亏']['money'].tolist()[0]
        future_pro=df_info[df_info['info_name']=='期货盈亏']['money'].tolist()[0]
        bond_pro = df_info[df_info['info_name'] == '国债盈亏']['money'].tolist()[0]
        etf_pro = df_info[df_info['info_name'] == 'etf盈亏']['money'].tolist()[0]
        if len(df3) == 0:
            bond_return = 0
        else:
            bond_mkt_yes = df3['昨日市值'].sum()
            bond_return = bond_pro / bond_mkt_yes
        if len(df4)==0:
            etf_return=0
        else:
            etf_mkt_yes = df4['mktvalue_yes'].sum()
            etf_return = etf_pro / etf_mkt_yes
        option_return=option_pro/option_yes
        future_return=future_pro/future_yes
        option_proportion=option_yes/self.asset_value_yes
        future_proportion=future_yes/self.asset_value_yes
        stock_proportion=stock_money/self.asset_value_yes
        etf_proportion = etf_yes / self.asset_value_yes
        #sum_p2=option_proportion+future_proportion+stock_proportion+cb_proportion+etf_proportion
        df_pro=pd.DataFrame()
        df_pro['product_split']=['产品','股票','期权','期货','转债','国债','ETF']
        error_pro=product_profit-stock_profit-option_pro-future_pro-cb_profit-bond_pro-etf_pro
        error_return=error_pro/self.asset_value_yes
        df_pro['昨日金额']=[self.asset_value_yes,stock_money,option_yes,future_yes,cb_mkt_value_yes,None,etf_yes]
        df_pro['盈亏']=[product_profit,stock_profit,option_pro,future_pro,cb_profit,bond_pro,etf_pro]
        df_pro['收益率(本身)bp']=[product_return,stock_return,option_return,future_return,cb_return,bond_return,etf_return]
        df_pro['仓位占比(昨日)'] = [1, stock_proportion, option_proportion, future_proportion, cb_proportion, 0,
                                    etf_proportion]
        df_pro.loc[df_pro['仓位占比(昨日)'] < 0, ['收益率(本身)bp']] = df_pro[df_pro['仓位占比(昨日)'] < 0][
                                                                           '收益率(本身)bp'] * -1
        df_pro['超额(本身)bp']=df_pro['收益率(本身)bp']
        df_pro.loc[df_pro['product_split'] == '国债', ['超额(本身)bp']] = df_pro[df_pro['product_split'] == '国债'][
            '收益率(本身)bp']
        df_pro['超额贡献bp']=df_pro['超额(本身)bp']*df_pro['仓位占比(昨日)']
        df_pro.loc[df_pro['仓位占比(昨日)'] < 0, ['超额贡献bp']] = df_pro[df_pro['仓位占比(昨日)'] < 0][
                                                                       '超额贡献bp'] * -1
        df_pro.loc[df_pro['product_split'] == '产品', ['超额(本身)bp']] = df_pro[df_pro['product_split'] == '产品'][
            '收益率(本身)bp'].tolist()
        df_pro.loc[df_pro['product_split'] == '产品', ['超额贡献bp']] = df_pro[df_pro['product_split'] == '产品'][
            '超额(本身)bp'].tolist()
        df_pro[['收益率(本身)bp','超额(本身)bp','超额贡献bp']]=round(df_pro[['收益率(本身)bp','超额(本身)bp','超额贡献bp']]*10000,4)
        error_attribution = df_pro[df_pro['product_split'] == '产品']['超额贡献bp'].tolist()[0] - \
                            df_pro[df_pro['product_split'] != '产品']['超额贡献bp'].sum()
        df_pro.loc['add2'] = ['误差',None, error_pro, None, None, None,round(error_attribution, 4)]
        sum_p2 = df_porinfo[df_porinfo['info_name'] == '杠杆率']['money'].tolist()[0]
        df_pro.loc['add']=['杠杆率',None,None,0,0,sum_p2,0]
        df_pro.reset_index(inplace=True,drop=True)
        df_porinfo.reset_index(inplace=True,drop=True)
        df_pro=pd.concat([df_pro,df_porinfo],axis=1)
        return df_pro
    def portfolio_tracking_main(self):
        if len(self.df_cb)>0:
            try:
                 df_holding, cb_profit, cb_mkt_value, cb_mkt_value_yes=self.CB_main()
            except:
                df_holding = pd.DataFrame()
                cb_profit, cb_mkt_value, cb_mkt_value_yes = None, None, None
        else:
            df_holding=pd.DataFrame()
            cb_profit, cb_mkt_value, cb_mkt_value_yes=None,None,None
        stock_profit,stock_money=self.stock_analysis()
        index_exposure=self.portfolio_index_exposure_withdraw()
        df_info, df_final= self.FOC_main(cb_mkt_value,cb_profit)
        df_porinfo = self.portfolio_info_processing(df_info)
        product=product_type(self.product_name)
        if product=='指增':
            df_porinfo=self.product_return_split_zhizeng(df_info, stock_profit, stock_money, df_porinfo,cb_profit,cb_mkt_value_yes)
        elif product=='绝对中性':
            df_porinfo=self.product_return_split_zhongxing_2(df_info, stock_profit, stock_money, df_porinfo,cb_profit,cb_mkt_value_yes)
        else:
            df_porinfo=self.product_return_split_zhongxing(df_info, stock_profit, stock_money, df_porinfo,cb_profit,cb_mkt_value_yes)
            df_porinfo.drop('昨日金额', inplace=True, axis=1)
        df_stock_factor=self.stock_exposure_calculate()
        option_future_exposure, exposure_final=self.option_future_exposure_calculate(df_final)
        df_cb_exposure=self.cb_exposure_calculate(df_holding)
        df_portfolio_exposure = self.final_portfolio_exposure_processing(df_stock_factor, exposure_final,
                                                                         index_exposure,df_cb_exposure)
        df_diff=self.weight_difference_calculate()
        return df_info,df_final,df_porinfo,option_future_exposure,df_portfolio_exposure,df_diff
    def saving_main(self):
        df_info, df_final, df_porinfo, option_future_exposure, df_portfolio_exposure,df_diff=self.portfolio_tracking_main()
        outputpath = glv.get('output')
        outputpath=os.path.join(outputpath,self.product_name)
        gt.folder_creator2(outputpath)
        available_date2=gt.intdate_transfer(self.available_date)
        outputpath = os.path.join(outputpath, str(self.product_name)+'_'+str(available_date2)+'.xlsx')
        with pd.ExcelWriter(outputpath, engine='openpyxl') as writer:
            df_porinfo.to_excel(writer, sheet_name='产品表现汇总', index=False)
            df_info.to_excel(writer, sheet_name='期权期货信息', index=False)
            df_diff.to_excel(writer,sheet_name='实盘仓位差异',index=False)
            df_portfolio_exposure.to_excel(writer,sheet_name='产品风险因子暴露', index=False)
            df_final.to_excel(writer, sheet_name='期货期权数据', index=False)
            option_future_exposure.to_excel(writer,sheet_name='期权期货因子暴露',index=False)


