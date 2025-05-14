import datetime
import os
import pandas as pd
import yaml
import sys
import datetime as datetime
import Portfolio_tracking.global_setting.global_dic as glv
import global_tools_func.global_tools as gt
import numpy as np
from Portfolio_tracking.futures_options_position_tracking.product_tracking_dp import data_prepared,product_data
class portfolio_tracking:
    def __init__(self):
        dp=data_prepared()
        self.realtime_data_df, self.realtime_data_future,self.realtime_data_index,self.realtime_data_etf=dp.realtime_data_withdraw()
        self.df_sz50_exposure,self.df_hs300_exposure,self.df_zz500_exposure,self.df_zz1000_exposure,self.df_zz2000_exposure,self.df_zzA500_exposure=dp.index_exposure_withdraw()
        self.df_factor=dp.stock_factor_exposure_withdraw()
    #辅助运算function
    def future_option_mapping(self,x):
        if str(x)[:2] == 'HO':
            return 'IH' + str(x)[2:]
        elif str(x)[:2] == 'IO':
            return 'IF' + str(x)[2:]
        elif str(x)[:2] == 'MO':
            return 'IM' + str(x)[2:]
        else:
            print(x,'没有找到匹配规则')
            return None
            # print('qnmd')
            # raise ValueError
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
            print(x,'没有找到匹配规则')
            return None
    def option_direction(self,x):
        if '卖' in x:
            return -1
        else:
            return 1
    def product_index_withdraw(self,product_name):
        inputpath_product = glv.get('product_detail')
        df_proindex = pd.read_excel(inputpath_product,sheet_name='product_detail')
        if product_name=='惠盈一号':
            product_name2='宣夜惠盈1号'
        elif product_name=='盛元8号':
            product_name2='盛丰500指增8号'
        else:
            raise ValueError
        index_type = df_proindex[df_proindex['product_name'] == product_name2]['index_type'].tolist()[0]
        return index_type
    def portfolio_index_exposure_withdraw(self,product_name):
        index=self.product_index_withdraw(product_name)
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

    def option_analysis(self,position_df, realtime_data_df, realtime_data_future):
        realtime_data_df['代码'] = realtime_data_df['代码'].str.split('.').str[0]
        merged_df = pd.merge(position_df, realtime_data_df, left_on='合约', right_on='代码')
        merged_df['option_code'] = merged_df['合约'].apply(lambda x: str(x)[:6])
        merged_df['future'] = merged_df['option_code'].apply(lambda x: self.future_option_mapping(x))
        realtime_data_future2=realtime_data_future.copy()
        realtime_data_future2.drop(columns='前结算价',inplace=True)
        merged_df = merged_df.merge(realtime_data_future2, on='future', how='left')
        merged_df = merged_df[
            ['合约', '买卖', '总持仓', '前结算价', '现价', 'Delta', '中价隐含波动率', 'future', 'future_price',
             'ratio']]
        merged_df['market_value'] = merged_df['future_price'] * merged_df['Delta'] * merged_df['总持仓'] * merged_df[
            'ratio'] / (merged_df['ratio'] / 100)
        merged_df['direction'] = merged_df['买卖'].apply(lambda x: self.option_direction(x))
        merged_df['market_value'] = merged_df['market_value'] * merged_df['direction']
        merged_df['proportion'] = abs(merged_df['Delta'] * merged_df['总持仓']* merged_df['direction'] / (merged_df['ratio'] / 100))
        merged_df['daily_profit'] = (merged_df['现价'] - merged_df['前结算价']) * merged_df['总持仓'] * merged_df[
            'direction'] * 100
        merged_df = merged_df[['合约', '买卖', '总持仓', 'Delta', 'market_value', 'daily_profit','proportion']]
        return merged_df
    def future_analysis(self,position_df, realtime_data_future):
        position_df2=position_df.copy()
        position_df2['new_code']=position_df['合约'].apply(lambda x: str(x)[:1])
        position_df3=position_df2[~(position_df2['new_code']=='T')]
        position_df4=position_df2[(position_df2['new_code']=='T')]
        merged_df = pd.merge(position_df3, realtime_data_future, left_on='合约', right_on='future')
        merged_df['Delta'] = 1
        merged_df['direction'] = merged_df['买卖'].apply(lambda x: self.option_direction(x))
        merged_df['market_value'] = merged_df['future_price'] * merged_df['ratio'] * merged_df['direction'] * merged_df[
            '总持仓']
        merged_df['proportion']=1
        merged_df['daily_profit'] = (merged_df['future_price'] - merged_df['前结算价']) * merged_df['ratio'] * \
                                    merged_df['direction'] * merged_df['总持仓']
        merged_df = merged_df[['合约', '买卖', '总持仓', 'Delta', 'market_value', 'daily_profit','proportion']]
        merged_df2 = pd.merge(position_df4, realtime_data_future, left_on='合约', right_on='future')
        merged_df2['direction'] = merged_df2['买卖'].apply(lambda x: self.option_direction(x))
        merged_df2['daily_profit'] = (merged_df2['future_price'] - merged_df2['前结算价']) * merged_df2['ratio'] * \
                                    merged_df2['direction'] * merged_df2['总持仓']
        bond_profit=merged_df2['daily_profit'].sum()
        return merged_df,bond_profit
    def FO_main(self,df_holding):
        df_info=pd.DataFrame()
        df1 = self.option_analysis(df_holding, self.realtime_data_df, self.realtime_data_future)
        df2,bond_profit = self.future_analysis(df_holding, self.realtime_data_future)
        df_final = pd.concat([df1, df2])
        mkt_value_option = df1['market_value'].sum()
        profit_option = df1['daily_profit'].sum()
        mkt_value_future = df2['market_value'].sum()
        profit_future = df2['daily_profit'].sum()
        mkt_value_total = df_final['market_value'].sum()
        profit_total = df_final['daily_profit'].sum()
        profit_total=profit_total+bond_profit
        df_info['info_name']=['期权期货总市值','期权期货国债总盈亏','期权市值','期货市值','期权盈亏','期货盈亏','国债盈亏']
        df_info['money']=[mkt_value_total,profit_total,mkt_value_option,mkt_value_future,profit_option,profit_future,bond_profit]
        return df_info,df_final
    def portfolio_info_processing(self,stock_money,asset_value,etf_money,df_info):
        df_porinfo=pd.DataFrame()
        mkt_value_total=df_info[df_info['info_name']=='期权期货总市值']['money'].tolist()[0]
        mkt_value_option=df_info[df_info['info_name']=='期权市值']['money'].tolist()[0]
        mkt_value_future = df_info[df_info['info_name'] == '期货市值']['money'].tolist()[0]
        leverage_ratio = round((stock_money + mkt_value_total) / asset_value,4)
        ratio_stock = round(stock_money/ asset_value,4)
        ratio_option=round(mkt_value_option/asset_value,4)
        ratio_etf=round(etf_money/asset_value,4)
        # ratio_option=round(asset_value/mkt_value_option,2)
        ratio_future=round(mkt_value_future/asset_value,4)
        df_porinfo['info_name']=['杠杆率','股票占比','期货占比','期权占比','ETF占比','股票市值','资产净值']
        # df_porinfo['money']=[leverage_ratio,ratio_stock,ratio_option,ratio_future,stock_money,asset_value]
        df_porinfo['money']=[leverage_ratio,ratio_stock,ratio_future,ratio_option,ratio_etf,stock_money,asset_value]
        # print(df_porinfo)
        return df_porinfo

    def stock_exposure_calculate(self,df_portfolio,asset_value,stock_money):
        proportion=stock_money/asset_value
        selecting_code_list = df_portfolio['code'].tolist()
        df_factor_exposure = self.df_factor[self.df_factor['code'].isin(selecting_code_list)]
        df_factor_exposure.fillna(0, inplace=True)
        df_factor_exposure.drop(columns='code', inplace=True)
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
    def option_future_exposure_calculate(self,df_detail,asset_value):
        df_detail['len'] = df_detail['合约'].apply(lambda x: len(x))
        df_future = df_detail[df_detail['len'] == 6]
        df_option = df_detail[~(df_detail['len'] == 6)]
        df_option['index'] = df_option['合约'].apply(lambda x: self.future_option_mapping(x))
        df_option['index_type'] = df_option['index'].apply(lambda x: self.index_finding(x))
        df_future['index_type'] = df_future['合约'].apply(lambda x: self.index_finding(x))
        df_option['weight'] = df_option['market_value'] / asset_value
        df_future['weight'] = df_future['market_value'] / asset_value
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
    def final_portfolio_exposure_processing(self,df_stock_factor,future_option_exposure,index_exposure):
        df_portfolio_exposure = df_stock_factor.merge(future_option_exposure, on='factor_name', how='left')
        df_portfolio_exposure.set_index('factor_name', inplace=True)
        df_portfolio_exposure['portfolio_exposure'] = df_portfolio_exposure.apply(lambda x: x.sum(), axis=1)
        df_portfolio_exposure.reset_index(inplace=True)
        df_portfolio_exposure=df_portfolio_exposure.merge(index_exposure,on='factor_name', how='left')
        df_portfolio_exposure['difference']=df_portfolio_exposure['portfolio_exposure']-df_portfolio_exposure['index_exposure']
        df_portfolio_exposure['ratio']=df_portfolio_exposure['difference']/abs(df_portfolio_exposure['index_exposure'])
        return df_portfolio_exposure
    def portfolio_tracking_main(self,product_name):
        index_type = self.product_index_withdraw(product_name)
        prodata=product_data(product_name)
        df_holding=prodata.position_withdraw()
        stock_money,asset_value=prodata.stock_info_dicesion()
        df_portfolio=prodata.portfolio_weight_withdraw()
        etf_money_today=prodata.etf_money_calculate(self.realtime_data_etf)
        stock_money_today = (1 + self.realtime_data_index[index_type].tolist()[0]) * stock_money
        asset_value_today = (1 + self.realtime_data_index[index_type].tolist()[0]) * asset_value
        index_exposure=self.portfolio_index_exposure_withdraw(product_name)
        df_info, df_final = self.FO_main(df_holding)
        df_porinfo = self.portfolio_info_processing(stock_money_today, asset_value_today, etf_money_today,df_info)
        df_stock_factor=self.stock_exposure_calculate(df_portfolio,asset_value,stock_money)
        option_future_exposure, exposure_final=self.option_future_exposure_calculate(df_final, asset_value)
        df_portfolio_exposure=self.final_portfolio_exposure_processing(df_stock_factor, exposure_final, index_exposure)
        return df_info,df_final,df_porinfo,option_future_exposure,df_portfolio_exposure
    def saving_main(self,product_name):
        df_info, df_final, df_porinfo, option_future_exposure, df_portfolio_exposure=self.portfolio_tracking_main(product_name)
        outputpath = glv.get('realtime_output')
        gt.folder_creator2(outputpath)
        outputpath = os.path.join(outputpath, str(product_name)+'_product_tracking.xlsx')
        with pd.ExcelWriter(outputpath, engine='openpyxl') as writer:
            df_porinfo.to_excel(writer, sheet_name='产品杠杆率', index=False)
            df_info.to_excel(writer, sheet_name='期权期货信息', index=False)
            df_portfolio_exposure.to_excel(writer,sheet_name='产品风险因子暴露', index=False)
            df_final.to_excel(writer, sheet_name='期货期权数据', index=False)
            option_future_exposure.to_excel(writer,sheet_name='期权期货因子暴露',index=False)
        # outputpath = glv.get('realtime_output2')
        # outputpath = os.path.join(outputpath, str(product_name) + '_product_tracking.xlsx')
        # with pd.ExcelWriter(outputpath, engine='openpyxl') as writer:
        #     df_porinfo.to_excel(writer, sheet_name='产品杠杆率', index=False)
        #     df_info.to_excel(writer, sheet_name='期权期货信息', index=False)
        #     df_portfolio_exposure.to_excel(writer, sheet_name='产品风险因子暴露', index=False)
        #     df_final.to_excel(writer, sheet_name='期货期权数据', index=False)
        #     option_future_exposure.to_excel(writer, sheet_name='期权期货因子暴露', index=False)




