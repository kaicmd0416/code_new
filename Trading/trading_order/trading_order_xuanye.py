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
class trading_xuanye:
    def __init__(self,df_weight,df_holding,df_mkt,target_date,stock_money,etf_pool,trading_time):
        self.df_weight=df_weight
        self.df_holding=df_holding
        self.df_mkt=df_mkt
        self.target_date=target_date
        self.stock_money=stock_money
        self.etf_pool=etf_pool
        self.trading_time=trading_time

    def trading_order_xuanye_mode_1(self):
        df_final = pd.DataFrame()
        etf_code =self.etf_pool
        inputpath = os.path.split(os.path.realpath(__file__))[0]
        inputpath = os.path.join(inputpath, '模板')
        inputpath = os.path.join(inputpath, '宣夜')
        inputpath = os.path.join(inputpath, 'trading_list模板.csv')
        outputpath = glv.get('product_trading')
        outputpath = os.path.join(outputpath, '宣夜')
        gt.folder_creator2(outputpath)
        target_date2 = gt.intdate_transfer(self.target_date)
        outputpath3 = os.path.join(outputpath, 'xy_' + target_date2 + '_ETF_trading_list.csv')
        outputpath2 = os.path.join(outputpath, 'xy_' + target_date2 + '_trading_list.csv')
        df = gt.readcsv(inputpath)
        df.columns = df.columns.tolist()[:2] + ['Time', 'Direction', 'UN1', 'open_action', 'UN2', '涨停', 'UN3', 'UN4']
        df[df.columns.tolist()[1]].iloc[1] = '宇量皓兴TWAP-' + str(self.target_date)
        df[df.columns.tolist()[2]].iloc[3] = self.trading_time
        df_today=self.df_weight.copy()
        code_list_today = df_today['code'].tolist()
        df_today = df_today.merge(self.df_mkt, on='code', how='left')
        df_today['weight'] = df_today['weight'].astype(float)
        df_today['close'] = df_today['close'].astype(float)
        df_error = df_today[(df_today['close'].astype(float) == 0)|(df_today['close'].isna())]
        if len(df_error) > 0:
            print('以下数据close出现问题')
            print(df_error)
        df_today['weight'] = df_today['weight'] / df_today['weight'].sum()
        df_today['quantity'] = self.stock_money * df_today['weight'] / df_today['close']
        df_today['quantity'] = round(df_today['quantity'] / 100, 0) * 100
        df_today = df_today[['code', 'quantity', 'close']]
        df_yes=self.df_holding.copy()
        df_weight = pd.DataFrame()
        code_list_yes = df_yes['code'].tolist()
        code_list = list(set(code_list_yes) | set(code_list_today))
        df_weight['code'] = code_list
        df_weight = df_weight.merge(df_yes, on='code', how='left')
        df_weight = df_weight.merge(df_today, on='code', how='left')
        df_weight.fillna(0, inplace=True)
        df_weight['difference'] = df_weight['quantity'] - df_weight['holding']
        list_difference = df_weight['difference'].tolist()
        df_weight_check = df_weight.copy()
        df_buying_check = df_weight_check[df_weight_check['difference'] > 0]
        df_selling_check = df_weight_check[df_weight_check['difference'] < 0]
        df_buying_check['mkt_value'] = df_buying_check['close'] * abs(df_buying_check['difference'])
        df_selling_check['mkt_value'] = df_selling_check['close'] * abs(df_selling_check['difference'])
        mkt_buying = df_buying_check['mkt_value'].sum()
        mkt_selling = df_selling_check['mkt_value'].sum()
        print('hy1号买额为:' + str(mkt_buying),
              '卖额为' + str(mkt_selling) + '买卖额差为' + str(mkt_buying - mkt_selling))
        list_action = []
        for i in list_difference:
            if i > 0:
                list_action.append('买入')
            elif i < 0:
                list_action.append('卖出')
            else:
                list_action.append('不动')
        df_weight['action'] = list_action
        df_weight['difference'] = abs(df_weight['difference'])
        df_weight = df_weight[df_weight['action'] != '不动']
        df_weight['new_code'] = df_weight['code'].apply(lambda x: str(x)[:2])
        df_weight.loc[
            (df_weight['new_code'] == '68') & (df_weight['action'] == '买入') & (df_weight['difference'] == 100), [
                'difference']] = 200
        df_weight['code'] = df_weight['code'].apply(lambda x: str(x)[:-3])
        df_weight1 = df_weight[df_weight['code'].isin(etf_code)]
        df_weight1 = df_weight1[['code', 'difference', 'action']]
        df_weight1.columns = ['code', 'quantity', '方向']
        df_weight2 = df_weight[~(df_weight['code'].isin(etf_code))]
        code_list_today = df_weight2['code'].tolist()
        quantity_list = df_weight2['difference'].tolist()
        action_list = df_weight2['action'].tolist()
        for i in range(len(df.columns.tolist())):
            columns_name = df.columns.tolist()[i]
            initial_list = df[columns_name].tolist()
            if i == 1:
                final_list = initial_list + code_list_today
            elif i == 2:
                final_list = initial_list + quantity_list
            elif i == 3:
                final_list = initial_list + action_list
            elif i == 5:
                final_list = initial_list + [0] * len(df_weight)
            elif i == 7:
                final_list = initial_list + ['FALSE'] * len(df_weight2)
            else:
                final_list = initial_list + [None] * len(df_weight2)
            df_final[columns_name] = final_list
        df_final.columns = df.columns.tolist()[:2] + [None] * 8
        df_final.to_csv(outputpath2, index=False, encoding='utf_8_sig')
        df_weight1.to_csv(outputpath3, index=False, encoding='gbk')

    def trading_order_xuanye_mode_2(self):
        df_final = pd.DataFrame()
        etf_code = self.etf_pool
        inputpath = os.path.split(os.path.realpath(__file__))[0]
        inputpath = os.path.join(inputpath, '模板')
        inputpath = os.path.join(inputpath, '宣夜')
        inputpath = os.path.join(inputpath, 'trading_list_v2模板.csv')
        outputpath = glv.get('product_trading')
        outputpath = os.path.join(outputpath, '宣夜')
        gt.folder_creator2(outputpath)
        target_date2 = gt.intdate_transfer(self.target_date)
        outputpath3 = os.path.join(outputpath, 'xy_' + target_date2 + '_ETF_trading_list.csv')
        outputpath2 = os.path.join(outputpath, 'xy_' + target_date2 + '_trading_list.csv')
        df = gt.readcsv(inputpath)
        df.columns = df.columns.tolist()[:2] + ['Time', 'Direction', 'UN2', '涨停', 'UN3', 'UN4']
        target_date2 = str(self.target_date)[:4] + str(self.target_date)[5:7] + str(self.target_date)[8:10]
        df[df.columns.tolist()[1]].iloc[1] = '宇量皓兴VWAP-' + str(target_date2)
        df[df.columns.tolist()[2]].iloc[3] = self.trading_time
        df_today=self.df_weight.copy()
        code_list_today = df_today['code'].tolist()
        df_today = df_today.merge(self.df_mkt, on='code', how='left')
        df_today['weight'] = pd.to_numeric(df_today['weight'], errors='coerce')
        df_today['close'] = pd.to_numeric(df_today['close'], errors='coerce')
        df_error = df_today[(df_today['close'] == 0.0) | (df_today['close'].isna()) | (df_today['weight'].isna())]
        df_today = df_today.dropna(subset=['weight', 'close'])
        df_today = df_today[~(df_today['close'] == 0.0)]
        if len(df_error) > 0:
            print('以下数据close出现问题')
            print(df_error)
        df_today['quantity'] = self.stock_money * df_today['weight'] / df_today['close']
        df_today['quantity'] = round(df_today['quantity'] / 100, 0) * 100
        df_today = df_today[['code', 'quantity', 'close']]
        df_yes=self.df_holding.copy()
        df_yes.rename(columns={'StockCode': 'code'}, inplace=True)
        df_weight = pd.DataFrame()
        code_list_yes = df_yes['code'].tolist()
        code_list = list(set(code_list_yes) | set(code_list_today))
        df_weight['code'] = code_list
        df_weight = df_weight.merge(df_yes, on='code', how='left')
        df_weight = df_weight.merge(df_today, on='code', how='left')
        df_weight.fillna(0, inplace=True)
        df_weight['difference'] = df_weight['quantity'] - df_weight['holding']
        df_weight_check = df_weight.copy()
        df_buying_check = df_weight_check[df_weight_check['difference'] > 0]
        df_selling_check = df_weight_check[df_weight_check['difference'] < 0]
        df_buying_check['mkt_value'] = df_buying_check['close'] * abs(df_buying_check['difference'])
        df_selling_check['mkt_value'] = df_selling_check['close'] * abs(df_selling_check['difference'])
        mkt_buying = df_buying_check['mkt_value'].sum()
        mkt_selling = df_selling_check['mkt_value'].sum()
        df_weight_check['mkt_holding'] = df_weight_check['close'] * df_weight_check['holding']
        mkt_holding = df_weight_check['mkt_holding'].sum()
        print('hy1号买额为:' + str(mkt_buying),
              '卖额为' + str(mkt_selling) + '买卖额差为' + str(mkt_buying - mkt_selling),
              'holding总市值为:' + str(mkt_holding))
        list_difference = df_weight['difference'].tolist()
        list_action = []
        for i in list_difference:
            if i > 0:
                list_action.append('买入')
            elif i < 0:
                list_action.append('卖出')
            else:
                list_action.append('不动')
        df_weight['action'] = list_action
        df_weight['difference'] = abs(df_weight['difference'])
        df_weight = df_weight[df_weight['action'] != '不动']
        df_weight['new_code'] = df_weight['code'].apply(lambda x: str(x)[:2])
        df_weight.loc[
            (df_weight['new_code'] == '68') & (df_weight['action'] == '买入') & (df_weight['difference'] == 100), [
                'difference']] = 200
        df_weight['code'] = df_weight['code'].apply(lambda x: str(x)[:-3])
        df_weight1 = df_weight[df_weight['code'].isin(etf_code)]
        df_weight1 = df_weight1[['code', 'difference', 'action']]
        df_weight1.columns = ['code', 'quantity', '方向']

        df_weight2 = df_weight[~(df_weight['code'].isin(etf_code))]
        code_list_today = df_weight2['code'].tolist()
        quantity_list = df_weight2['difference'].tolist()
        action_list = df_weight2['action'].tolist()
        for i in range(len(df.columns.tolist())):
            columns_name = df.columns.tolist()[i]
            initial_list = df[columns_name].tolist()
            if i == 1:
                final_list = initial_list + code_list_today
            elif i == 2:
                final_list = initial_list + quantity_list
            elif i == 3:
                final_list = initial_list + action_list
            elif i == 5:
                final_list = initial_list + ['FALSE'] * len(df_weight2)
            else:
                final_list = initial_list + [None] * len(df_weight2)
            df_final[columns_name] = final_list
        df_final.columns = df.columns.tolist()[:2] + [None] * 6
        df_final.to_csv(outputpath2, index=False, encoding='utf_8_sig')
        df_weight1.to_csv(outputpath3, index=False, encoding='gbk')

    def trading_order_xy_main(self,trading_mode):
        if trading_mode == 'mode_v1':
            self.trading_order_xuanye_mode_1()
        else:
            self.trading_order_xuanye_mode_2()

