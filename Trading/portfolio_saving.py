import os
import pandas as pd
import global_setting.global_dic as glv
import sys
from datetime import datetime
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt
from data_prepared import data_prepared
from holding_construct import holding_construction
global source,config_path
source=glv.get('source')
config_path=glv.get('config_path')
if source=='local':
    print('暂不支持local模式')
    raise ValueError
class portfolio_saving:
    def __init__(self,target_date,realtime=False):
        self.dp=data_prepared(target_date,realtime)
        self.target_date=target_date
        self.df_mkt=self.dp.mktData_withdraw()
    def portfolioInfo_saving(self):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        inputpath_config = glv.get('config_product')
        excel_file = pd.ExcelFile(inputpath_config)
        sheet_names = excel_file.sheet_names
        detail_name = sheet_names[0]
        df_info = pd.read_excel(inputpath_config, detail_name)
        df_final = pd.DataFrame()
        for sheet_name in sheet_names[1:]:
            slice_df_info = df_info[df_info['product_name'] == sheet_name]
            slice_df_info = slice_df_info[['product_name', 'index_type', 'product_code']]
            df = pd.read_excel(inputpath_config, sheet_name=sheet_name)
            df.rename(columns={'score_name': 'portfolio_name'}, inplace=True)
            df['product_name'] = sheet_name
            df = df.merge(slice_df_info, on='product_name', how='left')
            df_final = pd.concat([df_final, df])
        df_final['valuation_date'] = self.target_date
        df_final['update_time'] = current_time
        df_final = df_final[
            ['valuation_date', 'product_name', 'product_code', 'portfolio_name', 'weight', 'index_type', 'update_time']]
        inputpath_configsql = glv.get('config_sql')
        sm = gt.sqlSaving_main(inputpath_configsql, 'Portfolio_product')
        sm.df_to_sql(df_final)
    def paperPortfolio_saving(self):
        productCode_list = self.dp.productCode_withdraw()
        portfolioList = self.dp.portfolioList_withdraw()
        inputpath_configsql = glv.get('config_sql')
        sm=gt.sqlSaving_main(inputpath_configsql, 'Portfolio_holding')
        for product_code in productCode_list:
            for portfolio_name in portfolioList:
                stock_money = self.dp.productInfo_withdraw(product_code)
                df_portfolio=self.dp.portfolioWeight_withdraw(portfolio_name)
                hs = holding_construction(df_portfolio,self.df_mkt,stock_money)
                df_final=hs.consturction_main()
                df_final=df_final[df_final['quantity']!=0]
                df_final['valuation_date']=self.target_date
                df_final['product_code']=product_code
                df_final['portfolio_name']=portfolio_name
                df_final=df_final[['valuation_date','product_code','portfolio_name','code','quantity']]
                sm.df_to_sql(df_final)
    def sqlSaving_main(self):
        try:
            self.portfolioInfo_saving()
        except:
            print('portfolioInfo更新有误')
        try:
            self.paperPortfolio_saving()
        except:
            print('paperPortfolio更新有误')







if __name__ == '__main__':
    ps=portfolio_saving( '2025-06-09')
    ps.paperPortfolio_saving()
     # productInfo_withdraw('2025-06-09','STH580')