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
class portfolio_saving_main:
    def __init__(self,target_date,realtime=False):
        self.dp=data_prepared(target_date,realtime)
        if realtime==True:
             self.target_date=gt.strdate_transfer(datetime.today())
        else:
            self.target_date=target_date
        self.df_mkt,self.etfpool=self.dp.mktData_withdraw()
    def PortfolioInfo_saving(self):
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
        return df_final

    def PortfolioHolinng_saving(self):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        productCode_list = self.dp.productCode_withdraw()
        portfolioList = self.dp.portfolioList_withdraw()
        inputpath_configsql = glv.get('config_sql')
        sm=gt.sqlSaving_main(inputpath_configsql, 'Portfolio_holding')
        df_total=pd.DataFrame()
        for product_code in productCode_list:
            print(product_code)
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
                df_final['update_time']=current_time
                df_total=pd.concat([df_total,df_final])
                df_total['update_time']=current_time
                sm.df_to_sql(df_final)
        return df_total
    def ProductWeight_saving(self,df_prodInfo):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        product_code_list=df_prodInfo['product_code'].unique().tolist()
        inputpath_configsql = glv.get('config_sql')
        sm = gt.sqlSaving_main(inputpath_configsql, 'Product_weight')
        for product_code in product_code_list:
            df_final=pd.DataFrame()
            count=0
            slice_df=df_prodInfo[df_prodInfo['product_code']==product_code]
            portfolio_list=slice_df['portfolio_name'].tolist()
            for portfolio_name in portfolio_list:
                weight=slice_df[slice_df['portfolio_name']==portfolio_name]['weight'].tolist()[0]
                df_portfolio=self.dp.portfolioWeight_withdraw(portfolio_name)
                df_portfolio=df_portfolio[['code','weight']]
                df_portfolio['weight']=df_portfolio['weight']*weight
                df_portfolio.rename(columns={'weight':portfolio_name},inplace=True)
                if count==0:
                    df_final=df_portfolio
                    count+=1
                else:
                    df_final=df_final.merge(df_portfolio,on='code',how='outer')
            df_final.fillna(0,inplace=True)
            df_final.set_index('code', inplace=True, drop=True)
            df_final['weight'] = df_final.apply(lambda x: x.sum(), axis=1)
            df_final = df_final[df_final['weight'] != 0]
            df_final.reset_index(inplace=True)
            df_final['product_code']=product_code
            df_final['valuation_date']=self.target_date
            df_final=df_final[['valuation_date','product_code','code','weight']]
            df_final['update_time']=current_time
            sm.df_to_sql(df_final)
    def ProductHolding_saving(self,df_prodInfo,df_portHolding):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        product_code_list=df_prodInfo['product_code'].unique().tolist()
        inputpath_configsql = glv.get('config_sql')
        sm = gt.sqlSaving_main(inputpath_configsql, 'Product_holding')
        for product_code in product_code_list:
            slice_portHolding=df_portHolding[df_portHolding['product_code']==product_code]
            df_final=pd.DataFrame()
            count=0
            slice_df=df_prodInfo[df_prodInfo['product_code']==product_code]
            portfolio_list=slice_df['portfolio_name'].tolist()
            for portfolio_name in portfolio_list:
                weight=slice_df[slice_df['portfolio_name']==portfolio_name]['weight'].tolist()[0]
                df_portfolio=slice_portHolding[slice_portHolding['portfolio_name']==portfolio_name]
                df_portfolio['quantity']=df_portfolio['quantity']*weight
                df_portfolio = df_portfolio[['code', 'quantity']]
                df_portfolio.rename(columns={'quantity':portfolio_name},inplace=True)
                if count==0:
                    df_final=df_portfolio
                    count+=1
                else:
                    df_final=df_final.merge(df_portfolio,on='code',how='outer')
            df_final.fillna(0,inplace=True)
            df_final.set_index('code', inplace=True, drop=True)
            df_final['quantity'] = df_final.apply(lambda x: x.sum(), axis=1)
            df_final['quantity']=round(df_final['quantity']/100,0)*100
            df_final = df_final[df_final['quantity'] != 0]
            df_final.reset_index(inplace=True)
            df_final['product_code']=product_code
            df_final['valuation_date']=self.target_date
            df_final=df_final[['valuation_date','product_code','code','quantity']]
            df_final['update_time']=current_time
            sm.df_to_sql(df_final)
    def sqlSaving_main(self):
        df_info = self.PortfolioInfo_saving()
        df_holding = self.PortfolioHolinng_saving()
        self.ProductWeight_saving(df_info)
        self.ProductHolding_saving(df_info, df_holding)
        # try:
        #     df_info=self.PortfolioInfo_saving()
        # except:
        #     print('PortfolioInfo更新有误')
        # try:
        #     df_holding=self.PortfolioHolinng_saving()
        # except:
        #     print('PortfolioHoling更新有误')
        # try:
        #     self.ProductWeight_saving(df_info)
        # except:
        #     print('ProductWeight更新有误')
        # try:
        #     self.ProductHolding_saving(df_info,df_holding)
        # except:
        #     print('ProductHolding更新有误')