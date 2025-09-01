import datetime
import os
import pandas as pd
import Product_tracking.global_setting.global_dic as glv
import global_tools_func.global_tools as gt
from Product_tracking.tools_func.tools_func2 import *
from Product_tracking.tools_func.tools_func import *
import numpy as np
def inputpath_withdraw(product_name):
    inputpath = glv.get('product_tracking')
    inputpath=os.path.join(inputpath,product_name)
    return inputpath
def valid_productname():
    inputpath = glv.get('product_tracking')
    product_list=os.listdir(inputpath)
    return product_list
def cross_section_data_withdraw(target_date,product_name):
    inputpath=inputpath_withdraw(product_name)
    target_date2=gt.intdate_transfer(target_date)
    inputpath_product=gt.file_withdraw(inputpath,target_date2)
    try:
        df_info = pd.read_excel(inputpath_product, sheet_name='产品表现汇总')
        df_info2=pd.read_excel(inputpath_product, sheet_name='期权期货信息')
        df_factor = pd.read_excel(inputpath_product, sheet_name='产品风险因子暴露')
        df_factor = df_factor.fillna(0).replace([np.inf, -np.inf], 0)
        # 将除了factor_name以外的列转换为float
        numeric_columns = [col for col in df_factor.columns if col != 'factor_name']
        df_factor[numeric_columns] = df_factor[numeric_columns].astype(float)
        df_weight = pd.read_excel(inputpath_product, sheet_name='实盘仓位差异')
    except:
        print(product_name+'在'+str(target_date)+'中的数据存在缺失，请检查'+str(inputpath)+'是否存在该文件')
        df_info=pd.DataFrame()
        df_info2=pd.DataFrame()
        df_factor=pd.DataFrame()
        df_weight=pd.DataFrame()
    return df_info,df_info2,df_factor,df_weight
class factor_processing:
    def __init__(self,product_name,target_date):
        self.df_info,self.df_info2, self.df_factor,self.df_weight = cross_section_data_withdraw(target_date, product_name)
        self.target_date=target_date
        df_factor_original=self.factor_adding()
        self.factor_name_list =df_factor_original['factor_name'].tolist()
        self.df_factor=df_factor_original[['stockPortfolio_exposure', 'optionPortfolio_exposure',
       'futurePortfolio_exposure', 'cbPortfolio_exposure',
       'portfolio_exposure','stockItself_exposure', 'futureItself_exposure',
       'optionItself_exposure', 'cbItself_exposure']]
        self.df_factor2=df_factor_original[['index_exposure', 'ratio']]
        self.df_factor_return=self.factor_return_calculate()
        self.product_code=product_code_withdraw(product_name)
    def factor_return_calculate(self):
        inputpath = glv.get('factor_return')
        target_date2 = gt.intdate_transfer(self.target_date)
        inputpath2 = gt.file_withdraw(inputpath, target_date2)
        df_factor_return = gt.readcsv(inputpath2)
        df_factor_return.drop(columns='valuation_date', inplace=True)
        df_factor2 = self.df_factor.copy()
        factor_exposure = list(
            np.array(np.multiply(np.mat(df_factor2.values), np.mat(df_factor_return.values).T)))
        df_factor_return = pd.DataFrame(factor_exposure, columns=df_factor2.columns.tolist())
        return df_factor_return
    def factor_adding(self):
        proportion_list=self.df_info[self.df_info['info_name'].isin(['股票占比','期货占比','期权占比','可转债占比'])]['money'].tolist()
        [stock_proportion, future_proportion, option_proportion, cb_proportion] = proportion_list
        df=self.df_factor.copy()
        if stock_proportion != 0:
            df['stockItself_exposure'] = df['stock_exposure'] / stock_proportion
        else:
            df['stockItself_exposure'] = df['stock_exposure']
        if future_proportion != 0:
            df['futureItself_exposure'] = df['future_exposure'] / stock_proportion
        else:
            df['futureItself_exposure'] = df['future_exposure']
        if option_proportion != 0:
            df['optionItself_exposure'] = df['option_exposure'] / option_proportion
        else:
            df['optionItself_exposure'] = df['option_exposure']
        if cb_proportion != 0:
            df['cbItself_exposure'] = df['cb_exposure'] / cb_proportion
        else:
            df['cbItself_exposure'] = df['cb_exposure']
        df.rename(columns={'stock_exposure': 'stockPortfolio_exposure', 'future_exposure': 'futurePortfolio_exposure',
                           'option_exposure': 'optionPortfolio_exposure', 'cb_exposure': 'cbPortfolio_exposure'},
                  inplace=True)
        return df
    def factor2_processing(self):
        df=self.df_factor2.copy()
        df['factor_name']=self.factor_name_list
        df.set_index('factor_name',inplace=True,drop=True)
        df=df.T
        df.reset_index(inplace=True)
        df['index']=['indexItself','ratio']
        df.rename(columns={'index':'name'},inplace=True)
        df['type']=['factor_exposure','ratio']
        df['valuation_date']=self.target_date
        df['product_code']=self.product_code
        return df
    def factor_processing_main(self):
        df_factor_return=self.df_factor_return.copy()
        df_factor=self.df_factor.copy()
        df_final = pd.DataFrame()
        for name in df_factor.columns:
            name2 = name[:-9]
            slice_df1 = df_factor[[name]]
            slice_df2 = df_factor_return[[name]]
            slice_df1['factor_name'] = self.factor_name_list
            slice_df2['factor_name'] = self.factor_name_list
            slice_df1.set_index('factor_name', inplace=True, drop=True)
            slice_df2.set_index('factor_name', inplace=True, drop=True)
            slice_df1 = slice_df1.T
            slice_df2 = slice_df2.T
            slice_df1['type'] = 'factor_exposure'
            slice_df2['type'] = 'factor_return'
            slice_df1['name'] = name2
            slice_df2['name'] = name2
            slice_df1.reset_index(inplace=True, drop=True)
            slice_df2.reset_index(inplace=True, drop=True)
            df_final = pd.concat([df_final, slice_df1], axis=0)
            df_final = pd.concat([df_final, slice_df2], axis=0)
        df_final['valuation_date']=self.target_date
        df_final['product_code']=self.product_code
        # 重新排序列
        other_cols = [col for col in df_final.columns if col not in ['product_code', 'valuation_date', 'name', 'type']]
        df_final = df_final[['product_code', 'valuation_date', 'name', 'type'] + other_cols]
        df_final2=self.factor2_processing()
        df_final=pd.concat([df_final,df_final2])
        return df_final
class product_processing:
    def __init__(self,product_name,target_date):
        self.df_info,self.df_info2, self.df_factor,self.df_weight = cross_section_data_withdraw(target_date, product_name)
        self.target_date=target_date
        self.product_code=product_code_withdraw(product_name)
    def productReturn_processing(self):
        df=self.df_info.copy()
        if len(df)>0:
            df = df[['product_split', '盈亏', '收益率(本身)bp', '超额(本身)bp', '超额贡献bp']]
            df.columns = ['name', 'profit', 'return', 'excessReturn', 'excessReturn_contribution']
            df[['return', 'excessReturn', 'excessReturn_contribution']] = df[['return', 'excessReturn',
                                                                              'excessReturn_contribution']] / 10000
            df.set_index('name', inplace=True, drop=True)
            df = df.T
            df.reset_index(inplace=True)
            df.rename(columns={'index': 'type'}, inplace=True)
            other_cols = [col for col in df.columns if col not in ['product_code', 'valuation_date']]
            df['valuation_date'] = self.target_date
            df['product_code'] = self.product_code
            df = df[['product_code', 'valuation_date'] + other_cols]
        return df
    def productPM_processing(self):
        if len(self.df_info)>0 and len(self.df_info2)>0:
            df = self.df_info.copy()
            df = df[['info_name', 'money']]
            df.set_index('info_name', inplace=True, drop=True)
            df = df.T
            df.reset_index(inplace=True, drop=True)
            df.columns = ['leverageRatio', 'stockProportion', 'futureProportion', 'optionProportion',
                          'cbProportion', 'etfProportion', 'stockMktValue', 'accumuateNetValue', 'AssetValue']
            mkt_stock = df['stockMktValue'].tolist()[0]
            asset_value = df['AssetValue'].tolist()[0]
            df['valuation_date'] = self.target_date
            df['product_code'] = self.product_code
            df['type'] = 'weightProportion'
            df = df[['product_code', 'valuation_date', 'type', 'leverageRatio', 'stockProportion', 'futureProportion',
                     'optionProportion',
                     'cbProportion', 'etfProportion']]
            df.columns = ['product_code', 'valuation_date', 'type', '杠杆率', '股票', '期货', '期权',
                          '转债', 'ETF']
            df2 = self.df_info2.copy()
            df2.set_index('info_name', inplace=True, drop=True)
            df2 = df2.T
            df2.reset_index(inplace=True, drop=True)
            df2 = df2[['期权市值', '期货市值', '可转债市值', '国债市值', 'etf市值']]
            df2['valuation_date'] = self.target_date
            df2['product_code'] = self.product_code
            df2['type'] = 'mktValue'
            df2['股票市值'] = mkt_stock
            df2 = df2[
                ['product_code', 'valuation_date', 'type', '股票市值', '期权市值', '期货市值', '可转债市值', '国债市值',
                 'etf市值']]
            df2.columns = ['product_code', 'valuation_date', 'type', '股票', '期权', '期货', '转债', '国债', 'ETF']
            df['产品'] = df['杠杆率']
            mkt_bond = df2['国债'].tolist()[0]
            if mkt_bond != None:
                mkt_proportion = mkt_bond / asset_value
            else:
                mkt_proportion = 0
            df['国债'] = mkt_proportion
            df2['产品'] = asset_value
        else:
            df=pd.DataFrame()
            df2=pd.DataFrame()
        return df,df2
    def productInfo_processing(self):
        df=self.df_info.copy()
        if len(df)>0:
            df = df[['info_name', 'money']]
            df.set_index('info_name', inplace=True, drop=True)
            df = df.T
            df.reset_index(inplace=True, drop=True)
            df.columns = ['leverageRatio', 'stockProportion', 'futureProportion', 'optionProportion',
                          'cbProportion', 'etfProportion', 'stockMktValue', 'accumuateNetValue', 'AssetValue']
            df['valuation_date'] = self.target_date
            df['product_code'] = self.product_code
            df = df[['product_code', 'valuation_date', 'accumuateNetValue', 'AssetValue']]

        return df
    def productWeight_processing(self):
        df=self.df_weight.copy()
        if len(df)>0:
            df = df[~(df['code'] == '仓位差异')]
            df['valuation_date'] = self.target_date
            df['product_code'] = self.product_code
            other_cols = [col for col in df.columns if col not in ['product_code', 'valuation_date']]
            df = df[['product_code', 'valuation_date'] + other_cols]
            df['code'] = df['code'].apply(lambda x: str(x)[:6])
            df = gt.code_transfer(df)
        return df
    def productProcessing_main(self):
            df_return = self.productReturn_processing()
            df1,df2=self.productPM_processing()
            if len(df1)>0 and len(df2)>0 and len(df_return)>0:
                df_return = pd.concat([df_return, df1])
                df_return = pd.concat([df_return, df2])
                df_return.columns = ['product_code', 'valuation_date', 'type', 'product', 'stock', 'option', 'future',
                                     'cBond', 'bond', 'ETF', 'error', 'leverageRatio']

            df_info = self.productInfo_processing()
            df_weight = self.productWeight_processing()
            return df_return, df_info, df_weight
def ProdTrackingSql_main(start_date,end_date,product_name):
    outputpath_original = glv.get('output_sql')
    outputpath_original=os.path.join(outputpath_original,'ProductTracking')
    outputpath_factor=os.path.join(outputpath_original,'ProdFactor')
    outputpath_return=os.path.join(outputpath_original,'ProdDetail')
    outputpath_info=os.path.join(outputpath_original,'ProdInfo')
    outputpath_weight=os.path.join(outputpath_original,'ProdWeight')
    outputpath_factor=os.path.join(outputpath_factor,product_name)
    outputpath_return=os.path.join(outputpath_return,product_name)
    outputpath_info=os.path.join(outputpath_info,product_name)
    outputpath_weight=os.path.join(outputpath_weight,product_name)
    gt.folder_creator2(outputpath_factor)
    gt.folder_creator2(outputpath_return)
    gt.folder_creator2(outputpath_info)
    gt.folder_creator2(outputpath_weight)
    running_date_list = gt.working_days_list(start_date, end_date)
    for date in running_date_list:
        date2=gt.intdate_transfer(date)
        daily_factor=os.path.join(outputpath_factor,'ProdFactor_'+str(date2)+'.csv')
        daily_return=os.path.join(outputpath_return,'ProdReturn_'+str(date2)+'.csv')
        daily_info=os.path.join(outputpath_info,'ProdInfo_'+str(date2)+'.csv')
        daily_weight=os.path.join(outputpath_weight,'ProdWeight_'+str(date2)+'.csv')
        try:
            fp = factor_processing(product_name, date)
            df_factor = fp.factor_processing_main()
        except:
            df_factor=pd.DataFrame()
            print('factor_processing出现问题')
        try:
            pp = product_processing(product_name, date)
            df_return, df_info, df_weight = pp.productProcessing_main()
        except:
            df_return, df_info, df_weight=pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
            print('product_processing出现问题')
        if not df_factor.empty:
            df_factor.to_csv(daily_factor,index=False,encoding='gbk')
        else:
            print(str(product_name)+'在'+str(date)+'没有因子数据')
        if not df_return.empty:
            df_return.to_csv(daily_return,index=False,encoding='gbk')
        else:
            print(str(product_name)+'在'+str(date)+'没有收益数据')
        if not df_info.empty:
            df_info.to_csv(daily_info,index=False,encoding='gbk')
        else:
            print(str(product_name)+'在'+str(date)+'没有信息数据')
        if not df_weight.empty:
            df_weight.to_csv(daily_weight,index=False,encoding='gbk')
        else:
            print(str(product_name)+'在'+str(date)+'没有仓位数据')
# product_list=['念空瑞景39号','瑞锐精选','仁睿价值精选1号','瑞锐500指增','宣夜惠盈1号','高毅振英1号','念空知行4号']
# for product in product_list:
#       ProdTrackingSql_main('2024-07-01','2025-03-20',product)
# pp=product_processing(['念空瑞景39号','瑞锐精选','仁睿价值精选1号','瑞锐500指增','宣夜惠盈1号','高毅振英1号','念空知行4号'],'2025-03-11')
# pp.productProcessing_main()
