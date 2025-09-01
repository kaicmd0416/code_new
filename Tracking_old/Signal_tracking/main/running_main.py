from Signal_tracking.data_prepare.data_prepare import data_prepare
from Signal_tracking.analyse.score_analyse import score_analyse
from Signal_tracking.analyse.portfolio_anaylse import portfolio_analyse
import Signal_tracking.global_setting.global_dic as glv
import global_tools_func.global_tools as gt
import os
import pandas as pd
class analyse_main:
    def __init__(self,target_date):
        self.target_date=target_date
        dp=data_prepare(target_date)
        self.df_stock=dp.crossSection_stock_return_withdraw()
        self.df_index=dp.crossSection_index_return_withdraw()
        self.df_hs300, self.df_zzA500, self.df_zz500, self.df_zz1000=dp.index_component_withdraw()
    def columns_name(self):
        columns_name=['valuation_date','index_type','missing','top','component_1.0_0.9','component_0.9_0.8','component_0.8_0.7','component_0.7_0.6','component_0.6_0.5','component_0.5_0.4','component_0.4_0.3','component_0.3_0.2','component_0.2_0.1','component_0.1_0.0']
        return columns_name
    def columns_name2(self):
        columns_name=['valuation_date','portfolio_name','missing','top','component_1.0_0.9','component_0.9_0.8','component_0.8_0.7','component_0.7_0.6','component_0.6_0.5','component_0.5_0.4','component_0.4_0.3','component_0.3_0.2','component_0.2_0.1','component_0.1_0.0']
        return columns_name
    def score_withdraw(self,score_type):
        available_date = gt.last_workday_calculate(self.target_date)
        available_date = gt.intdate_transfer(available_date)
        inputpath = glv.get('score')
        inputpath = os.path.join(inputpath,score_type)
        inputpath = gt.file_withdraw(inputpath, available_date)
        df = gt.readcsv(inputpath)
        return df
    def portfolio_withdraw(self,portfolio_name):
        inputpath=glv.get('portfolio_weight')
        inputpath=os.path.join(inputpath,portfolio_name)
        target_date2=gt.intdate_transfer(self.target_date)
        inputpath=gt.file_withdraw(inputpath,target_date2)
        df=gt.readcsv(inputpath)
        return df
    def score_running_main(self,score_type_list,index_type_list):
        columns_name=self.columns_name()
        outputpath=glv.get('output1')
        for score_type in score_type_list:
            df_result = pd.DataFrame()
            outputpath1=os.path.join(outputpath,score_type)
            gt.folder_creator2(outputpath1)
            df_score=self.score_withdraw(score_type)
            sa=score_analyse(df_score,self.df_stock,self.df_index,self.df_hs300,self.df_zz500,self.df_zzA500,self.df_zz1000)
            for index_type in index_type_list:
                df_final=sa.portfolio_analyse(index_type)
                df_final['index_type']=index_type
                df_result=pd.concat([df_result,df_final])
                df_result['valuation_date']=self.target_date
                df_result=df_result[columns_name]
            target_date2 = gt.intdate_transfer(self.target_date)
            outputpath1 = os.path.join(outputpath1, str(score_type) + '_' + str(target_date2) + '.csv')
            df_result.to_csv(outputpath1, index=False, encoding='gbk')
        return df_result
    def portfolio_running_main(self,portfolio_list):
        outputpath=glv.get('output2')
        outputpath1=os.path.join(outputpath,'return_contribution')
        outputpath2 = os.path.join(outputpath, 'weight')
        gt.folder_creator2(outputpath1)
        gt.folder_creator2(outputpath2)
        df_result=pd.DataFrame()
        df_result2=pd.DataFrame()
        for portfolio_name in portfolio_list:
            df=self.portfolio_withdraw(portfolio_name)
            if len(df)>0:
                pa = portfolio_analyse(df, self.df_stock, self.df_index, self.df_hs300, self.df_zz500, self.df_zzA500,
                                       self.df_zz1000)
                df1, df2 = pa.portfolio_analyse(portfolio_name, self.target_date)
                df1['portfolio_name'] = portfolio_name
                df2['portfolio_name'] = portfolio_name
                df_result = pd.concat([df_result, df1])
                df_result2 = pd.concat([df_result2, df2])
            else:
                print('没有找到'+str(portfolio_name))
        df_result['valuation_date']=self.target_date
        df_result2['valuation_date'] = self.target_date
        columns_name=self.columns_name2()
        df_result=df_result[columns_name]
        df_result2 = df_result2[columns_name]
        target_date2=gt.intdate_transfer(self.target_date)
        outputpath1=os.path.join(outputpath1,'ReturnContribution_'+str(target_date2)+'.csv')
        outputpath2 = os.path.join(outputpath2, 'Weight_' + str(target_date2) + '.csv')
        df_result.to_csv(outputpath1,index=False)
        df_result2.to_csv(outputpath2, index=False)
        return df_result,df_result2




