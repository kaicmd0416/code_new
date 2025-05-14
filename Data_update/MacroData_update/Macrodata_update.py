import os
import pandas as pd
import global_setting.global_dic as glv
import sys
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt
from tools_func.tools_func import *
from MacroData_update.MacroData_preparing import macroData_preparing
from setup_logger.logger_setup import setup_logger

class MacroData_update:
    def __init__(self,start_date,end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.logger = setup_logger('Macrodata_update')
        self.logger.info('\n' + '*'*50 + '\nMACRODATA UPDATE PROCESSING\n' + '*'*50)
    def M1M2_data_update(self):
        self.logger.info('\nProcessing M1M2 data update...')
        outputpath=glv.get('output_m1m2')
        gt.folder_creator2(outputpath)
        inputlist=os.listdir(outputpath)
        if len(inputlist)==0:
            if self.start_date>='2024-01-01':
                start_date='2024-01-01'
            else:
                start_date=self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        for available_date in working_days_list:
            self.logger.info(f'Processing date: {available_date}')
            available_date2=gt.intdate_transfer(available_date)
            outputpath_daily=os.path.join(outputpath,'M1M2_'+str(available_date2)+'.csv')
            mdp=macroData_preparing(available_date)
            df_m1m2=mdp.raw_M1M2_wind_withdraw()
            if len(df_m1m2)>0:
                df_m1m2.set_index('CODE', inplace=True, drop=True)
                df_m1m2 = df_m1m2.T
                df_m1m2.reset_index(inplace=True)
                df_m1m2.drop(columns='index', inplace=True)
                df_m1m2['valuation_date']=available_date
                df_m1m2=df_m1m2[['valuation_date']+df_m1m2.columns.tolist()[:-1]]
                df_m1m2.to_csv(outputpath_daily, index=False)
                self.logger.info(f'Successfully saved M1M2 data for date: {available_date}')
            else:
                self.logger.warning(f'M1M2数据在：{available_date}数据更新出现错误')
    def Shibor_data_update(self):
        self.logger.info('\nProcessing Shibor data update...')
        outputpath=glv.get('output_shibor')
        gt.folder_creator2(outputpath)
        inputlist=os.listdir(outputpath)
        if len(inputlist)==0:
            if self.start_date>='2024-01-01':
                start_date='2024-01-01'
            else:
                start_date=self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        for available_date in working_days_list:
            self.logger.info(f'Processing date: {available_date}')
            available_date2=gt.intdate_transfer(available_date)
            outputpath_daily=os.path.join(outputpath,'Shibor_'+str(available_date2)+'.csv')
            mdp=macroData_preparing(available_date)
            df_shibor=mdp.raw_shibor_wind_withdraw()
            if len(df_shibor)>0:
                df_shibor.set_index('CODE', inplace=True, drop=True)
                df_shibor = df_shibor.T
                df_shibor.reset_index(inplace=True)
                df_shibor.rename(columns={'index':'type'}, inplace=True)
                df_shibor['valuation_date']=available_date
                df_shibor=df_shibor[['valuation_date']+df_shibor.columns.tolist()[:-1]]
                df_shibor.to_csv(outputpath_daily, index=False)
                self.logger.info(f'Successfully saved Shibor data for date: {available_date}')
            else:
                self.logger.warning(f'Shibor数据在：{available_date}数据更新出现错误')
    def CB_data_update(self):
        self.logger.info('\nProcessing China Government Bonds data update...')
        outputpath=glv.get('output_cb')
        gt.folder_creator2(outputpath)
        inputlist=os.listdir(outputpath)
        if len(inputlist)==0:
            if self.start_date>='2024-01-01':
                start_date='2024-01-01'
            else:
                start_date=self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        for available_date in working_days_list:
            self.logger.info(f'Processing date: {available_date}')
            available_date2=gt.intdate_transfer(available_date)
            outputpath_daily=os.path.join(outputpath,'ChinaGovernmentBonds_'+str(available_date2)+'.csv')
            mdp=macroData_preparing(available_date)
            df_cb=mdp.raw_CB_wind_withdraw()
            if len(df_cb)>0:
                df_cb.set_index('CODE', inplace=True, drop=True)
                df_cb = df_cb.T
                df_cb.reset_index(inplace=True)
                df_cb.drop(columns='index', inplace=True)
                df_cb['valuation_date']=available_date
                df_cb=df_cb[['valuation_date']+df_cb.columns.tolist()[:-1]]
                df_cb.to_csv(outputpath_daily, index=False)
                self.logger.info(f'Successfully saved China Government Bonds data for date: {available_date}')
            else:
                self.logger.warning(f'ChinaGovernmentBonds数据在：{available_date}数据更新出现错误')
    def CDB_data_update(self):
        self.logger.info('\nProcessing China Development Bank Bonds data update...')
        outputpath=glv.get('output_cdb')
        gt.folder_creator2(outputpath)
        inputlist=os.listdir(outputpath)
        if len(inputlist)==0:
            if self.start_date>='2024-01-01':
                start_date='2024-01-01'
            else:
                start_date=self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        for available_date in working_days_list:
            self.logger.info(f'Processing date: {available_date}')
            available_date2=gt.intdate_transfer(available_date)
            outputpath_daily=os.path.join(outputpath,'ChinaDevelopmentBankBonds_'+str(available_date2)+'.csv')
            mdp=macroData_preparing(available_date)
            df_cdb=mdp.raw_CDB_wind_withdraw()
            if len(df_cdb)>0:
                df_cdb.set_index('CODE', inplace=True, drop=True)
                df_cdb = df_cdb.T
                df_cdb.reset_index(inplace=True)
                df_cdb.drop(columns='index', inplace=True)
                df_cdb['valuation_date']=available_date
                df_cdb=df_cdb[['valuation_date']+df_cdb.columns.tolist()[:-1]]
                df_cdb.to_csv(outputpath_daily, index=False)
                self.logger.info(f'Successfully saved China Development Bank Bonds data for date: {available_date}')
            else:
                self.logger.warning(f'ChinaDevelopmentBankBonds数据在：{available_date}数据更新出现错误')
    def CMN_data_update(self):
        self.logger.info('\nProcessing China Medium Term Notes data update...')
        outputpath=glv.get('output_cmn')
        gt.folder_creator2(outputpath)
        inputlist=os.listdir(outputpath)
        if len(inputlist)==0:
            if self.start_date>='2024-01-01':
                start_date='2024-01-01'
            else:
                start_date=self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        for available_date in working_days_list:
            self.logger.info(f'Processing date: {available_date}')
            available_date2=gt.intdate_transfer(available_date)
            outputpath_daily=os.path.join(outputpath,'ChinaMediumTermNotes_'+str(available_date2)+'.csv')
            mdp=macroData_preparing(available_date)
            df_cmn=mdp.raw_CMN_wind_withdraw()
            if len(df_cmn)>0:
                df_cmn.set_index('CODE', inplace=True, drop=True)
                df_cmn = df_cmn.T
                df_cmn.reset_index(inplace=True)
                df_cmn.drop(columns='index', inplace=True)
                df_cmn['valuation_date']=available_date
                df_cmn=df_cmn[['valuation_date']+df_cmn.columns.tolist()[:-1]]
                df_cmn.to_csv(outputpath_daily, index=False)
                self.logger.info(f'Successfully saved China Medium Term Notes data for date: {available_date}')
            else:
                self.logger.warning(f'ChinaMediumTermNotes数据在：{available_date}数据更新出现错误')
    def CPI_data_update(self):
        self.logger.info('\nProcessing CPI data update...')
        outputpath=glv.get('output_cpi')
        gt.folder_creator2(outputpath)
        inputlist=os.listdir(outputpath)
        if len(inputlist)==0:
            if self.start_date>='2024-01-01':
                start_date='2024-01-01'
            else:
                start_date=self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        for available_date in working_days_list:
            self.logger.info(f'Processing date: {available_date}')
            available_date2=gt.intdate_transfer(available_date)
            outputpath_daily=os.path.join(outputpath,'CPI_'+str(available_date2)+'.csv')
            mdp=macroData_preparing(available_date)
            df_cpi=mdp.raw_cpi_wind_withdraw()
            if len(df_cpi)>0:
                df_cpi.set_index('CODE', inplace=True, drop=True)
                df_cpi = df_cpi.T
                df_cpi.reset_index(inplace=True)
                df_cpi.drop(columns='index', inplace=True)
                df_cpi['valuation_date']=available_date
                df_cpi=df_cpi[['valuation_date']+df_cpi.columns.tolist()[:-1]]
                df_cpi.to_csv(outputpath_daily, index=False)
                self.logger.info(f'Successfully saved CPI data for date: {available_date}')
            else:
                self.logger.warning(f'CPI数据在：{available_date}数据更新出现错误')
    def PPI_data_update(self):
        self.logger.info('\nProcessing PPI data update...')
        outputpath=glv.get('output_ppi')
        gt.folder_creator2(outputpath)
        inputlist=os.listdir(outputpath)
        if len(inputlist)==0:
            if self.start_date>='2024-01-01':
                start_date='2024-01-01'
            else:
                start_date=self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        for available_date in working_days_list:
            self.logger.info(f'Processing date: {available_date}')
            available_date2=gt.intdate_transfer(available_date)
            outputpath_daily=os.path.join(outputpath,'PPI_'+str(available_date2)+'.csv')
            mdp=macroData_preparing(available_date)
            df_ppi=mdp.raw_ppi_wind_withdraw()
            if len(df_ppi)>0:
                df_ppi.set_index('CODE', inplace=True, drop=True)
                df_ppi = df_ppi.T
                df_ppi.reset_index(inplace=True)
                df_ppi.drop(columns='index', inplace=True)
                df_ppi['valuation_date']=available_date
                df_ppi=df_ppi[['valuation_date']+df_ppi.columns.tolist()[:-1]]
                df_ppi.to_csv(outputpath_daily, index=False)
                self.logger.info(f'Successfully saved PPI data for date: {available_date}')
            else:
                self.logger.warning(f'PPI数据在：{available_date}数据更新出现错误')
    def PMI_data_update(self):
        self.logger.info('\nProcessing PMI data update...')
        outputpath=glv.get('output_pmi')
        gt.folder_creator2(outputpath)
        inputlist=os.listdir(outputpath)
        if len(inputlist)==0:
            if self.start_date>='2024-01-01':
                start_date='2024-01-01'
            else:
                start_date=self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        for available_date in working_days_list:
            self.logger.info(f'Processing date: {available_date}')
            available_date2=gt.intdate_transfer(available_date)
            outputpath_daily=os.path.join(outputpath,'PMI_'+str(available_date2)+'.csv')
            mdp=macroData_preparing(available_date)
            df_pmi=mdp.raw_pmi_wind_withdraw()
            if len(df_pmi)>0:
                df_pmi.set_index('CODE', inplace=True, drop=True)
                df_pmi = df_pmi.T
                df_pmi.reset_index(inplace=True)
                df_pmi.drop(columns='index', inplace=True)
                df_pmi['valuation_date']=available_date
                df_pmi=df_pmi[['valuation_date']+df_pmi.columns.tolist()[:-1]]
                df_pmi.to_csv(outputpath_daily, index=False)
                self.logger.info(f'Successfully saved PMI data for date: {available_date}')
            else:
                self.logger.warning(f'PMI数据在：{available_date}数据更新出现错误')
    def SocialFinance_data_update(self):
        self.logger.info('\nProcessing Social Finance data update...')
        outputpath=glv.get('output_socialFinance')
        gt.folder_creator2(outputpath)
        inputlist=os.listdir(outputpath)
        if len(inputlist)==0:
            if self.start_date>='2024-01-01':
                start_date='2024-01-01'
            else:
                start_date=self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        for available_date in working_days_list:
            self.logger.info(f'Processing date: {available_date}')
            available_date2=gt.intdate_transfer(available_date)
            outputpath_daily=os.path.join(outputpath,'SocialFinance_'+str(available_date2)+'.csv')
            mdp=macroData_preparing(available_date)
            df_sf=mdp.raw_socialFinance_wind_withdraw()
            if len(df_sf)>0:
                df_sf.set_index('CODE', inplace=True, drop=True)
                df_sf = df_sf.T
                df_sf.reset_index(inplace=True)
                df_sf.drop(columns='index', inplace=True)
                df_sf['valuation_date']=available_date
                df_sf=df_sf[['valuation_date']+df_sf.columns.tolist()[:-1]]
                df_sf.to_csv(outputpath_daily, index=False)
                self.logger.info(f'Successfully saved Social Finance data for date: {available_date}')
            else:
                self.logger.warning(f'SocialFinance数据在：{available_date}数据更新出现错误')
    def LargeOrderInflow_data_update(self):
        self.logger.info('\nProcessing Large Order Inflow data update...')
        outputpath=glv.get('output_LargeOrderInflow')
        gt.folder_creator2(outputpath)
        inputlist=os.listdir(outputpath)
        if len(inputlist)==0:
            if self.start_date>='2024-01-01':
                start_date='2024-01-01'
            else:
                start_date=self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        for available_date in working_days_list:
            self.logger.info(f'Processing date: {available_date}')
            available_date2=gt.intdate_transfer(available_date)
            outputpath_daily=os.path.join(outputpath,'LargeOrderInflow_'+str(available_date2)+'.csv')
            mdp=macroData_preparing(available_date)
            df_li=mdp.raw_LargeOrderInflow_wind_withdraw()
            if len(df_li)>0:
                df_li.set_index('CODE', inplace=True, drop=True)
                df_li = df_li.T
                df_li.reset_index(inplace=True)
                df_li.drop(columns='index', inplace=True)
                df_li['valuation_date']=available_date
                df_li=df_li[['valuation_date']+df_li.columns.tolist()[:-1]]
                df_li.to_csv(outputpath_daily, index=False)
                self.logger.info(f'Successfully saved Large Order Inflow data for date: {available_date}')
            else:
                self.logger.warning(f'LargeOrderInflow数据在：{available_date}数据更新出现错误')
    def USD_data_update(self):
        self.logger.info('\nProcessing USD data update...')
        outputpath=glv.get('output_USD')
        gt.folder_creator2(outputpath)
        inputlist=os.listdir(outputpath)
        if len(inputlist)==0:
            if self.start_date>='2024-01-01':
                start_date='2024-01-01'
            else:
                start_date=self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        for available_date in working_days_list:
            self.logger.info(f'Processing date: {available_date}')
            available_date2=gt.intdate_transfer(available_date)
            outputpath_daily=os.path.join(outputpath,'UsDollar_'+str(available_date2)+'.csv')
            mdp=macroData_preparing(available_date)
            df_usd=mdp.raw_USD_wind_withdraw()
            if len(df_usd)>0:
                df_usd.set_index('CODE', inplace=True, drop=True)
                df_usd = df_usd.T
                df_usd.reset_index(inplace=True)
                df_usd.rename(columns={'index':'type'}, inplace=True)
                df_usd['valuation_date']=available_date
                df_usd=df_usd[['valuation_date']+df_usd.columns.tolist()[:-1]]
                df_usd.to_csv(outputpath_daily, index=False)
                self.logger.info(f'Successfully saved USD data for date: {available_date}')
            else:
                self.logger.warning(f'UsDollar数据在：{available_date}数据更新出现错误')
    def USindex_data_update(self):
        self.logger.info('\nProcessing US Index data update...')
        outputpath=glv.get('output_USIndex')
        gt.folder_creator2(outputpath)
        inputlist=os.listdir(outputpath)
        if len(inputlist)==0:
            if self.start_date>='2024-01-01':
                start_date='2024-01-01'
            else:
                start_date=self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        for available_date in working_days_list:
            self.logger.info(f'Processing date: {available_date}')
            available_date2=gt.intdate_transfer(available_date)
            outputpath_daily=os.path.join(outputpath,'UsIndex_'+str(available_date2)+'.csv')
            mdp=macroData_preparing(available_date)
            df_usi=mdp.raw_USIndex_wind_withdraw()
            if len(df_usi)>0:
                df_usi.set_index('CODE', inplace=True, drop=True)
                df_usi = df_usi.T
                df_usi.reset_index(inplace=True)
                df_usi.rename(columns={'index':'type'},inplace=True)
                df_usi['valuation_date']=available_date
                df_usi=df_usi[['valuation_date']+df_usi.columns.tolist()[:-1]]
                df_usi.to_csv(outputpath_daily, index=False)
                self.logger.info(f'Successfully saved US Index data for date: {available_date}')
            else:
                self.logger.warning(f'UsIndex数据在：{available_date}数据更新出现错误')
    def IndexScore_data_update(self):
        self.logger.info('\nProcessing Index Score data update...')
        outputpath=glv.get('output_indexScore')
        gt.folder_creator2(outputpath)
        inputlist=os.listdir(outputpath)
        if len(inputlist)==0:
            if self.start_date>='2024-01-01':
                start_date='2024-01-01'
            else:
                start_date=self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        for available_date in working_days_list:
            self.logger.info(f'Processing date: {available_date}')
            available_date2=gt.intdate_transfer(available_date)
            outputpath_daily=os.path.join(outputpath,'IndexScoreD_'+str(available_date2)+'.csv')
            mdp=macroData_preparing(available_date)
            df_score=mdp.raw_indexScore_withdraw()
            df_index_hs300 = gt.index_weight_withdraw('沪深300', available_date)
            df_index_gz2000 = gt.index_weight_withdraw('国证2000', available_date)
            if len(df_score)>0 and len(df_index_hs300)>0 and len(df_index_gz2000)>0:
                hs300_code_list = df_index_hs300['code'].tolist()
                gz2000_code_list = df_index_gz2000['code'].tolist()
                df_score_hs300 = df_score[df_score['code'].isin(hs300_code_list)]
                df_score_gz2000 = df_score[df_score['code'].isin(gz2000_code_list)]
                hs300_final_score = df_score_hs300['final_score'].mean()
                gz2000_final_score = df_score_gz2000['final_score'].mean()
                df_final=pd.DataFrame()
                df_final['valuation_date']=[available_date]
                df_final['hs300_score']=hs300_final_score
                df_final['gz2000_score']=gz2000_final_score
                if len(df_final)>0:
                    df_final.to_csv(outputpath_daily, index=False)
                    self.logger.info(f'Successfully saved Index Score data for date: {available_date}')
            else:
                self.logger.warning(f'Index Score data数据在：{available_date}数据更新出现错误')
if __name__ == '__main__':
    mdu= MacroData_update('2025-05-09','2025-05-09')
    mdu.IndexScore_data_update()

