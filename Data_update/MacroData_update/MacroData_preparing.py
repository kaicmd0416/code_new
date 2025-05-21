import os
import sys
path = os.getenv('GLOBAL_TOOLSFUNC')
import global_setting.global_dic as glv
import pandas as pd
import yaml
sys.path.append(path)
import global_tools as gt
from setup_logger.logger_setup import setup_logger

class macroData_preparing:
    def __init__(self,available_date):
        self.available_date =available_date
        
    def rename_code_by_folder_wind(self, df, folder_name):
        """
        根据rename_mapping.yaml中的配置重命名指定文件夹中的代码
        
        Args:
            df (pd.DataFrame): 需要处理的DataFrame
            folder_name (str): 需要处理的文件夹名称
        Returns:
            pd.DataFrame: 处理后的DataFrame
        """
        # 读取rename_mapping.yaml文件
        yaml_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config_project', 'Data_update', 'rename_mapping_wind.yaml')
        with open(yaml_path, 'r', encoding='utf-8') as f:
            mapping_config = yaml.safe_load(f)
        
        # 获取指定folder_name的映射配置
        folder_mappings = [m for m in mapping_config['rename_mapping'] if m['folder_name'] == folder_name]
        
        if not folder_mappings:
            print(f"Warning: No mapping found for folder {folder_name}")
            return df
        
        # 创建代码映射字典
        code_mapping = {m['original_name']: m['new_name'] for m in folder_mappings}
        
        # 重命名代码
        if 'CODE' in df.columns:
            df['CODE'] = df['CODE'].replace(code_mapping)
        else:
            print(f"Warning: No 'CODE' column found in DataFrame")
        return df

    def raw_M1M2_wind_withdraw(self):
        inputpath=glv.get('input_m1m2_wind')
        available_date2=gt.intdate_transfer(self.available_date)
        inputpath=gt.file_withdraw(inputpath,available_date2)
        df=gt.readcsv(inputpath)
        df=self.rename_code_by_folder_wind(df,'M1M2')
        return df
    def raw_shibor_wind_withdraw(self):
        inputpath=glv.get('input_shibor_wind')
        available_date2=gt.intdate_transfer(self.available_date)
        inputpath=gt.file_withdraw(inputpath,available_date2)
        df=gt.readcsv(inputpath)
        df=self.rename_code_by_folder_wind(df,'Shibor')
        return df
    def raw_CB_wind_withdraw(self):
        inputpath=glv.get('input_CB_wind')
        available_date2=gt.intdate_transfer(self.available_date)
        inputpath=gt.file_withdraw(inputpath,available_date2)
        df=gt.readcsv(inputpath)
        df=self.rename_code_by_folder_wind(df,'ChinaGovernmentBonds')
        return df
    def raw_CDB_wind_withdraw(self):
        inputpath=glv.get('input_CDB_wind')
        available_date2=gt.intdate_transfer(self.available_date)
        inputpath=gt.file_withdraw(inputpath,available_date2)
        df=gt.readcsv(inputpath)
        df=self.rename_code_by_folder_wind(df,'ChinaDevelopmentBankBonds')
        return df
    def raw_CMN_wind_withdraw(self):
        inputpath=glv.get('input_CMN_wind')
        available_date2=gt.intdate_transfer(self.available_date)
        inputpath=gt.file_withdraw(inputpath,available_date2)
        df=gt.readcsv(inputpath)
        df=self.rename_code_by_folder_wind(df,'ChinaMediumTermNotes')
        return df
    def raw_cpi_wind_withdraw(self):
        inputpath=glv.get('input_cpi_wind')
        available_date2=gt.intdate_transfer(self.available_date)
        inputpath=gt.file_withdraw(inputpath,available_date2)
        df=gt.readcsv(inputpath)
        df=self.rename_code_by_folder_wind(df,'CPI')
        return df
    def raw_ppi_wind_withdraw(self):
        inputpath=glv.get('input_ppi_wind')
        available_date2=gt.intdate_transfer(self.available_date)
        inputpath=gt.file_withdraw(inputpath,available_date2)
        df=gt.readcsv(inputpath)
        df=self.rename_code_by_folder_wind(df,'PPI')
        return df
    def raw_pmi_wind_withdraw(self):
        inputpath=glv.get('input_pmi_wind')
        available_date2=gt.intdate_transfer(self.available_date)
        inputpath=gt.file_withdraw(inputpath,available_date2)
        df=gt.readcsv(inputpath)
        df=self.rename_code_by_folder_wind(df,'PMI')
        return df
    def raw_socialFinance_wind_withdraw(self):
        inputpath=glv.get('input_SocialFinance_wind')
        available_date2=gt.intdate_transfer(self.available_date)
        inputpath=gt.file_withdraw(inputpath,available_date2)
        df=gt.readcsv(inputpath)
        df=self.rename_code_by_folder_wind(df,'SocialFinance')
        return df
    def raw_LargeOrderInflow_wind_withdraw(self):
        inputpath=glv.get('input_LargeOrderInflow_wind')
        available_date2=gt.intdate_transfer(self.available_date)
        inputpath=gt.file_withdraw(inputpath,available_date2)
        df=gt.readcsv(inputpath)
        df=self.rename_code_by_folder_wind(df,'LargeOrderInflow')
        return df
    def raw_USD_wind_withdraw(self):
        inputpath=glv.get('input_USD_wind')
        available_date2=gt.intdate_transfer(self.available_date)
        inputpath=gt.file_withdraw(inputpath,available_date2)
        df=gt.readcsv(inputpath)
        df=self.rename_code_by_folder_wind(df,'UsDollar')
        return df
    def raw_intIndex_tushare_withdraw(self):
        inputpath=glv.get('input_internationalIndex_tushare')
        available_date2=gt.intdate_transfer(self.available_date)
        inputpath=gt.file_withdraw(inputpath,available_date2)
        df=gt.readcsv(inputpath)
        return df
    def raw_indexScore_withdraw(self):
        inputpath=glv.get('output_score')
        inputpath=os.path.join(inputpath,'a3')
        available_date2 = gt.intdate_transfer(self.available_date)
        inputpath = gt.file_withdraw(inputpath, available_date2)
        df = gt.readcsv(inputpath)
        return df
if __name__ == '__main__':
    mdp=macroData_preparing('2025-05-08')
    mdp.raw_M1M2_wind_withdraw()

