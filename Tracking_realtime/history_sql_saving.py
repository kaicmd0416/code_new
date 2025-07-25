import pandas as pd
import os
import sys
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import datetime
import global_tools as gt
import global_setting.global_dic as glv
import numpy as np
def sql_path():
    yaml_path = os.path.join(os.path.dirname(__file__), 'project_config', 'trackingrealtime_sql.yaml')
    return yaml_path
global inputpath_sql,config_path
config_path=glv.get('path_config')
inputpath_sql=sql_path()
class historySql_saving:
    def __init__(self):
        today = datetime.date.today()
        self.date = gt.strdate_transfer(today)
        self.now = datetime.datetime.now().replace(tzinfo=None)
    def foHolding_saving(self):
        inputpath = f"Select * from realtime_futureoptionholding Where valuation_date='{self.date}'"
        df_final = gt.data_getting(inputpath, config_path)
        if len(df_final) > 0:
            df_final['update_time']=self.now
            sm = gt.sqlSaving_main(inputpath_sql, 'optionfuture_holding_his', delete=True)
            sm.df_to_sql(df_final)
        return df_final
    def foHolding_check(self):
        inputpath = f"Select * from history_futureoptionholding Where valuation_date='{self.date}'"
        df_final = gt.data_getting(inputpath, config_path)
        if len(df_final)>0:
            status='exist'
        else:
            status='not_exist'
        return status
    def portfolioreturn_saving(self):
        inputpath = f"Select * from realtime_portfolioreturn Where valuation_date='{self.date}'"
        df_final = gt.data_getting(inputpath, config_path)
        if len(df_final) > 0:
            df_final['update_time'] = self.now
            sm = gt.sqlSaving_main(inputpath_sql, 'portfolioreturn_his', delete=True)
            sm.df_to_sql(df_final)
        return df_final
    def portfolioreturn_check(self):
        inputpath = f"Select * from history_portfolioreturn Where valuation_date='{self.date}'"
        df_final = gt.data_getting(inputpath, config_path)
        if len(df_final)>0:
            status='exist'
        else:
            status='not_exist'
        return status
    def productreturn_saving(self):
        inputpath = f"Select * from realtime_productstockreturn Where valuation_date='{self.date}'"
        df_final = gt.data_getting(inputpath, config_path)
        if len(df_final) > 0:
            df_final['update_time'] = self.now
            sm = gt.sqlSaving_main(inputpath_sql, 'productreturn_his', delete=True)
            sm.df_to_sql(df_final)
        return df_final
    def productreturn_check(self):
        inputpath = f"Select * from history_productstockreturn Where valuation_date='{self.date}'"
        df_final = gt.data_getting(inputpath, config_path)
        if len(df_final)>0:
            status='exist'
        else:
            status='not_exist'
        return status
    def proinfo_saving(self):
        inputpath = f"Select * from realtime_proinfo Where valuation_date='{self.date}'"
        df_final = gt.data_getting(inputpath, config_path)
        if len(df_final) > 0:
            df_final['update_time'] = self.now
            sm = gt.sqlSaving_main(inputpath_sql, 'proinfo_his', delete=True)
            sm.df_to_sql(df_final)
        return df_final
    def proinfo_check(self):
        inputpath = f"Select * from history_proinfo Where valuation_date='{self.date}'"
        df_final = gt.data_getting(inputpath, config_path)
        if len(df_final)>0:
            status='exist'
        else:
            status='not_exist'
        return status
    def holdingchanging_saving(self):
        inputpath = f"Select * from realtime_holdingchanging Where valuation_date='{self.date}'"
        df_final = gt.data_getting(inputpath, config_path)
        if len(df_final) > 0:
            df_final['update_time'] = self.now
            sm = gt.sqlSaving_main(inputpath_sql, 'holding_changing_his', delete=True)
            sm.df_to_sql(df_final)
        return df_final
    def holdingchanging_check(self):
        inputpath = f"Select * from history_holdingchanging Where valuation_date='{self.date}'"
        df_final = gt.data_getting(inputpath, config_path)
        if len(df_final)>0:
            status='exist'
        else:
            status='not_exist'
        return status
    def historySql_main(self):
        status1=self.foHolding_check()
        if status1=='not_exist':
            self.foHolding_saving()
        status2 = self.portfolioreturn_check()
        if status2 == 'not_exist':
            self.portfolioreturn_saving()
        status3 = self.productreturn_check()
        if status3 == 'not_exist':
            self.productreturn_saving()
        status4 = self.proinfo_check()
        if status4 == 'not_exist':
            self.proinfo_saving()
        status5 = self.holdingchanging_check()
        if status5 == 'not_exist':
            self.holdingchanging_saving()

if __name__ == '__main__':
    hs=historySql_saving()
    hs.foHolding_check()