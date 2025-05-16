import os
import sys
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt
from MacroData_update.Macrodata_update import MacroData_update

def MacroData_update_main(start_date,end_date,is_sql):
    MDU=MacroData_update(start_date,end_date,is_sql)
    MDU.Shibor_data_update()
    MDU.M1M2_data_update()
    MDU.CB_data_update()
    MDU.CDB_data_update()
    MDU.CMN_data_update()
    MDU.CPI_data_update()
    MDU.PPI_data_update()
    MDU.PMI_data_update()
    MDU.SocialFinance_data_update()
    MDU.LargeOrderInflow_data_update()
    MDU.USD_data_update()
    MDU.USindex_data_update()
    MDU.IndexScore_data_update()
def MacroData_update_main2(start_date,end_date,is_sql):
    MDU=MacroData_update(start_date,end_date,is_sql)
    MDU.Shibor_data_update()
    MDU.M1M2_data_update()
    MDU.CB_data_update()
    MDU.CDB_data_update()
    MDU.CMN_data_update()
    MDU.CPI_data_update()
    MDU.PPI_data_update()
    MDU.PMI_data_update()
    MDU.SocialFinance_data_update()
    MDU.LargeOrderInflow_data_update()
    MDU.USD_data_update()
    MDU.USindex_data_update()
    MDU.IndexScore_data_update()
if __name__ == '__main__':
    MacroData_update_main('2025-01-01', '2025-05-10',True)