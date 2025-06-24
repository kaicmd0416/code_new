import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
from FactorData_update.factor_update import FactorData_update
from MktData_update.MktData_update_main import MktData_update_main,CBData_update_main
from Score_update.score_update_main import score_update_main
from File_moving.File_moving import File_moving
from Time_tools.time_tools import time_tools
from TimeSeries_update.time_series_data_update import timeSeries_data_update
import global_tools as gt
from L4Data_update.L4_running_main import L4_update_main
from vix.vix_calculation import VIX_calculation_main
from MacroData_update.MacroData_upate_main import MacroData_update_main
def MarketData_history_main(start_date,end_date,is_sql):
    MktData_update_main(start_date,end_date,is_sql)
    tdu = timeSeries_data_update(start_date, end_date)
    VIX_calculation_main(start_date,end_date, False,is_sql)
    tdu.Mktdata_update_main()
    tdu.macrodata_update_main()
    CBData_update_main(start_date,end_date,is_sql)
def ScoreData_history_main(start_date,end_date,is_sql):
    score_type = 'fm'
    score_update_main(score_type, start_date,end_date,is_sql)
def FactorData_history_main(start_date,end_date,is_sql):
    fu = FactorData_update(start_date,end_date,is_sql)
    tdu = timeSeries_data_update(start_date, end_date)
    fu.FactorData_update_main()
    tdu.Factordata_update_main()
def MacroData_history_main(start_date,end_date,is_sql):
    MacroData_update_main(start_date, end_date, is_sql)
if __name__ == '__main__':
    start_date='2020-01-01'
    end_date='2022-01-04'
    ScoreData_history_main(start_date, end_date,True)
