import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
from FactorData_update.factor_update import FactorData_update
from MktData_update.MktData_update_main import MktData_update_main,CBData_update_main
from Score_update.score_update_main import score_update_main
from File_moving.File_moving import File_moving
from Time_tools.time_tools import time_tools
from Data_checking.data_check import checking
import global_tools as gt
from L4Data_update.L4_running_main import L4_update_main
from vix.vix_calculation import VIX_calculation_main
from MacroData_update.MacroData_upate_main import MacroData_update_main
def MarketData_history_main(start_date,end_date):
    MktData_update_main(start_date,end_date)
    MacroData_update_main(start_date,end_date)
    VIX_calculation_main(start_date,end_date, False)
    CBData_update_main(start_date,end_date)
def ScoreData_history_main(start_date,end_date):
    score_type = 'fm'
    score_update_main(score_type, start_date,end_date)
def FactorData_history_main(start_date,end_date):
    fu = FactorData_update(start_date,end_date)
    fu.FactorData_update_main()

if __name__ == '__main__':
    start_date='2025-05-12'
    end_date='2025-05-12'
    FactorData_history_main(start_date, end_date)
