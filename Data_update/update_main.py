import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
from FactorData_update.factor_update import FactorData_update
from MktData_update.MktData_update_main import MktData_update_main,CBData_update_main
from Score_update.score_update_main import score_update_main
from File_moving.File_moving import File_moving
from Time_tools.time_tools import time_tools
from Data_checking.data_check import DataCheck
import global_tools as gt
from L4Data_update.L4_running_main import L4_update_main
from vix.vix_calculation import VIX_calculation_main
from MacroData_update.MacroData_upate_main import MacroData_update_main
from TimeSeries_update.time_series_data_update import timeSeries_data_update
def MarketData_update_main():
    tt = time_tools()
    date = tt.target_date_decision_mkt()
    date = gt.strdate_transfer(date)
    tdu=timeSeries_data_update(date,date)
    MktData_update_main(date, date)
    MacroData_update_main(date, date)
    VIX_calculation_main(date, date, False)
    tdu.Mktdata_update_main()
    tdu.macrodata_update_main()
    CBData_update_main(date, date)
def ScoreData_update_main():
    tt = time_tools()
    date = tt.target_date_decision_score()
    date = gt.strdate_transfer(date)
    score_type = 'fm'
    score_update_main(score_type, date, date)
def FactorData_update_main():
    tt = time_tools()
    date = tt.target_date_decision_factor()
    date = gt.strdate_transfer(date)
    tdu = timeSeries_data_update(date, date)
    fu = FactorData_update(date, date)
    fu.FactorData_update_main()
    tdu.Factordata_update_main()
def L4Data_update_main():
    L4_update_main()
def daily_update_auto():
    fm=File_moving()
    fm.file_moving_update_main()
    MarketData_update_main()
    try:
       ScoreData_update_main()
    except:
        pass
    FactorData_update_main()
    L4_update_main()
    DC=DataCheck()
    DC.checking_crossSectiondata_main()
    DC.checking_timeseriesdata_main()
if __name__ == '__main__':
    daily_update_auto()