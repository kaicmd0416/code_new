from trading_order.trading_order_processing import trading_xy_main,trading_rr_main
from portfolio_saving import portfolio_saving_main
import os
import pandas as pd
import global_setting.global_dic as glv
import sys
from datetime import date
import datetime
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt
global source,config_path
from datetime import date
source=glv.get('source')
config_path=glv.get('config_path')
def target_date_decision():
    if gt.is_workday2() == True:
        today = date.today()
        next_day = gt.next_workday_calculate(today)
        critical_time = '20:00'
        time_now = datetime.datetime.now().strftime("%H:%M")
        if time_now >= critical_time:
            return next_day
        else:
            today = gt.strdate_transfer(today)
            return today
    else:
        today = date.today()
        next_day = gt.next_workday_calculate(today)
        return next_day
def PortfolioSaving_main(is_realtime=False):
    target_date=target_date_decision()
    ps=portfolio_saving_main(target_date,is_realtime)
    ps.sqlSaving_main()
def TradingOder_main(is_realtime=False):
    trading_xy_main('v2',is_realtime)
    trading_rr_main(is_realtime)
if __name__ == '__main__':
    #PortfolioSaving_main()
    TradingOder_main()