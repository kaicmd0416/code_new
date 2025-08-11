import pandas as pd
import os
import sys
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import datetime
import global_tools as gt
import global_setting.global_dic as glv
from calculate_main.product_calculate import product_tracking
from calculate_main.portfolio_calculate import portfolio_tracking
from history_sql_saving import historySql_saving
config_path=glv.get('config_path')
def tracking_realtime_main():
    if gt.is_workday_auto()==True:
        time = datetime.datetime.now()
        if time.hour == 9 and time.minute < 30:
            gt.table_manager(config_path,'tracking_realtime', 'realtime_futureoptionholding')
            gt.table_manager(config_path, 'tracking_realtime', 'realtime_holdingchanging')
            gt.table_manager(config_path, 'tracking_realtime', 'realtime_portfolioreturn')
            gt.table_manager(config_path, 'tracking_realtime', 'realtime_proinfo')
        pt = portfolio_tracking()
        pt.portfolioTracking_main()
        product_list=['SGS958','SLA626','SNY426','SSS044','SVU353','STH580','SST132']
        for product_code in product_list:
            try:
                pt2 = product_tracking(product_code)
                pt2.productTracking_main()
            except:
                print(f"{product_code}更新有误")
        if time.hour >= 16:
            hs = historySql_saving()
            hs.historySql_main()
tracking_realtime_main()