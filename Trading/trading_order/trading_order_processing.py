import os
import pandas as pd
import global_setting.global_dic as glv
import sys
from datetime import datetime
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt
global source,config_path
from datetime import date
import datetime
source=glv.get('source')
config_path=glv.get('config_path')
from data_prepared import data_prepared
from trading_order.trading_order_xuanye import trading_xuanye
from trading_order.trading_order_renrui import trading_renrui
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
def nontrading_list_getting(product_code):
    inputpath=glv.get('config_trading')
    df=pd.read_excel(inputpath,sheet_name=product_code)
    if len(df)>0:
        df=gt.code_transfer(df)
    nontrading_list=df['code'].tolist()
    return nontrading_list
def trading_xy_main(trading_mode,is_realtime):
    # to_mode选v1是跃然的t0 选v2是景泰的t0
    # trading_mode选v1是twap 选v2是vwap
    target_time=target_date_decision()
    dp=data_prepared(target_time,is_realtime)
    stock_money=dp.productInfo_withdraw('SGS958')
    df_mkt,etf_pool=dp.mktData_withdraw()
    df_weight=dp.productTargetWeight_withdraw('SGS958')
    df_holding=dp.productHolding_withdraw('SGS958')
    nontrading_code_list=nontrading_list_getting('SGS958')
    trading_time=dp.tradingTime_withdraw('SGS958')

    if len(nontrading_code_list)>0:
         df_nontrading=df_holding[df_holding['code'].isin(nontrading_code_list)]
         df_nontrading=df_nontrading.merge(df_mkt,on='code',how='left')
         df_nontrading['mkt_value']=df_nontrading['holding']*df_nontrading['close']
         lock_money=df_nontrading['mkt_value'].sum()
         df_holding=df_holding[~(df_holding['code'].isin(nontrading_code_list))]
         df_weight = df_weight[~(df_weight['code'].isin(nontrading_code_list))]
         df_weight['weight']=df_weight['weight']/df_weight['weight'].sum()
         stock_money=stock_money-lock_money
    trading_xy=trading_xuanye(df_weight,df_holding,df_mkt,target_time,stock_money,etf_pool,trading_time)
    trading_xy.trading_order_xy_main(trading_mode)
def trading_rr_main(is_realtime):
    target_date = target_date_decision()
    target_date2=gt.intdate_transfer(target_date)
    target_time = target_date_decision()
    dp = data_prepared(target_time, is_realtime)
    stock_money = dp.productInfo_withdraw('SLA626')
    df_mkt, etf_pool = dp.mktData_withdraw()
    df_weight = dp.productTargetWeight_withdraw('SLA626')
    df_holding = dp.productHolding_withdraw('SLA626')
    nontrading_code_list = nontrading_list_getting('SLA626')
    if len(nontrading_code_list) > 0:
        df_nontrading = df_holding[df_holding['code'].isin(nontrading_code_list)]
        df_nontrading = df_nontrading.merge(df_mkt, on='code', how='left')
        df_nontrading['mkt_value'] = df_nontrading['holding'] * df_nontrading['close']
        lock_money = df_nontrading['mkt_value'].sum()
        df_holding = df_holding[~(df_holding['code'].isin(nontrading_code_list))]
        df_weight = df_weight[~(df_weight['code'].isin(nontrading_code_list))]
        df_weight['weight'] = df_weight['weight'] / df_weight['weight'].sum()
        stock_money = stock_money - lock_money
    trading_renr=trading_renrui(df_weight,df_holding,df_mkt,target_date,stock_money)
    trading_renr.trading_order_renrui()
