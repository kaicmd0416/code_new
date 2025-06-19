import pandas as pd
import os
import sys
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt
import datetime
from datetime import date
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
def renrHolding_check():
    target_date=target_date_decision()
    target_date=gt.intdate_transfer(target_date)
    inputpath = 'D:\Trading_data\\trading_order\仁睿'
    inputpath2 = 'D:\Trading_data_test\\trading_order\仁睿价值精选1号'
    inputpath = os.path.join(inputpath, 'renr_'+str(target_date)+'_trading_list.csv')
    inputpath2 = os.path.join(inputpath2, '仁睿价值精选1号_'+str(target_date)+'_trading_list.csv')
    df=gt.readcsv(inputpath)
    df2=gt.readcsv(inputpath2)
    df2 = df2[['代码', '数量', '方向']]
    df2.columns = ['代码', 'quantity_prod', '方向_prod']
    df = df.merge(df2, on='代码', how='outer')
    df.fillna(0, inplace=True)
    df['difference_quantity'] = abs(df['数量'].astype(float) - df['quantity_prod'].astype(float))
    df['difference_direction']=abs(df['方向'].astype(float) - df['方向_prod'].astype(float))
    df = df[(df['difference_quantity'] != 0)|(df['difference_direction'])!=0]
    print('任瑞trading差异')
    print(df)
def xyHolding_check():
    target_date=target_date_decision()
    target_date=gt.intdate_transfer(target_date)
    inputpath = 'D:\Trading_data\\trading_order\宣夜'
    inputpath2 = 'D:\Trading_data_test\\trading_order\宣夜惠盈1号'
    inputpath = os.path.join(inputpath, 'xy_'+str(target_date)+'_trading_list.csv')
    inputpath2 = os.path.join(inputpath2, '宣夜惠盈1号_'+str(target_date)+'_trading_list.csv')
    df = pd.read_csv(inputpath, header=None)
    df2 = pd.read_csv(inputpath2, header=None)
    df = df.loc[7:]
    df2 = df2.loc[7:]
    df = df[[1, 2, 3]]
    df2 = df2[[1, 2, 3]]
    df.columns = ['代码', '数量', '方向']
    df2.columns = ['代码', '数量', '方向']
    def direction_decision(x):
        if x=='买入':
            return 0
        else:
            return 1
    df['方向']=df['方向'].apply(lambda x: direction_decision(x))
    df2['方向']=df2['方向'].apply(lambda x: direction_decision(x))
    df2 = df2[['代码', '数量', '方向']]
    df2.columns = ['代码', 'quantity_prod', '方向_prod']
    df = df.merge(df2, on='代码', how='outer')
    df.fillna(0, inplace=True)
    df['difference_quantity'] = abs(df['数量'].astype(float) - df['quantity_prod'].astype(float))
    df['difference_direction']=abs(df['方向'].astype(float) - df['方向_prod'].astype(float))
    df = df[(df['difference_quantity'] != 0)|(df['difference_direction']!=0)]
    print('宣夜trading差异')
    print(df)
def Holding_checking_main():
    renrHolding_check()
    xyHolding_check()