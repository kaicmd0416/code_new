import os
import pandas as pd
import numpy as np
# import chinese_calendar as cal
from datetime import datetime, timedelta, date
import global_tools_func.global_tools as gt
from Trading.global_setting import global_dic as glv
from Trading.trading_weight.trading_weight_selecting import product_target_weight_withdraw,target_date_decision
from Trading.trading_order.trading_order_xuanye import trading_order_xy_main,t0_trading_xy_main
from Trading.trading_order.trading_order_renrui import trading_order_renrui
def realtime_data_checking(df):
    date=df['日期'].unique().tolist()
    date = [i for i in date if len(str(i)) > 7]
    if len(date)>1:
        print('realtime_data_checking:日期存在多个值，请检查')
        print(date)
        raise ValueError
    time=df['时间'].unique().tolist()
    time=[i for i in time if len(str(i))>7]
    if len(time) > 1:
        first_time = datetime.strptime(time[0], "%H:%M:%S")
        for t in time[1:]:
            current_time = datetime.strptime(t, "%H:%M:%S")
            time_diff = abs((current_time - first_time).total_seconds() / 60)
            if time_diff > 5:
                print('realtime_data_checking:时间列表中存在相差超过5分钟的值，请检查')
                print(f'第一个时间: {time[0]}, 相差{time_diff:.2f}分钟的时间: {t}')
                problem_codes = df[df['时间'] == t]['code'].tolist()
                print(f'对应的股票代码: {problem_codes}')
    now=datetime.now()
    # Combine date and time and calculate time difference
    data_datetime = datetime.strptime(f"{date[0]} {time[0]}", "%Y%m%d %H:%M:%S")
    time_diff = abs((now - data_datetime).total_seconds() / 60)  # Convert to minutes
    
    if time_diff > 10:
        print(f'realtime_data_checking:数据时间与当前时间相差{time_diff:.2f}分钟，超过10分钟，请检查')
        raise ValueError
    
def etf_close_withdraw():
    target_date = target_date_decision()
    inputpath=glv.get('portfolio_weight')
    inputpath=os.path.join(inputpath,'gms')
    target_date=gt.intdate_transfer(target_date)
    inputpath=gt.file_withdraw(inputpath,target_date)
    df=pd.read_csv(inputpath)
    df=df[['code','price']]
    return df
def stock_close_withdraw(df_today):
    target_date=target_date_decision()
    available_date = gt.last_workday_calculate(target_date)
    available_date=gt.intdate_transfer(available_date)
    inputpath2 = glv.get('stock_close')
    inputpath2=gt.file_withdraw(inputpath2,available_date)
    df_close=gt.readcsv(inputpath2)
    df_today2=df_today.copy()
    df_today2['new_code']=df_today2['code'].apply(lambda x: str(x)[-4:])
    df_today2=df_today2[~(df_today2['new_code']=='.ETF')]
    code_list=df_today2['code'].tolist()
    df_close=df_close[code_list]
    df_close = df_close.T
    df_close.reset_index(inplace=True)
    df_close.columns = ['code', 'close']
    df_etf= etf_close_withdraw()
    if df_close.isna().sum().tolist() !=[0,0]:
        print('Stock_data_close.csv数据存在Nan值，请查看')
        raise ValueError
    if len(df_etf)>0:
        df_etf.columns=['code','close']
    df_close=pd.concat([df_close,df_etf])
    return df_close

def realtime_etf_price_withdraw():
    inputpath=glv.get('realtime_data')
    df=pd.read_excel(inputpath,sheet_name='etf_info')
    df_etf=etf_close_withdraw()
    df_etf['code']=df_etf['code'].apply(lambda x: str(x)[:6])
    code_list=df_etf['code'].tolist()
    df['代码']=df['代码'].apply(lambda x: str(x)[:6])
    df.rename(columns={'代码':'code','现价':'close'},inplace=True)
    df=df[df['code'].isin(code_list)]
    realtime_data_checking(df)
    df_etf.drop(columns=['price'],inplace=True)
    df_etf=df_etf.merge(df,on='code',how='left')
    df_etf=df_etf[['code','close']]
    return df_etf
def realtime_stock_price_withdraw(df_today):
    inputpath=glv.get('realtime_data')
    df=pd.read_excel(inputpath,sheet_name='stockprice')
    df.rename(columns={'代码':'code','现价':'close'},inplace=True)
    df_today2=df_today.copy()
    df_today2['new_code']=df_today2['code'].apply(lambda x: str(x)[-4:])
    df_today2['code'] = df_today2['code'].apply(lambda x: str(x)[:6])
    df_today2=df_today2[~(df_today2['new_code']=='.ETF')]
    code_list=df_today2['code'].tolist()
    df['code']=df['code'].apply(lambda x: str(x)[:6])
    df=df[df['code'].isin(code_list)]
    realtime_data_checking(df)
    df_today2=df_today2.merge(df,on='code',how='left')
    df_today2=df_today2[['code','close']]
    df_etf=realtime_etf_price_withdraw()
    if len(df_etf)>0:
        df_etf.columns=['code','close']
    df_close=pd.concat([df_today2,df_etf])
    return df_close
def parameter_getting(product_name):
    inputpath1=glv.get('input_product')
    df = pd.read_excel(inputpath1,sheet_name='info_sheet')
    df2=df.copy()
    df=df[df['产品名称']==product_name]
    t0_money=df['t0_money'].tolist()[0]
    trading_time=df['trading_time'].tolist()[0]
    end_time=df['end_time'].tolist()[0]
    account_money=df['account_money'].tolist()[0]
    if account_money==None or np.isnan(account_money)==True:
        account_money=accountmoney_getting(product_name)
    df_nontrading=pd.read_excel(inputpath1,sheet_name=product_name)
    if len(df_nontrading)>0:
        nontrading_code_list=df_nontrading['code'].tolist()
    else:
        nontrading_code_list=[]
    return account_money,t0_money,trading_time,end_time,nontrading_code_list,df2,df_nontrading
def accountmoney_getting(product_name):
    inputpath=glv.get('input_holding')
    inputpath=os.path.join(inputpath,product_name)
    inputpath = os.path.join(inputpath, 'stock')
    inputlist=os.listdir(inputpath)
    inputlist=[i for i in inputlist if 'Account' in i]
    inputlist.sort()
    inputname=inputlist[-1]
    inputpath=os.path.join(inputpath,inputname)
    df=gt.readcsv(inputpath)
    account_money=df['总资产'].tolist()[0]
    return account_money
def holding_withdraw(product_name):
    inputpath = glv.get('input_holding')
    inputpath = os.path.join(inputpath, product_name)
    inputpath=os.path.join(inputpath,'stock')
    inputlist = os.listdir(inputpath)
    inputlist = [i for i in inputlist if 'PositionDetail' in i or 'PositionStatics' in i]
    inputlist.sort()
    inputname = inputlist[-1]
    inputpath = os.path.join(inputpath, inputname)
    df = gt.readcsv(inputpath)
    df['证券代码'] = df['证券代码'].astype(int)
    df['证券代码'] = df['证券代码'].apply(lambda x: '{:06d}'.format(x))
    df['市场缩写']='.SZ'
    df.loc[df['市场名称']=='上证所',['市场缩写']]='.SH'
    df['code']=df['证券代码'].astype(str)+df['市场缩写'].astype(str)
    df_holding=df[['code','当前拥股']]
    df_price=df[['code','最新价']]
    df_holding.columns=['StockCode','HoldingQty']
    df_price.columns=['code','price']
    df_price=None
    return df_holding,df_price
def holding_withdraw2(product_name):
    target_date=target_date_decision()
    available_date=gt.last_workday_calculate(target_date)
    available_date2=gt.intdate_transfer(available_date)
    inputpath_holding=glv.get('input_holding')
    inputpath_yes=os.path.join(inputpath_holding,product_name)
    inputpath_yes=gt.file_withdraw(inputpath_yes,available_date2)
    df_yes =gt.readcsv(inputpath_yes)
    return df_yes
def trading_xy_main(trading_mode,t0_mode,is_realtime):
    # to_mode选v1是跃然的t0 选v2是景泰的t0
    # trading_mode选v1是twap 选v2是vwap
    product_name='宣夜惠盈1号'
    target_time=target_date_decision()
    account_money,t0_money,trading_time,end_time,nontrading_code_list,df,df_nontrading = parameter_getting(product_name)
    stock_money = account_money - t0_money
    df_today= product_target_weight_withdraw(product_name, yesterday=False)
    df_yes,df_price = holding_withdraw(product_name)
    # df_today['code']=df_today['code'].apply(lambda x: str(x)[:6])
    if is_realtime==True:
        df_close=realtime_stock_price_withdraw(df_today)
    else:
        df_close = stock_close_withdraw(df_today)
    df_close['code'] = df_close['code'].apply(lambda x: str(x)[:6])
    df_yes['StockCode'] = df_yes['StockCode'].apply(lambda x: str(x)[:6])
    df_today['code'] = df_today['code'].apply(lambda x: str(x)[:6])
    df_close['code']=df_close['code'] = df_close['code'].astype(int)
    df_close['code'] = df_close['code'].apply(lambda x: '{:06d}'.format(x))
    df_yes['StockCode'] = df_yes['StockCode'] = df_yes['StockCode'].astype(int)
    df_yes['StockCode'] = df_yes['StockCode'].apply(lambda x: '{:06d}'.format(x))
    df_today['code'] = df_today['code'] = df_today['code'].astype(int)
    df_today['code'] = df_today['code'].apply(lambda x: '{:06d}'.format(x))
    if len(nontrading_code_list)>0:
         df_yes=df_yes[~(df_yes['code'].isin(nontrading_code_list))]
    df_today2=trading_order_xy_main(df_today, df_yes, df_close, target_time, stock_money, trading_time, product_name,
                          trading_mode)
    t0_trading_xy_main(df_yes, df_today2,target_time,t0_money,end_time,product_name,t0_mode)
    return df,df_nontrading
def trading_rr_main(is_realtime):
    target_date = target_date_decision()
    target_date2=gt.intdate_transfer(target_date)
    product_name = '仁睿价值精选1号'
    account_money,t0_money,trading_time,end_time,nontrading_code_list,df,df_nontrading = parameter_getting(product_name)
    df_today = product_target_weight_withdraw(product_name, yesterday=False)
    df_yes, df_price = holding_withdraw(product_name)
    # df_today['code']=df_today['code'].apply(lambda x: str(x)[:6])
    if is_realtime==True:
        df_close=realtime_stock_price_withdraw(df_today)
    else:
        df_close = stock_close_withdraw(df_today)
    df_today['code']=df_today['code'].apply(lambda x: str(x)[:6])
    df_close['code'] = df_close['code'].apply(lambda x: str(x)[:6])
    df_yes['StockCode'] = df_yes['StockCode'].apply(lambda x: str(x)[:6])
    df_yes['StockCode'] = df_yes['StockCode'] = df_yes['StockCode'].astype(int)
    df_yes['StockCode'] = df_yes['StockCode'].apply(lambda x: '{:06d}'.format(x))
    df_yes.columns=['code','当前拥股']
    if len(nontrading_code_list)>0:
         df_yes=df_yes[~(df_yes['code'].isin(nontrading_code_list))]
    df_final= trading_order_renrui(df_today,df_yes,df_close,target_date,account_money,product_name)
    return df,df_nontrading
