import sys
import os
import pandas as pd
import Portfolio_tracking.global_setting.global_dic as glv
import global_tools_func.global_tools as gt
from Portfolio_tracking.portfolio_performance.portfolio_weight_withdraw import portfolio_weight_withdraw,portfolio_config_withdraw,file_name_withdraw
def crossSection_stock_return_withdraw(target_date):
    target_date2 = gt.intdate_transfer(target_date)
    inputpath_stockreturn = glv.get('input_stockreturn')
    inputpath_stockreturn = gt.file_withdraw(inputpath_stockreturn, target_date2)
    df_stock = gt.readcsv(inputpath_stockreturn)
    df_stock.set_index('valuation_date', inplace=True)
    df_stock = df_stock.T
    df_stock.reset_index(inplace=True)
    df_stock.columns = ['code', 'return']
    return df_stock

def crossSection_index_return_withdraw(target_date):
    target_date2 = gt.intdate_transfer(target_date)
    inputpath_indexreturn = glv.get('input_indexreturn')
    file_name = gt.file_withdraw(inputpath_indexreturn, target_date2)
    inputpath_index = os.path.join(inputpath_indexreturn, file_name)
    df = gt.readcsv(inputpath_index, dtype=str)
    return df
def portfolio_performance_calculate(df,df_weight_yes,df_index_return,df_stock,index_type):
    df = df.merge(df_stock, on='code', how='left')
    df.fillna(0, inplace=True)
    df_weight_yes.rename(columns={'weight':'yes_weight'},inplace=True)
    df=df.merge(df_weight_yes,on='code',how='outer')
    df.fillna(0,inplace=True)
    df['turn_over']=abs(df['weight']-df['yes_weight'])
    df['cost'] = df['turn_over'] * 0.001
    df['return'] = df['return'] - df['cost']
    df['portfolio'] = df['return'] * df['weight']
    portfolio_return = df['portfolio'].sum()
    index_return=df_index_return[index_type].tolist()[0]
    excess_portfolio_return=float(portfolio_return)-float(index_return)
    return portfolio_return,excess_portfolio_return
def cross_section_portfolio_performance(score_name,target_date,index_type,df_stock_return,df_index_return):
    try:
        df_weight=portfolio_weight_withdraw(score_name,target_date,yesterday=False)
        df_weight_yes = portfolio_weight_withdraw(score_name, target_date, yesterday=True)
    except:
        df_weight=pd.DataFrame()
        df_weight_yes=pd.DataFrame()
    if len(df_weight)!=0 and len(df_weight_yes)!=0:
        portfolio_return,excess_portfolio_return=portfolio_performance_calculate(df_weight,df_weight_yes,df_index_return,df_stock_return,index_type)
    else:
        portfolio_return=None
        excess_portfolio_return=None
    return portfolio_return,excess_portfolio_return
def cross_portfolio_update_main(target_date):
    df_config = portfolio_config_withdraw()
    index_type_list=df_config['index_type'].unique().tolist()
    df_stock_return=crossSection_stock_return_withdraw(target_date)
    df_index_return=crossSection_index_return_withdraw(target_date)
    target_date2=gt.intdate_transfer(target_date)
    outputpath=glv.get('cross_section_output')
    outputpath_return=os.path.join(outputpath,'return')
    outputpath_excess_return = os.path.join(outputpath, 'excess_return')
    for index_type in index_type_list:
        outputpath_return2=os.path.join(outputpath_return,file_name_withdraw(index_type))
        outputpath_excess_return2=os.path.join(outputpath_excess_return,file_name_withdraw(index_type))
        gt.folder_creator2(outputpath_return2)
        gt.folder_creator2(outputpath_excess_return2)
        outputpath_return2=os.path.join(outputpath_return2,str(file_name_withdraw(index_type))+'_portfolioReturn_'+target_date2+'.csv')
        outputpath_excess_return2 = os.path.join(outputpath_excess_return2, str(file_name_withdraw(index_type))+'_portfolioExcessReturn_' + target_date2 + '.csv')
        df_portfolio_return = pd.DataFrame()
        df_excess_portfolio_return = pd.DataFrame()
        portfolio_return_list = []
        excess_portfolio_return_list = []
        portfolio_name_list=df_config[df_config['index_type']==index_type]['score_name'].tolist()
        portfolio_name_list.sort()
        for portfolio_name in portfolio_name_list:
            portfolio_return, excess_portfolio_return = cross_section_portfolio_performance(portfolio_name, target_date,index_type, df_stock_return,df_index_return)
            portfolio_return_list.append(portfolio_return)
            excess_portfolio_return_list.append(excess_portfolio_return)
        df_portfolio_return['score_name'] = portfolio_name_list
        df_portfolio_return['portfolio_return'] = portfolio_return_list
        df_excess_portfolio_return['score_name'] = portfolio_name_list
        df_excess_portfolio_return['excess_portfolio_return'] = excess_portfolio_return_list
        df_portfolio_return.set_index('score_name', inplace=True, drop=True)
        df_portfolio_return = df_portfolio_return.T
        df_excess_portfolio_return.set_index('score_name', inplace=True, drop=True)
        df_excess_portfolio_return = df_excess_portfolio_return.T
        df_excess_portfolio_return.reset_index(inplace=True, drop=True)
        df_portfolio_return.reset_index(inplace=True, drop=True)
        df_excess_portfolio_return['valuation_date'] = target_date
        df_portfolio_return['valuation_date'] = target_date
        df_portfolio_return = df_portfolio_return[['valuation_date'] + df_portfolio_return.columns.tolist()[:-1]]
        df_excess_portfolio_return = df_excess_portfolio_return[
                ['valuation_date'] + df_excess_portfolio_return.columns.tolist()[:-1]]
        # if df_portfolio_return.isna().any().any() or df_excess_portfolio_return.isna().any().any():
        #         print('portfolio中存在数据未更新')
        # else:
        df_portfolio_return.to_csv(outputpath_return2, index=False)
        df_excess_portfolio_return.to_csv(outputpath_excess_return2, index=False)






