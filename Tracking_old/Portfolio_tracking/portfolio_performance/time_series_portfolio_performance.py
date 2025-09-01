import sys
import os
import pandas as pd
import Portfolio_tracking.global_setting.global_dic as glv
import global_tools_func.global_tools as gt
from Portfolio_tracking.portfolio_performance.cross_section_portfolio_performance import cross_section_portfolio_performance,crossSection_stock_return_withdraw,crossSection_index_return_withdraw,portfolio_weight_withdraw
from Portfolio_tracking.portfolio_performance.portfolio_weight_withdraw import portfolio_config_withdraw,file_name_withdraw
def specific_portfolio_history_return_calulate(start_date,end_date,score_name,index_type):#特定portfolio,某一个时间段内的return,后续可以把换手率加上
    outputpath=glv.get('portfolio_history')
    start_date2=gt.intdate_transfer(start_date)
    end_date2=gt.intdate_transfer(end_date)
    outputpath=os.path.join(outputpath,score_name+'_'+start_date2+'_'+end_date2+'.xlsx')
    date_list = gt.working_days_list(start_date, end_date)
    return_list = []
    excess_return_list = []
    df_final=pd.DataFrame()
    for date in date_list:
        print(date)
        df_stock_return=crossSection_stock_return_withdraw(date)
        df_index_return = crossSection_index_return_withdraw(date)
        portfolio_return, excess_portfolio_return = cross_section_portfolio_performance(score_name,date,index_type,df_stock_return,df_index_return)
        excess_return_list.append(excess_portfolio_return)
        return_list.append(portfolio_return)
    df_final['valuation_date']=date_list
    df_final['return']=return_list
    df_final['excess_return']=excess_return_list
    df_final['net_value']=(1+df_final['return']).cumprod()
    df_final['excess_net_value'] = (1 + df_final['excess_return']).cumprod()
    df_final.to_excel(outputpath,index=False)
def specific_portfolio_weight_analyse(start_date,end_date,score_name):
    date_list = gt.working_days_list(start_date, end_date)
    for target_date in date_list:
          df_weight=portfolio_weight_withdraw(score_name,target_date,yesterday=False)
          df_weight.sort_values(by='weight',ascending=False,inplace=True)
          df_weight.reset_index(inplace=True,drop=True)
          slice_df=df_weight.iloc[:5]
          print(target_date)
          print(slice_df)

def time_series_portfolio_performance_update():
    df_config = portfolio_config_withdraw()
    index_type_list = df_config['index_type'].unique().tolist()
    inputpath=glv.get('cross_section_output')
    inputpath_return=os.path.join(inputpath,'return')
    inputpath_excess_return=os.path.join(inputpath,'excess_return')
    for index_type in index_type_list:
        portfolio_name_list = df_config[df_config['index_type'] == index_type]['score_name'].tolist()
        portfolio_name_list.sort()
        index_short_name=file_name_withdraw(index_type)
        inputpath_return2=os.path.join(inputpath_return,index_short_name)
        inputpath_excess_return2=os.path.join(inputpath_excess_return,index_short_name)
        input_list_return = os.listdir(inputpath_return2)
        input_list_excess_return = os.listdir(inputpath_excess_return2)
        input_list_return.sort()
        input_list_excess_return.sort()
        available_date_1 = str(input_list_return[-1])[-12:-4]
        available_date_2 = str(input_list_excess_return[-1])[-12:-4]
        available_date_1 = gt.strdate_transfer(available_date_1)
        available_date_2 = gt.strdate_transfer(available_date_2)
        outputpath = glv.get('time_series_output')
        outputpath_return=os.path.join(outputpath,'return')
        gt.folder_creator2(outputpath_return)
        outputpath_excess_return=os.path.join(outputpath,'excess_return')
        gt.folder_creator2(outputpath_excess_return)
        outputpath_return = os.path.join(outputpath_return, str(index_short_name)+'_portfolioes_return.xlsx')
        outputpath_excess_return = os.path.join(outputpath_excess_return, str(index_short_name)+'_portfolioes_excess_return.xlsx')
        try:
            df_output_return = pd.read_excel(outputpath_return)
            df_output_excess_return = pd.read_excel(outputpath_excess_return)
        except:
            df_output_return = pd.DataFrame(columns=['valuation_date']+portfolio_name_list)
            df_output_excess_return = pd.DataFrame(columns=['valuation_date']+portfolio_name_list)
        if len(df_output_return) == 0 and len(df_output_excess_return) == 0:
            end_date_return = str(input_list_return[0])[-12:-4]
            end_date_excess_return = str(input_list_excess_return[0])[-12:-4]
            end_date_return = gt.strdate_transfer(end_date_return)
            end_date_excess_return = gt.strdate_transfer(end_date_excess_return)
        else:
            end_date_return = df_output_return['valuation_date'].tolist()[-1]
            end_date_excess_return = df_output_excess_return['valuation_date'].tolist()[-1]
        if end_date_return != end_date_excess_return:
            print('两个文件结束日期不一致')
            raise ValueError
        if available_date_1 == available_date_2 != end_date_excess_return:
            time_list = gt.working_days_list(end_date_excess_return, available_date_1)
            for date in time_list:
                print(date)
                date2 = gt.intdate_transfer(date)
                daily_inputpath_return = gt.file_withdraw(inputpath_return2, date2)
                daily_inputpath_excess_return = gt.file_withdraw(inputpath_excess_return2, date2)
                daily_df_return = gt.readcsv(daily_inputpath_return)
                daily_df_excess_return = gt.readcsv(daily_inputpath_excess_return)
                df_output_return = pd.concat([df_output_return, daily_df_return])
                df_output_excess_return = pd.concat([df_output_excess_return, daily_df_excess_return])
            df_output_return.to_excel(outputpath_return, index=False)
            df_output_excess_return.to_excel(outputpath_excess_return, index=False)
        else:
            print('已经更新到最新日期:' + available_date_1)