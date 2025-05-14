import pandas as pd
import os
import Optimizer_python.global_setting.global_dic as glv
import sys
import global_tools_func.global_tools as gt
def component_bu(df_score,index_type,df_hs300,df_zz500,df_zz1000,df_zz2000,df_zzA500):
    if index_type=='沪深300':
        quantile_1=0.4
        df_component=df_hs300
        critical_number=130
    elif index_type=='中证500':
        quantile_1=0.25
        df_component=df_zz500
        critical_number=300
    elif index_type=='中证A500':
        quantile_1 = 0.15
        critical_number=215
        df_component=df_zzA500
    elif index_type=='中证1000':
        quantile_1=0
        df_component=df_zz1000
        critical_number = 600
    elif index_type=='中证2000':
        quantile_1=0
        df_component=df_zz2000
    else:
        raise ValueError
    available_date=df_score['valuation_date'].tolist()[-1]
    code_list_1 = df_score['code'].tolist()
    code_list_2 = df_component['code'].tolist()
    code_list_missing = list(set(code_list_2) - set(code_list_1))
    if len(code_list_missing)>critical_number and len(code_list_missing)-critical_number>20 and index_type!='中证500':
        print(len(code_list_missing),critical_number)
        print('请检查rr_score，rr_score样本量过短')
        raise ValueError
    quantile_score = df_score['final_score'].quantile(quantile_1)
    slice_df_bu = pd.DataFrame()
    slice_df_bu['code'] = code_list_missing
    slice_df_bu['valuation_date'] = available_date
    slice_df_bu['final_score'] = quantile_score
    daily_df = pd.concat([df_score, slice_df_bu])
    daily_df['final_score'] = (daily_df['final_score'] - daily_df['final_score'].mean()) / daily_df['final_score'].std()
    daily_df.sort_values(by='final_score', ascending=False, inplace=True)
    return daily_df
def basic_score_withdraw(score_type,available_date):
    available_date2=gt.intdate_transfer(available_date)
    inputpath_score=glv.get('input_score')
    inputpath_score2=os.path.join(inputpath_score,score_type)
    inputpath_score2=gt.file_withdraw(inputpath_score2,available_date2)
    df_score=gt.readcsv(inputpath_score2)
    df_score['final_score']=(df_score['final_score']-df_score['final_score'].mean())/df_score['final_score'].std()
    return df_score
def score_zz800_stockpool_processing(df_score,df_hs300,df_zz500):
    code_list_hs300=df_hs300['code'].tolist()
    code_list_zz500=df_zz500['code'].tolist()
    code_list_final=list(set(code_list_hs300)|set(code_list_zz500))
    df_score=df_score[df_score['code'].isin(code_list_final)]
    df_score['final_score'] = (df_score['final_score'] - df_score['final_score'].mean()) / df_score['final_score'].std()
    return df_score
def score_zz1800_stockpool_processing(df_score,df_hs300,df_zz500,df_zz1000):
    code_list_hs300=df_hs300['code'].tolist()
    code_list_zz500=df_zz500['code'].tolist()
    code_list_zz1000=df_zz1000['code'].tolist()
    code_list_final=list(set(code_list_hs300)|set(code_list_zz500)|set(code_list_zz1000))
    df_score=df_score[df_score['code'].isin(code_list_final)]
    df_score['final_score'] = (df_score['final_score'] - df_score['final_score'].mean()) / df_score['final_score'].std()
    return df_score
def score_zz3800_stockpool_processing(df_score,df_hs300,df_zz500,df_zz1000,df_zz2000):
    code_list_hs300=df_hs300['code'].tolist()
    code_list_zz500=df_zz500['code'].tolist()
    code_list_zz1000=df_zz1000['code'].tolist()
    code_list_zz2000=df_zz2000['code'].tolist()
    code_list_final=list(set(code_list_hs300)|set(code_list_zz500)|set(code_list_zz1000)|set(code_list_zz2000))
    df_score=df_score[df_score['code'].isin(code_list_final)]
    df_score['final_score'] = (df_score['final_score'] - df_score['final_score'].mean()) / df_score['final_score'].std()
    return df_score
def score_withdraw_main(score_type,available_date,mode_type,index_type,df_hs300,df_zz500,df_zz1000,df_zz2000,df_zzA500):
    df_score=basic_score_withdraw(score_type,available_date)
    if mode_type=='mode_v1':
        df_final=df_score
        #if str(score_type)[:2]=='rr':
            #f_final=component_bu(df_score, index_type, df_hs300, df_zz500, df_zz1000, df_zz2000, df_zzA500)
    elif mode_type=='mode_v2':
        if len(df_hs300)==0 or len(df_zz500)==0:
            print('权重股数据缺失')
            raise ValueError
        else:
            df_final=score_zz800_stockpool_processing(df_score,df_hs300,df_zz500)
    elif mode_type=='mode_v3':
        if len(df_hs300)==0 or len(df_zz500)==0 or len(df_zz1000)==0:
            print('权重股数据缺失')
            raise ValueError
        else:
            df_final=score_zz1800_stockpool_processing(df_score,df_hs300,df_zz500,df_zz1000)
    elif mode_type=='mode_v4':
        if len(df_hs300)==0 or len(df_zz500)==0 or len(df_zz1000)==0 or len(df_zz2000)==0:
            print('权重股数据缺失')
            raise ValueError
        else:
            df_final=score_zz3800_stockpool_processing(df_score,df_hs300,df_zz500,df_zz1000,df_zz2000)
    else:
        print('there is no mode type: '+str(mode_type))
        raise ValueError
    return df_final
