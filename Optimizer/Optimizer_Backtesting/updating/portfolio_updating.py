import sys
from datetime import date
import datetime
import global_setting.global_dic as glv
import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt
import pandas as pd
from Optimizer_Backtesting.portfolio_checking.portfolio_checking import portfolio_checking,portfolio_Error_raising
import os
def history_config_withdraw():
    inputpath = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    inputpath = os.path.join(inputpath, 'config_history.xlsx')
    df = pd.read_excel(inputpath)
    return df
def score_name_withdraw(score_type):
    inputpath_mode_dic = glv.get('mode_dic')
    # inputpath = os.path.split(os.path.realpath(__file__))[0]
    # inputpath_mode_dic = os.path.join(os.path.dirname(inputpath), 'mode_dictionary.xlsx')
    df_mode_dic = pd.read_excel(inputpath_mode_dic)
    if len(score_type)==4:
         df_mode_dic['score_type']=df_mode_dic['score_name'].apply(lambda x: str(x)[:4])
    else:
        df_mode_dic['score_type'] = df_mode_dic['score_name'].apply(lambda x: str(x)[:2])
    score_list2 = df_mode_dic[df_mode_dic['score_type'] == 'co']['score_name'].tolist()
    df_mode_dic=df_mode_dic[(df_mode_dic['score_type']==score_type)]
    score_list=df_mode_dic['score_name'].tolist()
    if score_type=='fm':
        score_list=score_list+score_list2
    return score_list
def target_date_decision():
    if gt.is_workday2() == True:
        today = date.today()
        next_day = gt.next_workday_calculate(today)
        critical_time = '20:00'
        time_now = datetime.datetime.now().strftime("%H:%M")
        if time_now >= critical_time:
            return next_day
        else:
            today=today.strftime('%Y-%m-%d')
            return today
    else:
        today = date.today()
        next_day=gt.next_workday_calculate(today)
        return next_day
def score_type_decision():
    critical_time_fm_start = '19:30'#rr 21:00
    critical_time_fm_end = '24:00'#24:00
    critical_time_vp02_start='02:00'#vp02 02:00
    critical_time_vp02_end='04:00'#04:00
    critical_time_vp_start='07:00'#vp01 07:00
    critical_time_vp_end='11:00'#11:00
    time_now = datetime.datetime.now().strftime("%H:%M")
    if critical_time_fm_start<=time_now<=critical_time_fm_end:
        return 'fm'
    # elif critical_time_vp02_start<=time_now<=critical_time_vp02_end:
    #     return 'vp02'
    # elif critical_time_vp_start<=time_now<=critical_time_vp_end:
    #     return 'vp01'
    else:
        return None
def portfolio_updating(score_type):
    inputpath_portfolio=glv.get('portfolio_data')
    outputpath_weight=glv.get('output_weight')
    outputpath_check=glv.get('output_check')
    score_name_list = score_name_withdraw(score_type)
    #score_name_list=['vp01_hs300_tighten','fm03_hs300_tighten','fm01_zz500_loosen','fm03_zz500_tighten','vp01_zz500_tighten','fm03_zzA500_tighten','vp01_zzA500_tighten','fm03_zz500_NL','fm03_zz1000_NL']
    target_date = target_date_decision()
    for score_name in score_name_list:
        target_date2=gt.intdate_transfer(target_date)
        daily_outputpath=os.path.join(outputpath_weight,score_name)
        daily_check=os.path.join(outputpath_check,score_name)
        daily_style=os.path.join(daily_check,'style')
        daily_industry=os.path.join(daily_check,'industry')
        gt.folder_creator2(daily_style)
        gt.folder_creator2(daily_industry)
        gt.folder_creator2(daily_outputpath)
        daily_outputpath=os.path.join(daily_outputpath,str(score_name)+'_'+target_date2+'.csv')
        daily_style = os.path.join(daily_style, str(score_name) + '_StyleCheck_' + target_date2 + '.csv')
        daily_industry = os.path.join(daily_industry, str(score_name) + '_IndustryCheck_' + target_date2 + '.csv')
        daily_inputpath=os.path.join(inputpath_portfolio,score_name)
        daily_inputpath=os.path.join(daily_inputpath,target_date)
        inputpath_weight=os.path.join(daily_inputpath,'weight.csv')
        inputpath_code=os.path.join(daily_inputpath,'Stock_code.csv')
        try:
            df_weight=pd.read_csv(inputpath_weight,header=None)
            df_code=pd.read_csv(inputpath_code)
        except:
            print(str(score_name)+'没有更新')
            continue
        df_weight.columns=['weight']
        df_code.columns=['code']
        df_final=pd.concat([df_code,df_weight],axis=1)
        if df_final['weight'].sum()<0.99 or df_final['weight'].sum()>1.01:
            print(str(score_name)+'没有更新')
        df_final['weight']=df_final['weight']/df_final['weight'].sum()
        df_style, df_industry = portfolio_checking(score_name, target_date)
        df_style.to_csv(daily_style,encoding='gbk',index=False)
        df_industry.to_csv(daily_industry,encoding='gbk',index=False)
        df_final.to_csv(daily_outputpath,index=False)
def portfolio_updating2(score_name,start_date,end_date):
    inputpath_portfolio=glv.get('portfolio_data')
    outputpath_weight=glv.get('output_weight')
    working_days_list=gt.working_days_list(start_date,end_date)
    for target_date in working_days_list:
        target_date2=gt.intdate_transfer(target_date)
        daily_outputpath=os.path.join(outputpath_weight,score_name)
        gt.folder_creator2(daily_outputpath)
        daily_outputpath=os.path.join(daily_outputpath,str(score_name)+'_'+target_date2+'.csv')
        daily_inputpath=os.path.join(inputpath_portfolio,score_name)
        daily_inputpath=os.path.join(daily_inputpath,target_date)
        inputpath_weight=os.path.join(daily_inputpath,'weight.csv')
        inputpath_code=os.path.join(daily_inputpath,'Stock_code.csv')
        try:
            df_weight=pd.read_csv(inputpath_weight,header=None)
            df_code=pd.read_csv(inputpath_code)
        except:
            print(str(score_name)+'没有更新')
            continue
        df_weight.columns=['weight']
        df_code.columns=['code']
        df_final=pd.concat([df_code,df_weight],axis=1)
        if df_final['weight'].sum()<0.99 or df_final['weight'].sum()>1.01:
            print(str(score_name)+'没有更新')
        df_final['weight']=df_final['weight']/df_final['weight'].sum()
        df_final.to_csv(daily_outputpath,index=False)

def portfolio_updating_auto():#bat文件触发这个
    # score_type=score_type_decision()
    score_type='fm'
    if score_type!=None:
        portfolio_updating(score_type)
        target_date = target_date_decision()
        portfolio_Error_raising(target_date)
def portfolio_updating_bu():
     df=history_config_withdraw()
     for i in range(len(df)):
        start_date=df['start_date'].tolist()[i]
        end_date = df['end_date'].tolist()[i]
        start_date=gt.strdate_transfer(start_date)
        end_date = gt.strdate_transfer(end_date)
        score_name = df['score_name'].tolist()[i]
        portfolio_updating2(score_name,start_date,end_date)

if __name__ == '__main__':
    #portfolio_updating(score_type='fm')
    portfolio_updating_auto()
    #portfolio_updating_bu()
