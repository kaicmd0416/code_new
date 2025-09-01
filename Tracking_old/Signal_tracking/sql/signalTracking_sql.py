import Signal_tracking.global_setting.global_dic as glv
from Signal_tracking.tools_func.tools_func import *

class SignalTracking_sql:
    def __init__(self,start_date,end_date):
        self.start_date=start_date
        self.end_date=end_date
    def index_type_decision(self,x):
        if 'hs300' in x or '沪深300' in x:
            return '000300.SH'
        elif 'zz500' in x or '中证500' in x:
            return '000905.SH'
        elif 'zz1000' in x or '中证1000' in x:
            return '000852.SH'
        elif 'zz2000' in x or '中证2000' in x:
            return '932000.CSI'
        elif 'zzA500' in x or '中证A500' in x:
            return '000510.SH'
        else:
            return x
    #处理signal的
    def signalInputpath_withdraw(self,portfolio_name):
        inputpath = glv.get('signal_split_tracking')
        inputpath = os.path.join(inputpath, portfolio_name)
        return inputpath
    def signal_crosssectionData_withdraw(self,target_date, portfolio_name):
        inputpath = self.signalInputpath_withdraw(portfolio_name)
        target_date2 = gt.intdate_transfer(target_date)
        inputpath = gt.file_withdraw(inputpath, target_date2)
        df = gt.readcsv(inputpath)
        return df
    #处理portfolio的
    def PWInputpath_withdraw(self,type):
        if type=='portfolio':
             inputpath=glv.get('portfolio_split_tracking')
        elif type=='weight':
            inputpath = glv.get('weight_tracking')
        else:
            inputpath=None
            print('没有对应的type')
        return inputpath
    def PW_crosssectionData_withdraw(self,target_date,type):
        inputpath = self.PWInputpath_withdraw(type)
        target_date2 = gt.intdate_transfer(target_date)
        inputpath = gt.file_withdraw(inputpath, target_date2)
        df = gt.readcsv(inputpath)
        return df
    def PWSplit_sql_main(self,date):
            df_weight=self.PW_crosssectionData_withdraw(date,'weight')
            df_portfolio=self.PW_crosssectionData_withdraw(date,'portfolio')
            if not df_weight.empty and not df_portfolio.empty:
                df_weight['type']='weight'
                df_portfolio['type']='returnContribution'
                other_cols = [col for col in df_portfolio.columns if
                              col not in ['valuation_date', 'portfolio_name','type']]
                df_portfolio[other_cols]=df_portfolio[other_cols]/10000
                df_weight['benchmark']=df_weight['portfolio_name'].apply(lambda x: self.index_type_decision(x))
                df_portfolio['benchmark'] = df_portfolio['portfolio_name'].apply(lambda x: self.index_type_decision(x))
                df_final=pd.concat([df_portfolio,df_weight])
                df_final.rename(columns={'portfolio_name':'name'},inplace=True)
            else:
                df_final=pd.DataFrame()
            return df_final


    def signalSplit_sql_main(self):
        inputpath = glv.get('signal_split_tracking')
        input_list = os.listdir(inputpath)
        outputpath_original = glv.get('output_sql')
        outputpath_signal = os.path.join(outputpath_original, 'SignalTracking')
        gt.folder_creator2(outputpath_signal)
        working_days_list = gt.working_days_list(self.start_date, self.end_date)
        for date in working_days_list:
            print(date)
            date2 = gt.intdate_transfer(date)
            df_final = pd.DataFrame()
            outputpath_daily = os.path.join(outputpath_signal, 'SignalTracking_' + str(date2) + '.csv')
            for portfolio_name in input_list:
                df = self.signal_crosssectionData_withdraw(date, portfolio_name)
                if len(df) == 0:
                    print(portfolio_name + '在' + str(date) + '没有数据')
                else:
                    df['name'] = portfolio_name
                    df_final = pd.concat([df_final, df])
                    df_final['type']='excessReturn'
            other_cols = [col for col in df_final.columns if col not in ['valuation_date','name','index_type','type']]
            df_final[other_cols]=df_final[other_cols]/10000
            df_final2=self.PWSplit_sql_main(date)
            if not df_final.empty and not df_final2.empty:
                df_final['benchmark'] = df_final['index_type'].apply(lambda x: self.index_type_decision(x))
                df_final.drop(columns='index_type', inplace=True)
                df_final3=pd.concat([df_final,df_final2])
                df_final3=df_final3[['valuation_date','name','benchmark','type']+other_cols]
                df_final3.to_csv(outputpath_daily, index=False)
            else:
                print(date+'中signal_tracking存在有df为空，请检查数据')
# sts=SignalTracking_sql('2024-01-01','2025-03-20')
# sts.signalSplit_sql_main()





