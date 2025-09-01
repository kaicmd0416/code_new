import Portfolio_tracking.global_setting.global_dic as glv
from Portfolio_tracking.tools_func.tools_func import *
class PortfolioTracking_sql:
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
    def inputpath_withdraw(self,index_type):
        index_short = index_shortname_withdraw(index_type)
        inputpath = glv.get('cross_section_output')
        inputpath_excess = os.path.join(inputpath, 'excess_return')
        inputpath_return = os.path.join(inputpath, 'return')
        inputpath_excess = os.path.join(inputpath_excess, index_short)
        inputpath_return = os.path.join(inputpath_return, index_short)
        return inputpath_excess, inputpath_return
    def cross_section_data_withdraw(self,target_date, index_type):
        inputpath1, inputpath2 = self.inputpath_withdraw(index_type)
        target_date2 = gt.intdate_transfer(target_date)
        inputpath_excess = gt.file_withdraw(inputpath1, target_date2)
        inputpath_return = gt.file_withdraw(inputpath2, target_date2)
        df_excess = gt.readcsv(inputpath_excess)
        df_return = gt.readcsv(inputpath_return)
        return df_excess, df_return

    def PortfolioTracking_sql_main(self):
        outputpath_original = glv.get('output_sql')
        outputpath_signal = os.path.join(outputpath_original, 'PortfolioTracking')
        gt.folder_creator2(outputpath_signal)
        working_days_list = gt.working_days_list(self.start_date, self.end_date)
        for date in working_days_list:
            date2=gt.intdate_transfer(date)
            outputpath_daily=os.path.join(outputpath_signal,'PortfolioTracking_'+date2+'.csv')
            df_final=pd.DataFrame()
            for index_type in ['沪深300','中证500','中证1000','中证A500']:
                df_excess, df_return=self.cross_section_data_withdraw(date,index_type)
                if not df_excess.empty and not df_return.empty:
                    df_excess.set_index('valuation_date',inplace=True,drop=True)
                    df_excess=df_excess.T
                    df_excess.reset_index(inplace=True)
                    df_excess.columns=['name','excessReturn']
                    df_return.set_index('valuation_date', inplace=True, drop=True)
                    df_return = df_return.T
                    df_return.reset_index(inplace=True)
                    df_return.columns = ['name', 'return']
                    daily_df=df_excess.merge(df_return,on='name',how='left')
                    daily_df['valuation_date'] = date
                    daily_df['type'] = index_type
                    daily_df=daily_df[['valuation_date','name','type','excessReturn','return']]
                else:
                    print(index_type+'在'+date+'没有portfolio_tracking的数据')
                    daily_df=pd.DataFrame()
                if len(daily_df)>0:
                    df_final=pd.concat([df_final,daily_df])
            if len(df_final)>0:
                df_final['type']=df_final['type'].apply(lambda x: self.index_type_decision(x))
                df_final.rename(columns={'type':'benchmark'},inplace=True)
                df_final.to_csv(outputpath_daily,index=False,encoding='gbk')
# pts=PortfolioTracking_sql('2024-01-01','2025-03-20')
# pts.PortfolioTracking_sql_main()