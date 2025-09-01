import Product_tracking.global_setting.global_dic as glv
import pandas as pd
import numpy as np
from scipy import stats
from scipy.optimize import newton
from scipy.stats import norm
import global_tools_func.global_tools as gt
def product_name_transfer(product_name):
    inputpath=glv.get('product_detail')
    df=pd.read_excel(inputpath)
    product_name2=df[df['product_name']==product_name]['other_name'].tolist()[0]
    return product_name2
def product_type(product_name):
    inputpath=glv.get('product_detail')
    df=pd.read_excel(inputpath)
    product_discription=df[df['product_name']==product_name]['product_discription'].tolist()[0]
    return product_discription
def product_list():
    inputpath = glv.get('product_detail')
    df = pd.read_excel(inputpath)
    product_list=df['product_name'].tolist()
    return product_list
def product_code_withdraw(product_name):
    inputpath=glv.get('product_detail')
    df=pd.read_excel(inputpath)
    product_code=df[df['product_name']==product_name]['product_code'].tolist()[0]
    return product_code
class delta_calculator:
    def __init__(self,available_date,df,df_future):
        self.available_date=available_date
        self.df=df
        self.df_future=df_future

    def process_df(self):
        df=self.df.copy()
        df['future_code']=df['科目名称'].apply(lambda x: self.future_option_mapping(x))
        df=df.merge(self.df_future,on='future_code',how='left')
        df['option_type']=df['科目名称'].apply(lambda x: 'call' if 'C' in x else 'put')
        df['strike_price']=df['科目名称'].apply(lambda x: float(x.split('-')[-1].split()[0]))
        df['date']=df['科目名称'].apply(lambda x: '20'+str(x)[2:4]+'-'+str(x)[4:6])
        df['maturity_date']=df['date'].apply(lambda x:self.get_third_friday(x))
        df['maturity_date']=pd.to_datetime(df['maturity_date'])
        df['T']=(df['maturity_date']-pd.to_datetime(self.available_date)).dt.days/365
        df=self.calculate_sigma(df)
        return df
        
    def future_option_mapping(self,x):
        if str(x)[:2] == 'HO':
            return 'IH' + str(x)[2:6]
        elif str(x)[:2] == 'IO':
            return 'IF' + str(x)[2:6]
        elif str(x)[:2] == 'MO':
            return 'IM' + str(x)[2:6]
        else:
            print('qnmd')
            raise ValueError
    def bsm_option_price(self,S, K, r, T, sigma, option_type='call'):
    # 计算B-S模型下的期权价格
    # :param S: 标的资产价格
    # :param K: 行权价格
    # :param r: 无风险利率
    # :param T: 到期时间（年）
    # :param sigma: 波动率
    # :param option_type: 期权类型（'call'表示看涨期权，'put'表示看跌期权）
    # :return: 期权价格

        d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
    
        if option_type == 'call':
            option_price = S * stats.norm.cdf(d1) - K * np.exp(-r * T) * stats.norm.cdf(d2)
        elif option_type == 'put':
            option_price = K * np.exp(-r * T) * stats.norm.cdf(-d2) - S * stats.norm.cdf(-d1)
    
        return option_price

    def BM_IV(self,P, S, K, r, T, option_type,option_name):
        sigma_min = 0.0000001
        sigma_max = 1.000
        sigma_mid = (sigma_min + sigma_max) / 2
        status='normal'
        if option_type=='call':
            def call_bs(S, K, sigma, r, T):
                d1 = (np.log(S / K) + (r + pow(sigma, 2) / 2) * T) / (sigma * np.sqrt(T))
                d2 = d1 - sigma * np.sqrt(T)
                call_value = S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
                return call_value
            call_min = call_bs(S, K, sigma_min, r, T)
            call_max = call_bs(S, K, sigma_max, r, T)
            call_mid = call_bs(S, K, sigma_mid, r, T)
            diff = P - call_mid
            if P < call_min or P > call_max:
                print('-----------------------------------------')
                print(option_name,'输入参数为',P, S, K, r, T, option_type)
                print('理论最小价格为:',call_min, '理论最大价格为:',call_max,'市场价格为:', P)
                print('-----------------------------------------')
                status='error'
            if status=='error':
                sigma_mid=None
            else:
                while abs(diff) > 1e-6:
                    diff = P - call_bs(S, K, sigma_mid, r, T)
                    sigma_mid = (sigma_min + sigma_max) / 2
                    call_mid = call_bs(S, K, sigma_mid, r, T)
                    if P > call_mid:
                        sigma_min = sigma_mid
                    else:
                        sigma_max = sigma_mid
        else:
            def put_bs(S, K, sigma, r, T):
                d1 = (np.log(S / K) + (r + pow(sigma, 2) / 2) * T) / (sigma * np.sqrt(T))
                d2 = d1 - sigma * np.sqrt(T)
                put_value = K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
                return put_value
            put_min = put_bs(S, K, sigma_min, r, T)
            put_max = put_bs(S, K, sigma_max, r, T)
            put_mid = put_bs(S, K, sigma_mid, r, T)
            diff = P - put_mid
            if P < put_min or P > put_max:
                print('-----------------------------------------')
                print(option_name,'输入参数为',P, S, K, r, T, option_type)
                print('理论最小价格为:',put_min, '理论最大价格为:',put_max,'市场价格为:', P)
                print('-----------------------------------------')
                status='error'
            if status=='error':
                sigma_mid=None
            else:
                while abs(diff) > 1e-6:
                    diff = P - put_bs(S, K, sigma_mid, r, T)
                    sigma_mid = (sigma_min + sigma_max) / 2
                    put_mid = put_bs(S, K, sigma_mid, r, T)
                    if P > put_mid:
                        sigma_min = sigma_mid
                    else:
                        sigma_max = sigma_mid
        return sigma_mid

    def implied_volatility(self,option_price, S, K, T, r, option_type,option_name):
        def difference(sigma):
            return self.bsm_option_price(S, K, T, r, sigma, option_type) - option_price
        iv=self.BM_IV(option_price, S, K, r, T, option_type,option_name)
        if iv==None:
            try:
                print('开始使用牛顿法')
                iv=newton(difference, 0.2, tol=1E-2)
            except:
                print('无解')
                if str(option_name)[:2]=='IO':
                     iv = 0.1
                else:
                     iv = 0.2
        return iv
    def calculate_sigma(self, df):
        df['sigma'] = df.apply(lambda row: self.implied_volatility(
            S=row['future_price'],
            K=row['strike_price'],
            r=0.02,
            T=row['T'],
            option_price=row['市价'],
            option_type=row['option_type'],
            option_name=row['科目名称'],
        ), axis=1)
        return df
    def d(self,s,k,r,T,sigma):
        d1 = (np.log(s / k) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        return (d1,d2)
    def delta(self,s,k,r,T,sigma,n):
        if n=='put':
            n2=-1
        else:
            n2=1
        d1 = self.d(s, k, r, T, sigma)[0]
        delta = n2 * stats.norm.cdf(n2 * d1)
        return delta
    def get_third_friday(self,year_month):
    # 将YYYY_MM转换为datetime
        year = int(year_month.split('-')[0])
        month = int(year_month.split('-')[1])
        first_day = pd.Timestamp(f'{year}-{month:02d}-01')
    
    # 获取当月第一个星期五
        friday = first_day + pd.Timedelta(days=(4 - first_day.weekday() + 7) % 7)
    
    # 获取第三个星期五
        third_friday = friday + pd.Timedelta(weeks=2)
    
    # 检查是否为工作日，如果不是则往后顺延到下一个工作日
        if gt.is_workday(third_friday)==False:
                third_friday = gt.next_workday_calculate(third_friday)
    
        return third_friday

    def delta_calculator_main(self):
        df=self.process_df()
        df['Delta'] = df.apply(lambda row:  self.delta(
            s=row['future_price'],
            k=row['strike_price'],
            r=0.02,
            T=row['T'],
            sigma=row['sigma'],
            n=row['option_type']
        ), axis=1)
        return df
    
    

    
