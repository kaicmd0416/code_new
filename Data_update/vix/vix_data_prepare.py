import global_setting.global_dic as glv
from Time_tools.time_tools import time_tools
import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC_NEW')
sys.path.append(path)
import global_tools as gt
import pandas as pd
from datetime import datetime, timedelta
import calendar
import re
import datetime
class option_dataprepared:
    def __init__(self,target_date,index_type,realtime):
        self.target_date=target_date
        self.option_code=self.option_code_transfer(index_type)
        self.realtime=realtime
    def option_code_transfer(self,index_type):
        if index_type=='hs300':
            return 'IO'
        elif index_type=='sz50':
            return 'HO'
        elif index_type=='zz1000':
            return 'MO'
        else:
            raise ValueError
    def new_Ticker_withdraw(self,x):  # 将合约以月份进行分类
        if not isinstance(x, str):
            return x
            
        first_dash = x.find('-')
        if first_dash == -1:
            return x
            
        return x[:first_dash]
    def digit_withdrow(self,x):  # 提取合约的行权价格
        for i in range(-6, 0):
            if x[i:].isdigit() == False:
                continue
            else:
                return int(x[i:])
    def direction(self,x):  # 将合约以认购和认沽的种类进行分类
        if not isinstance(x, str):
            return None
            
        # Find the first and second '-' positions
        first_dash = x.find('-')
        if first_dash == -1:
            return None
            
        second_dash = x.find('-', first_dash + 1)
        if second_dash == -1:
            return None
            
        # Extract the character between the dashes
        char_between = x[first_dash + 1:second_dash]
        
        if char_between == 'C':
            return 'Call'
        elif char_between == 'P':
            return 'Put'
        else:
            return None
    def get_third_friday(self, x):
        """
        计算给定月份的第三个星期五，如果第三个星期五不是工作日，则返回下一个工作日
        Args:
            x (str): 输入日期，格式为'YYYY-MM'
        Returns:
            str: 第三个星期五的日期，格式为'YYYY-MM-DD'
        """
        # 将输入转换为datetime对象
        if isinstance(x, str):
            # 添加日期部分
            x = pd.to_datetime(x + '-01')
        
        # 获取当月第一天
        first_day = x.replace(day=1)
        
        # 获取当月日历
        cal = calendar.monthcalendar(x.year, x.month)
        
        # 找到第三个星期五
        fridays = [week[calendar.FRIDAY] for week in cal if week[calendar.FRIDAY] != 0]
        third_friday = fridays[2]  # 索引2表示第三个星期五
        
        # 构建完整的日期
        third_friday_date = first_day.replace(day=third_friday)
        
        # 转换为字符串格式
        third_friday_date = third_friday_date.strftime('%Y-%m-%d')
        
        # 检查是否为工作日，如果不是则返回下一个工作日
        if gt.is_workday(third_friday_date):
            return third_friday_date
        else:
            return gt.next_workday_calculate(third_friday_date)

    def process_code(self,x):
        """
        处理代码字符串，找到最后一个点号的位置并向前截取
        Args:
            x (str): 输入的代码字符串
        Returns:
            str: 处理后的代码字符串
        """
        if not isinstance(x, str):
            return x
        last_dot_index = x.rfind('.')
        if last_dot_index != -1:
            return x[:last_dot_index]
        return x
    def expire_date_withdraw(self,x):
        if not isinstance(x, str):
            return x
            
        # Find the first '-' position
        first_dash = x.find('-')
        if first_dash == -1:
            return x
            
        # Get the string before the first '-'
        before_dash = x[:first_dash]
        
        # Find the first 4 consecutive digits
        match = re.search(r'\d{4}', before_dash)
        if not match:
            return x
            
        # Extract the 4 digits and add '20' prefix
        four_digits = match.group()
        yyyymm = '20' + four_digits
        
        # Format as YYYY-MM
        return f"{yyyymm[:4]}-{yyyymm[4:]}"

    def volume_calculate(self,x):  # 计算每日不同合约的成交总量
        x['volume'] = x['volume'].sum()
        return x
    
    def crosssection_option_withdraw_daily(self):
        target_date=self.target_date
        inputpath=glv.get('output_optiondata')
        target_date=gt.intdate_transfer(target_date)
        inputpath=gt.file_withdraw(inputpath,target_date)
        df=gt.readcsv(inputpath)
        if len(df)==0:
            print('option_data没有找到'+str(target_date)+'的文件')
        else:
            df['code'] = df['code'].apply(lambda x: self.process_code(x))
            df['index_type'] = df['code'].apply(lambda x: str(x)[:2])
            df = df[df['index_type'] == self.option_code]
            df['maturity_date'] = df['code'].apply(lambda x: self.expire_date_withdraw(x))
            df['maturity_date'] = df['maturity_date'].apply(lambda x: self.get_third_friday(x))
            df['maturity'] = pd.to_datetime(df['maturity_date']) - pd.to_datetime(df['valuation_date'])
            df['maturity'] = df['maturity'].apply(lambda x: x.days)
            df['maturity'] = df['maturity'] / 365
            df['new_Ticker'] = df['code'].apply(lambda x: self.new_Ticker_withdraw(x))
            df['contract_type'] = df['code'].apply(lambda x: self.direction(x))
            df['exercise_price'] = df['code'].apply(lambda x: self.digit_withdrow(x))
            df['r'] = 0.02
            df['Volume_Shares'] = df['volume']
            df = df.groupby(['new_Ticker', 'valuation_date']).apply(lambda x: self.volume_calculate(x))
            df.rename(columns={'valuation_date': 'Date', 'volume': 'Volume', 'close': 'Close'}, inplace=True)
            df.set_index('Date', inplace=True)

        return df
    def crosssection_option_withdraw_realtime(self):
        target_date=datetime.date.today()
        target_date=gt.strdate_transfer(target_date)
        available_date=gt.last_workday_calculate(target_date)
        available_date=gt.intdate_transfer(available_date)
        inputpath_yes = glv.get('output_optiondata')
        inputpath_yes = gt.file_withdraw(inputpath_yes, available_date)
        df_yes = gt.readcsv(inputpath_yes)
        df_yes=df_yes[['code','volume']]
        inputpath=glv.get('input_realtime')
        df=pd.read_excel(inputpath,sheet_name='option_info')
        df=df[['代码','现价','日期']]
        df.columns=['code','close','valuation_date']
        df['valuation_date']=pd.to_datetime(df['valuation_date'].astype(str))
        df['code']=df['code'].apply(lambda x: self.process_code(x))
        df['index_type']=df['code'].apply(lambda x: str(x)[:2])
        df=df[df['index_type']==self.option_code]
        df['maturity_date']=df['code'].apply(lambda x: self.expire_date_withdraw(x))
        df['maturity_date']=df['maturity_date'].apply(lambda x:self.get_third_friday(x))
        df['maturity']=pd.to_datetime(df['maturity_date'])-pd.to_datetime(df['valuation_date'])
        df['maturity']=df['maturity'].apply(lambda x: x.days)
        df['maturity']=df['maturity']/365
        df['new_Ticker'] = df['code'].apply(lambda x: self.new_Ticker_withdraw(x))
        df['contract_type'] = df['code'].apply(lambda x: self.direction(x))
        df['exercise_price'] = df['code'].apply(lambda x: self.digit_withdrow(x))
        df['r']=0.02
        df=df.merge(df_yes,on='code',how='left')
        df['Volume_Shares']=df['volume']
        df = df.groupby(['new_Ticker', 'valuation_date']).apply(lambda x: self.volume_calculate(x))
        df.rename(columns={'valuation_date':'Date','volume':'Volume','close':'Close'},inplace=True)
        df.set_index('Date', inplace=True)
        return df
    def crosssection_option_withdraw(self):
        if self.realtime==False:
            df=self.crosssection_option_withdraw_daily()
        else:
            df=self.crosssection_option_withdraw_realtime()
        return df

if __name__ == '__main__':
    od=option_dataprepared('2025-04-28','hs300')
    print(od.crosssection_option_withdraw())