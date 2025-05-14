import pandas as pd
import Portfolio_tracking.global_setting.global_dic as glv
from utils.db_processor import DatabaseProcessor
class sql_processing:
    def __init__(self,product_name,simulation,df_proinfo,df_future,df_commodity,df_fo_difference,df_fo_difference2,df_etf_difference,df_stock_difference):
        if product_name=='惠盈一号':
            product_name='宣夜惠盈一号'
        self.product_name=product_name
        self.simulation=simulation
        self.df_proinfo=df_proinfo
        self.df_future=df_future
        self.df_commodity=df_commodity
        self.df_fo_difference=df_fo_difference
        self.df_fo_difference2=df_fo_difference2
        self.df_etf_difference=df_etf_difference
        self.df_stock_difference=df_stock_difference
        # 创建一个数据库处理器实例
        # self.db_processor = DatabaseProcessor()
    def product_code_withdraw(self):
        inputpath=glv.get('config_product')
        df=pd.read_excel(inputpath,sheet_name='product_detail')
        product_code=df[df['other_name']==self.product_name]['product_code'].tolist()[0]
        return product_code
    def df_proinfo_processing(self):
        product_code=self.product_code_withdraw()
        df=self.df_proinfo.copy()
        df1=df[['info_name','money']]
        df2=df[['profit_name','profit']]
        df1.columns=['type','value']
        df2.columns=['type','value']
        df_final=pd.concat([df1,df2])
        df_final['product_code']=product_code
        df_final['simulation']=self.simulation
        #在这里接入存sql的代码，然后primary key 为 product_code,type,simulation
        #其中只有value为float，其他的都是str格式
        #注意这里面入库需要额外加一列update_time，代表入库时间

        # 在您的sql_saving.py中使用
        # db_processor = DatabaseProcessor()
        # db_processor.process_with_interval(df_final, 'realtime_proinfo')
        return df_final
    def df_futureoption_processing(self):
        product_code = self.product_code_withdraw()
        df_futureoption=self.df_future.copy()
        df_commodity=self.df_commodity.copy()
        df_futureoption=df_futureoption[['合约','买卖','总持仓','Delta','market_value','daily_profit','proportion','len']]
        df_commodity= df_commodity[
            ['合约', '买卖', '总持仓', 'Delta', 'market_value', 'daily_profit', 'proportion']]
        df_futureoption.columns=['code','direction','HoldingQty','delta','mkt_value','daily_profit','proportion','len']
        df_commodity.columns=['code','direction','HoldingQty','delta','mkt_value','daily_profit','proportion']
        df_futureoption.drop(columns='len',inplace=True)
        df_futureoption['type']='stock_futureOption'
        df_commodity['type']='commodity_futureOption'
        df_final=pd.concat([df_futureoption,df_commodity])
        df_final['product_code'] = product_code
        df_final['simulation'] = self.simulation
        def direction_decision(x):
            if x=='买':
                return 'long'
            elif x=='卖':
                return'short'
            else:
                return None
        df_final['direction']= df_final['direction'].apply(lambda x: x.strip())
        df_final['direction']=df_final['direction'].apply(lambda x: direction_decision(x))
        #在这里接入存sql的代码，然后primary key 为 product_code,simulation,code,direction
        #其中HoldingQty,delta,mkt_value,daily_profit,proportion为float其他的为str
        #注意这里面入库需要额外加一列update_time，代表入库时间
        #应该创建一个tracking的库，这个对应的表名为 realtime_futureoptionHolding

        return df_final
    def df_changing(self):
        product_code = self.product_code_withdraw()
        df_stock=self.df_stock_difference.copy()
        df_etf=self.df_etf_difference.copy()
        df_future=self.df_fo_difference.copy()
        df_future2=self.df_fo_difference2.copy()
        df_stock=df_stock[['code','HoldingQty','HoldingQty_yes','difference','action']]
        df_etf = df_etf[['code', 'HoldingQty', 'HoldingQty_yes', 'difference', 'action']]
        df_stock['direction']='long'
        df_stock['type']='stock'
        df_etf['direction']='long'
        df_etf['type']='etf'
        df_future=df_future[['合约','买卖','总持仓','昨仓','difference','action']]
        df_future2=df_future2[['合约','买卖','总持仓','昨日持仓','difference','action']]
        df_future.columns=['code','direction','HoldingQty','HoldingQty_yes','difference','action']
        df_future2.columns = ['code', 'direction', 'HoldingQty', 'HoldingQty_yes', 'difference', 'action']
        def direction_decision(x):
            if x=='买':
                return 'long'
            elif x=='卖':
                return'short'
            else:
                return None
        df_future['direction'] = df_future['direction'].apply(lambda x: x.strip())
        df_future2['direction'] = df_future2['direction'].apply(lambda x: x.strip())
        df_future['direction']=df_future['direction'].apply(lambda x: direction_decision(x))
        df_future2['direction'] = df_future2['direction'].apply(lambda x: direction_decision(x))
        df_future['type']='daily_futureOption'
        df_future2['type']='overnight_futureOption'
        df_final=pd.concat([df_stock,df_etf,df_future,df_future2])
        df_final['simulation']=self.simulation
        df_final['product_code']=product_code
        #在这里接入存sql的代码，然后primary key 为 product_code,simulation,code,direction,type
        #其中HoldingQty,HoldingQty_yes,difference为float
        #注意这里面入库需要额外加一列update_time，代表入库时间
        #应该创建一个tracking的库，这个对应的表名为 realtime_holdingChanging
        return df_final
    def productTracking_sqlmain(self):
        df_proinfo = self.df_proinfo_processing()
        df_futureoption = self.df_futureoption_processing()
        df_changing = self.df_changing()


        # 创建数据字典
        data_dict = {
            'realtime_proinfo': df_proinfo,
            'realtime_futureoptionholding': df_futureoption,
            'realtime_holdingchanging': df_changing
        }

        # 一次性写入所有数据
        db_processor = DatabaseProcessor()
        db_processor.write_multiple_tables(data_dict)


