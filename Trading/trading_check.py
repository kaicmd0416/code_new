import pandas as pd
import os
import sys
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt
# inputpath='D:\Trading_data\\trading_order\仁睿'
# inputpath2='D:\OneDrive\Trading_data_test\\trading_order\仁睿价值精选1号'
# inputpath=os.path.join(inputpath,'renr_20250612_trading_list.csv')
# inputpath2=os.path.join(inputpath2,'仁睿价值精选1号_20250612_trading_list.csv')
inputpath='D:\Trading_data\\trading_order\宣夜'
inputpath2='D:\OneDrive\Trading_data_test\\trading_order\宣夜惠盈1号'
inputpath=os.path.join(inputpath,'xy_20250612_trading_list.csv')
inputpath2=os.path.join(inputpath2,'宣夜惠盈1号_20250612_trading_list.csv')
df=pd.read_csv(inputpath,header=None)
df2=pd.read_csv(inputpath2,header=None)
df=df.loc[7:]
df2=df2.loc[7:]
df=df[[1,2,3]]
df2=df2[[1,2,3]]
df.columns=['代码','数量','方向']
df2.columns=['代码','数量','方向']
df2=df2[['代码','数量','方向']]
df2.columns=['代码','quantity_prod','方向_prod']
df=df.merge(df2,on='代码',how='outer')
df.fillna(0,inplace=True)
df['difference_quantity']=abs(df['数量'].astype(float)-df['quantity_prod'].astype(float))
df=df[(df['difference_quantity']!=0)]
print(df)