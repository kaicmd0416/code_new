import os
import pandas as pd
from Trading.global_setting import global_dic as glv
import global_tools_func.global_tools as gt
def trading_order_renrui(df_today,df_yes,df_close,target_date,account_money,product_name):
    inputpath = os.path.split(os.path.realpath(__file__))[0]
    product_name2=str(product_name)[:2]
    inputpath=os.path.join(inputpath,'模板')
    inputpath=os.path.join(inputpath,product_name2)
    inputpath = os.path.join(inputpath, '默认篮子格式.csv')
    outputpath = glv.get('output_trading_order')
    outputpath = os.path.join(outputpath, product_name)
    gt.folder_creator2(outputpath)
    target_date2=gt.intdate_transfer(target_date)
    outputpath=os.path.join(outputpath,str(product_name)+'_'+target_date2+'_trading_list.csv')
    df = gt.readcsv(inputpath)
    code_list_today = df_today['code'].tolist()
    df_today=df_today.merge(df_close,on='code',how='left')
    df_today['quantity']=account_money*df_today['weight']/df_today['close']
    df_today['quantity']=round(df_today['quantity']/100,0)*100
    df_today=df_today[['code','quantity','close']]
    df_yes.rename(columns={'证券代码': 'code'}, inplace=True)
    df_yes2 = gt.code_transfer(df_yes)
    df_yes2['code']=df_yes2['code'].apply(lambda x: str(x)[:-3])
    df_weight = pd.DataFrame()
    code_list_yes = df_yes2['code'].tolist()
    code_list = list(set(code_list_yes) | set(code_list_today))
    df_weight['code'] = code_list
    df_weight = df_weight.merge(df_yes2, on='code', how='left')
    df_weight = df_weight.merge(df_today, on='code', how='left')
    df_weight.fillna(0,inplace=True)
    df_weight['difference']=df_weight['quantity']-df_weight['当前拥股']
    df_weight_check = df_weight.copy()
    df_buying_check = df_weight_check[df_weight_check['difference'] > 0]
    df_selling_check = df_weight_check[df_weight_check['difference'] < 0]
    df_buying_check['mkt_value'] = df_buying_check['close'] * abs(df_buying_check['difference'])
    df_selling_check['mkt_value'] = df_selling_check['close'] * abs(df_selling_check['difference'])
    mkt_buying = df_buying_check['mkt_value'].sum()
    mkt_selling = df_selling_check['mkt_value'].sum()
    df_weight_check['mkt_holding'] = df_weight_check['close'] * df_weight_check['当前拥股']
    mkt_holding = df_weight_check['mkt_holding'].sum()
    print('rr1号买额为:' + str(mkt_buying), '卖额为' + str(mkt_selling) + '买卖额差为' + str(mkt_buying - mkt_selling),
          'holding总市值为:' + str(mkt_holding))
    list_difference=df_weight['difference'].tolist()
    list_action=[]
    for i in list_difference:
        if i>0:
            list_action.append('0')
        elif i<0:
            list_action.append('1')
        else:
            list_action.append('不动')
    df_weight['action']=list_action
    df_weight['difference']=abs(df_weight['difference'])
    df_weight=df_weight[df_weight['action']!='不动']
    df_weight['new_code'] = df_weight['code'].apply(lambda x: str(x)[:2])
    df_weight.loc[(df_weight['new_code'] == '68') & (df_weight['action'] == '0') & (df_weight['difference'] == 100), [
        'difference']] = 200
    code_list_today=df_weight['code'].tolist()
    quantity_list=df_weight['difference'].tolist()
    action_list=df_weight['action'].tolist()
    df['代码'] = code_list_today
    df['数量'] = quantity_list
    df['方向'] = action_list
    df_final = df
    df_final.to_csv(outputpath,index=False,encoding='utf_8_sig')
    return df_final