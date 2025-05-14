from Trading.trading_weight.trading_weight_selecting import trading_weight_combination_main
from Trading.trading_order.trading_order_processing import trading_xy_main,trading_rr_main
from Trading.global_setting import global_dic as glv
from Trading.trading_weight.trading_weight_selecting import target_date_decision
import global_tools_func.global_tools as gt
import os
def trading_weight_main():#portfolio_weight准备完毕之后触发这个
    trading_weight_combination_main()
def xy_trading_main():
    # to_mode选v1是跃然的t0 选v2是景泰的t0
    # trading_mode选v1是twap 选v2是vwap
    t0_mode='mode_v2'
    trading_mode='mode_v2'
    is_realtime=False
    #is_realtime=True
    df,df_nontrading=trading_xy_main(trading_mode,t0_mode,is_realtime)
    date=target_date_decision()
    date=gt.intdate_transfer(date)
    outputpath=glv.get('product_info')
    gt.folder_creator2(outputpath)
    outputpath1=os.path.join(outputpath,'tradingInfo_'+str(date)+'.csv')
    outputpath2=os.path.join(outputpath,'xyNTrading_'+str(date)+'.csv')
    if len(df)>0:
        df.to_csv(outputpath1,index=False)
    if len(df_nontrading)>0:
        df_nontrading.to_csv(outputpath2,index=False)

def rr_trading_main():#是否盘中交易is_realtime=False 正常的 Ture是盘中交易
    #is_realtime=True
    is_realtime = False
    df,df_nontrading=trading_rr_main(is_realtime)
    date=target_date_decision()
    date=gt.intdate_transfer(date)
    outputpath=glv.get('product_info')
    gt.folder_creator2(outputpath)
    outputpath1=os.path.join(outputpath,'tradingInfo_'+str(date)+'.csv')
    outputpath2=os.path.join(outputpath,'rrNTrading_'+str(date)+'.csv')
    if len(df)>0:
        df.to_csv(outputpath1,index=False)
    if len(df_nontrading)>0:
        df_nontrading.to_csv(outputpath2,index=False)

if __name__ == '__main__':
    #trading_weight_main()
    xy_trading_main()
    rr_trading_main()
    pass
