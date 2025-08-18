from MktData_update.Mktdata_update import (indexData_update,indexComponent_update,
                                           stockData_update,futureData_update,optionData_update,
                                           etfData_update,cbData_update,lhb_amt_update_main, nlb_update_main,futureDifference_update_main)
import os
import sys
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt

def MktData_update_main(start_date,end_date,is_sql):
    IRU = indexData_update(start_date,end_date, is_sql)
    ICU = indexComponent_update(start_date,end_date, is_sql)
    SDU = stockData_update(start_date,end_date, is_sql)
    FDU = futureData_update(start_date,end_date, is_sql)
    ODU = optionData_update(start_date,end_date, is_sql)
    EDU = etfData_update(start_date,end_date, is_sql)
    LAU = lhb_amt_update_main(start_date,end_date, is_sql)
    NLU = nlb_update_main(start_date,end_date, is_sql)
    FDDU = futureDifference_update_main(start_date,end_date, is_sql)
    SDU.stock_data_update_main()
    IRU.index_data_update_main()
    ICU.index_component_update_main()
    FDU.future_data_update_main()
    ODU.option_data_update_main()
    EDU.etf_data_update_main()
    LAU.lhb_amt_update_main()
    NLU.nlb_update_main()
    FDDU.FutureDifference_update_main()
def CBData_update_main(start_date,end_date,is_sql):
     CBU = cbData_update(start_date,end_date,is_sql)
     CBU.cb_data_update_main()
def MktData_update_main2(start_date,end_date,is_sql):
    working_days_list=gt.working_days_list(start_date,end_date)
    for available_date in working_days_list:
        print(available_date)
        #IRU = indexData_update(available_date,available_date,is_sql)
        #ICU=indexComponent_update(available_date,available_date,is_sql)
        SDU=stockData_update(available_date,available_date,is_sql)
        #FDU=futureData_update(available_date,available_date,is_sql)
        #ODU=optionData_update(available_date,available_date,is_sql)
        # EDU=etfData_update(available_date,available_date,is_sql)
        #LAU = lhb_amt_update_main(start_date,end_date,is_sql)
        #NLU = nlb_update_main(start_date,end_date,is_sql)
        #FDDU = futureDifference_update_main(start_date,end_date,is_sql)
        SDU.stock_data_update_main()
        #IRU.index_data_update_main()
        #ICU.index_component_update_main()
        #FDU.future_data_update_main()
        #ODU.option_data_update_main()
        # EDU.etf_data_update_main()
        # LAU.lhb_amt_update_main()
        #NLU.nlb_update_main()
        #FDDU.FutureDifference_update_main()
if __name__ == '__main__':
    #CBData_update_main('2015-01-05', '2025-07-14',True)
    MktData_update_main2('2025-08-17', '2025-08-17',True)
    # MktData_update_main('2022-07-19','2025-07-14',True)

