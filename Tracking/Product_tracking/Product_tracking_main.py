from Product_tracking.running_main.FOL_tracking import portfolio_tracking
from Product_tracking.timeseries_pros.timeSeries_prodTracking import ProdTracking_timeSeries_main
import datetime
from datetime import date
import global_tools_func.global_tools as gt
from Product_tracking.tools_func.tools_func import product_list
from Product_tracking.sql.prodTracking_sql import ProdTrackingSql_main
def time_zoom_decision():
    product_list1=product_list()
    return product_list1
    # time_now = datetime.datetime.now().strftime("%H:%M")
    # if '06:00'<=time_now<='12:00':
    #     return ['瑞锐精选','仁睿价值精选1号','瑞锐500','盛元8号','高毅振英1号']
    # else:
    #     return ['宣夜惠盈一号']
def update_main(): #触发这个
    today = date.today()
    available_date = gt.last_workday_calculate(today)
    product_list1 = time_zoom_decision()
    for product_name in product_list1:
        print(product_name)
        try:
            pt = portfolio_tracking(product_name, available_date)
            pt.saving_main()
            ProdTracking_timeSeries_main(product_name)
            ProdTrackingSql_main(available_date,available_date,product_name)
        except:
            print(str(product_name)+'估值表解析更新有误，请手动检查估值表和估值表提取data.')
def history_main(product_list,start_date,end_date):#date为目标时间
   #product_list模板 ['瑞锐精选','仁睿价值精选1号','瑞锐500指增','盛丰500指增8号','宣夜惠盈1号','高毅振英1号','念空瑞景39号','念空知行4号']
   working_days_list=gt.working_days_list(start_date,end_date)
   for available_date in working_days_list:
       print(available_date)
       for product_name in product_list:
           print(product_name)
           # pt = portfolio_tracking(product_name, available_date)
           # pt.saving_main()
           try:
               pt = portfolio_tracking(product_name, available_date)
               pt.saving_main()
           except:
               print(str(product_name) + '估值表解析在'+str(available_date)+'有误，请手动检查估值表和估值表提取data.')
   for product_name in product_list:
       try:
            ProdTracking_timeSeries_main(product_name)
       except:
            print(product_name+'时序拼接出现问题')
       ProdTrackingSql_main(start_date, end_date,product_name)
if __name__ == '__main__':
    # update_main()
    # update_main()
    #ProdTracking_timeSeries_main('瑞锐500指增')
<<<<<<< Updated upstream
<<<<<<< Updated upstream
    history_main(['高毅振英1号'],'2025-03-20','2025-03-26')
=======
    #history_main(['宣夜惠盈1号'],'2025-02-02','2025-05-02')
    history_main(['念空瑞景39号','瑞锐500指增','瑞锐精选','仁睿价值精选1号','宣夜惠盈1号','高毅振英1号','念空知行4号'],'2025-04-01','2025-04-30')
>>>>>>> Stashed changes
=======
    #history_main(['宣夜惠盈1号'],'2025-02-02','2025-05-02')
    history_main(['念空瑞景39号','瑞锐500指增','瑞锐精选','仁睿价值精选1号','宣夜惠盈1号','高毅振英1号','念空知行4号'],'2025-04-01','2025-04-30')
>>>>>>> Stashed changes
    #update_main()
    #pass
    # product_list=['念空瑞景39号','瑞锐500指增','瑞锐精选','仁睿价值精选1号','宣夜惠盈1号','高毅振英1号','念空知行4号']
    # for product_name in product_list:
    #     ProdTracking_timeSeries_main(product_name)
    #pass
