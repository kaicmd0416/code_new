from Portfolio_tracking.portfolio_performance.cross_section_portfolio_performance import cross_portfolio_update_main
from Portfolio_tracking.timeseries_pros.timeSeries_portTracking import PortTracking_timeSeries_main
import  datetime
import global_tools_func.global_tools as gt
from Portfolio_tracking.sql.portfolioTracking_sql import PortfolioTracking_sql
def backtesting_update_main():#每天1515跑完之后跑，等历史数据补齐之后，将里面注释的释放
    today=datetime.date.today()
    date=gt.strdate_transfer(today)
    start_date = date
    for i in range(3):
        start_date = gt.last_workday_calculate(start_date)
    working_days_list = gt.working_days_list(start_date, date)
    for target_date in working_days_list:
        print(target_date)
        try:
            cross_portfolio_update_main(target_date)
        except:
            print('portfolio_tracking截面数据在'+str(target_date)+'更新出现问题')
    try:
        PortTracking_timeSeries_main()
    except:
        print('portfolio_tracking时序数据更新出现问题')
def backtesting_history_main(start_date,end_date):
    working_day_list=gt.working_days_list(start_date,end_date)
    for target_date in working_day_list:
        print(target_date)
        try:
             cross_portfolio_update_main(target_date)
        except:
             print('portfolio_tracking截面数据在'+str(target_date)+'更新出现问题')
    try:
         PortTracking_timeSeries_main()
    except:
        print('portfolio_tracking时序数据更新出现问题')
    try:
        pts=PortfolioTracking_sql(start_date,end_date)
        pts.PortfolioTracking_sql_main()
    except:
        print('portfolio_tracking中sql数据更新出现问题')
def backtesting_update_bu(start_date,end_date):
    working_days_list=gt.working_days_list(start_date,end_date)
    for date in working_days_list:
        try:
           cross_portfolio_update_main(date)
        except:
            print(date+'更新出现问题')
    try:
        PortTracking_timeSeries_main()
    except:
        print('portfolio_tracking时序数据更新出现问题')
    try:
        pts=PortfolioTracking_sql(start_date,end_date)
        pts.PortfolioTracking_sql_main()
    except:
        print('portfolio_tracking中sql数据更新出现问题')
if __name__ == '__main__':
<<<<<<< Updated upstream
    backtesting_update_main()
    # backtesting_history_main('2025-02-28', '2025-03-03')
=======
    #backtesting_update_main()
    backtesting_history_main('2025-01-01', '2025-04-30')
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
    #backtesting_history_main('2025-01-01', '2025-03-17')
    # time_series_portfolio_performance_update()
    # backtesting_update_bu(start_date='2025-03-12',end_date='2025-03-17')

