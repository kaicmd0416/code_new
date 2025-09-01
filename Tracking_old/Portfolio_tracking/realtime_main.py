from Portfolio_tracking.futures_options_position_tracking.FOL_main_new import FOL_running_main
from Portfolio_tracking.real_time.portfolio_realtime_return import stock_realtime_main
def realtime_main():#触发这个
    FOL_running_main()
    stock_realtime_main()

if __name__ == '__main__':
    realtime_main()