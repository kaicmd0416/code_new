from Portfolio_tracking.futures_options_position_tracking.FOL_tracking import portfolio_tracking
from Portfolio_tracking.futures_options_position_tracking.product_tracking_dp import product_data
def FOL_running_main():#触发这个
    pt=portfolio_tracking()
    pt.saving_main('惠盈一号')
    # pt.saving_main('盛元8号')
def future_option_running_main():#当估值表有问题的时候
    pt = portfolio_tracking()
    pd1=product_data('惠盈一号')
    df1=pd1.position_withdraw()
    # pd2 = product_data('盛元8号')
    # df2=pd2.position_withdraw()
    df_info_HY, df_final_HY = pt.FO_main(df1)
    # df_info_SY, df_final_SY = pt.FO_main(df2)
    print('宣夜惠盈')
    print(df_info_HY)
    # print('---------')
    # print('盛元8号')
    # print(df_info_SY)

