import pandas as pd
import numpy as np

#for gross bookings, relevant metric could be level shift at the end of the year/beginning, or clickshare data.
#afer testing, I discovered that the automatic change point derection could to some extend detect the level shift.
#however, this depends on the chnagepoint range that is specified.Fow now, the level shift is commented out.
#for cancellations data, chain daily cancellation data is potentially very relevant regressor
#because the bulk cancellation of chains account for much of the variation in cancellation data apart from normal fluctuations.
#the cancellation query for chains is at
#https://gitlab.booking.com/core/main/blob/trunk/apps/oozie/workflows/hdbi-globalchains-kpi-tableau/hql/query_reconcillation.hql
# for now, if has_add_regressors in the main script is set to True, the gen_regressors function return None 
def gen_regressors(perf, metric_name):
    """add additional regressors based on the metric

    params:
        perf: panda dataframe, prophet ready dataframe with date
        metric_name: string
    returns:
        panda dataframe with additional regressors
    """
    metric_name_list = ["gross_bookings", "cacancellations"]
    if metric_name not in metric_name_list:
        return None
    elif metric_name == "gross_bookings":
       #  df = pd.DataFrame(perf.ds)
       #  df = df.assign(date = lambda x: pd.to_datetime(x['ds']))
       #  df['year'] = df["date"].dt.year
       #  df['month'] = df["date"].dt.month
       #  df['day'] = df["date"].dt.day
       #
       #  df['year_number'] = df['year'].rank(method = 'dense').astype(int) -1
       #  df['level_shift'] = np.where((df['month'] >= 12) & (df['day'] >= 28),
       #                                        df['year_number']+1, df['year_number'])
       # return df[['ds', 'level_shift']]
        return None
    # add cancellation chain query, daily cancellation data
    elif metric_name == "cancellations":
        return None
