import pandas as pd
from scipy import stats

def prepare_df_to_prophet(df, metric_name,tra,lamda):
    """function to prepare a spark dataframe for prophet (data transformation if necessary). 
    Attributes:
        metric_name: string, 
        tra: transformation of dataframe, log transform or boxcox transform
        lambda: rate of boxcox transformation
    Returns: panda dataframe with ds (datetime) and y(forecasted variable)
    """
    
    df_pd = df.toPandas()
    df_pd['yyyy_mm_dd']= pd.to_datetime(df_pd['yyyy_mm_dd'])
    df_pd = df_pd.rename(columns = {metric_name: 'y', 'yyyy_mm_dd': 'ds'})
    df_pd = df_pd.sort_values('ds')
    
    if tra=='log':
        df_pd['y']=np.log(df_pd['y'])
    elif tra=='box_cox':
        df_pd['y']=stats.boxcox(df_pd['y'],  lmbda=lamda)  
        print(df_aus.head())
    return df_pd
