def gen_weekly_aggregates_yoy(prediction, perf_last_year, metric_name, gen_yoy = False):
    '''Generates weekly aggregated forecast for FP&A with year-over-year (yoy) comparison

    Returns a panda dataframe with this year's forecast with last_year's actual
    numbers with yoy growth rate.

    Parameters
    -----------
    prediction : panda dataframe, predicted values for metrics
    perf_last_year: panda dataframe, actual values for Metrics with previous years' data
    metric_name:  string or list of string, metrics of interestes
    -----------

    Returns
    -------
    panda dataframe
    '''
 
    prediction['week'] = prediction.ds.dt.week
    prediction['year'] = prediction.ds.dt.year
    # only keep those days where it is a full week for weekly aggregates
    pred_this_year = prediction.groupby(['year', 'week']).filter(lambda x: x['yhat'].count() == 7)
    pred_this_year = pred_this_year.groupby(['year', 'week']).agg({"yhat": "sum"}).reset_index()
    
    if gen_yoy == True: 
        # get last year's booking at the same week
        perf_last_year['week'] = perf_last_year.ds.dt.week
        perf_last_year['year'] = perf_last_year.ds.dt.year
        perf_last_year = perf_last_year[(perf_last_year.year == pred_this_year.year.unique()[0] -1) &
                                        (perf_last_year.week.isin(pred_this_year.week.unique().tolist())) ]
        perf_last_year = perf_last_year.groupby(['week', 'year']).agg({'y': 'sum'}).reset_index()

        yoy = pred_this_year.merge(perf_last_year, on = 'week')
        yoy['yoy'] = yoy['yhat']/yoy['y'] - 1
        print (yoy.head())
        yoy = yoy.rename(columns = {"year_x": 'year_{number}'.format(number = yoy.year_x.unique()[0]),
                                    'year_y': 'year_{number}'.format(number = yoy.year_y.unique()[0]),
                                    'yhat': '{metric_name}_{number}'.format(metric_name = "predicted_" + metric_name,
                                                                            number = yoy.year_x.unique()[0] ),
                                    'y': '{metric_name}_{number}'.format(metric_name = "real" + metric_name,
                                                                         number = yoy.year_y.unique()[0])
                                   })
        return yoy
    else:
        return pred_this_year
