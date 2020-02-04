from __future__ import division, print_function
import pandas as pd
import os
import hashlib
import logging
import numpy as np
from fbprophet import Prophet
from datetime import datetime, timedelta
from pyspark.sql import functions as f, types as t
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

from forecast_lib.utils.prepare_df_to_prophet import prepare_df_to_prophet
from forecast_lib.utils.days_setting import minusndays
from forecast_lib.utils.get_holidays_table import get_holidays_table
from forecast_lib.utils.gen_regressors import gen_regressors
from forecast_lib.utils.parameter_search import create_model_evaluation_udf
from forecast_lib.utils.gen_params_indices import gen_params_indices
from forecast_lib.utils.gen_weekly_aggregates_yoy import gen_weekly_aggregates_yoy
from forecast_lib.utils.ts_cross_validation import ts_train_test_split, ts_train_test_backtest_split
from forecast_lib.utils.evaluation_metrics import evaluation_smape
from forecast_lib.utils.suppress_stdout_stderr import suppress_stdout_stderr

class ForecastBase(object):
    """base class for forecasting for daily metrics

    Attributes:
        Metrics    : performance metrics data
        run_date   : string, the date that the forecast begins
        optimizer  : string, random search 
        eval_level : string, evaluating the model at daily MAPE or quarterly APE or weekly MAPE
        pos        : string, default to All, so that the forecat is at account level, can be at single point of sale
        cv_type    : string, cross validation type, backtesting or rolling_window
        has_add_regressors : boolean, whether the metrics has additional regressors or not
        forecast_horizon   : integer, number of days to forecast into the future   
    """

    def __init__(self,Metrics,run_date,forecast_horizon,optimizer,eval_level,pos,has_add_regressors,cv_type):
        self.run_date           = run_date
        self.forecast_horizon   = forecast_horizon
        self.start_date         = "2015-01-01"
        self.end_date           = minusndays(self.run_date,1)
        self.optimizer          = optimizer
        self.eval_level         = eval_level
        self.pos                = pos
        self.has_add_regressors = has_add_regressors
        self.Metrics            = Metrics(self.start_date,self.end_date,self.pos)
        self.metric_name        = self.Metrics.metric_name
        self.cv_type            = cv_type

    def get_booker_cc(self):
        """returns pos and booker cc correspondence. this function is not defined in the base class, and only defined
        in the child class account for every account (trivago, gha, tripadvisor and kayak)
        """
        raise NotImplementedError("every account need to define account specific booker_cc, pos correspondence")

    def use_prophet(self):
        """ use facebook prophet package to predict for relevant metrics within the forecast horizon
        it contains 7 steps, see comments in the code for more detail
        1. get performance metrics data, prep for prophet package
        2. get holiday data
        3. get additional regressor for a metric if necessary
        4. predict using default prophet configuration
        5. predict using hyperparameter tuning via grid search, cross validation is done by either rolling window or 
        6. compare out of sample SMAPE produced by step 4 model with step 5 model and repredict using the better model
        7. generate component plots and predicted data at daily and weekly level.

        Returns:
            the best hyperparameters
            and predicted daily metrics for the expected forecast horizon
        """

        # 1. getting the relevant metric performance stats,currently only allow for one metric
        metric_values = self.Metrics.values.cache()
        grouped_metric = metric_values.groupBy("yyyy_mm_dd")\
                                        .agg(f.sum(self.metric_name).alias(self.metric_name)).cache()
        perf = prepare_df_to_prophet(df = grouped_metric, metric_name = self.metric_name, tra = None, lamda = None)
        
        # set the cap and floor of the data in case there is
        cap, floor = np.max(perf.y) * 1.1, np.min(perf.y) * 0.9
        perf["cap"] = cap
        perf["floor"]  = floor

        # 2. get holiday tables
        holiday_tables = get_holidays_table(start_date = self.start_date)
        # 2.1. join with corresponding pos table with distinct pos, rank the pos based on relevant metrics
        rank_pos = metric_values\
                .where("yyyy_mm_dd >= '{cutoff_date}'".format(cutoff_date =
                    (datetime.strptime(self.end_date, "%Y-%m-%d") - timedelta(365)).strftime("%Y-%m-%d")))\
                .groupBy("pos")\
                .agg(f.sum(self.metric_name).alias(self.metric_name))\
                .orderBy(self.metric_name, ascending=False).toPandas()
        rank_pos = rank_pos.assign(pos_rank = lambda x: x[self.metric_name].rank(ascending = False))
        
        # 2.2 get booker_cc, pos correspondence
        booker_cc_df = self.get_booker_cc()

        # 2.3 for top 5 pos, keep all holidays, others count for at least 10 times
        holiday_tables = holiday_tables.merge(booker_cc_df).merge(rank_pos[['pos', 'pos_rank']])
        top5 = holiday_tables[holiday_tables['pos_rank']<=5][[ "ds", "holiday"]].drop_duplicates()
        other_pos_counts = holiday_tables[holiday_tables['pos_rank']>5].groupby(['ds', 'holiday']).count().reset_index()
        other_pos = other_pos_counts[other_pos_counts.pos >= 10][['ds', 'holiday']]
        filtered_holiday_table = top5.append(other_pos)\
                        .drop_duplicates()\
                        .assign(lower_window = 0, upper_window = 0)

        # 2.4 manually adjust for some holidays where the upper and lower window is bigger than 0
        # TODO: this has quite some subjectivity and require data inspection
        special_holidays = ['ChristmasEve', 'ThanksgivingDay','NewYearsDay']
        special_holidays_table = filtered_holiday_table[filtered_holiday_table['holiday']\
                                                        .isin(special_holidays)]\
                                                        .assign(lower_window = -1, upper_window = 1)
        special_holidays_table['upper_window'] = np.where(special_holidays_table['holiday'] ==  'ChristmasEve', 0,
                                                           special_holidays_table['upper_window'])

        final_holiday_table = filtered_holiday_table[~filtered_holiday_table['holiday']\
                                             .isin(special_holidays)]\
                                             .append(special_holidays_table)\
                                             .sort_values(by = ['ds'])
        
        # 3. additional regressors
        if self.has_add_regressors:
            add_reg_df = gen_regressors(perf = perf, metric_name = self.metric_name)
            if add_reg_df is not None:
                perf = perf.merge(add_reg_df, on = "ds")

        # 4. default prophet model prediction
        # if separate training and test sets by the forecast horizon, it would be too optimistic about the stationarity of the models
        # so only recent 14 days of data is kept as out of sample validation set, namely here test = validation set.
        # TODO: examine whether it makes to keep the validation set as 14 days 
        train_test_split = 14
        train = perf.iloc[:-train_test_split, ]
        test  = perf.iloc[-train_test_split:, ]

        # use the default prophet model with holidays as baseline
        # supress_stdout_stderr is to suppress the optimization output from Stan used by prophet package
        with suppress_stdout_stderr():
            default_mod = Prophet(holidays = final_holiday_table)
            forecast = default_mod.fit(train).predict(test)
            
        default_mape = evaluation_smape(eval_level = self.eval_level,
                                        forecast_horizon = self.forecast_horizon,
                                        true_df = test, forecast_df = forecast)
        logging.info("The {eval_level} smape for the default model is {default_mape}".format(eval_level = self.eval_level,
                                                                                    default_mape = default_mape))
        # 5.1 run the model with Random search
        if self.optimizer == 'random_search':
            logging.info("Cross Validation type is {cv_type}, start with random grid search for hyperparameters".format(
                    cv_type = self.cv_type))
            params_sp = gen_params_indices(df = train, pred_size = self.forecast_horizon, cv_type = self.cv_type)
            # broadcast the in sample training set
            bc_train = sc.broadcast(train)
            # specify the udf
            mod_eval_udf = create_model_evaluation_udf(eval_level = self.eval_level, perf_df = perf, bc_df = bc_train,
                                                       pred_size = self.forecast_horizon, holiday_table = final_holiday_table, 
                                                       cap= cap, floor = floor, has_add_regressors = self.has_add_regressors)

            results_sp = params_sp.select('params', f.expr('explode(train_indices)').alias('train_indices') )\
                                  .withColumn('smape', mod_eval_udf(f.col("params"), f.col("train_indices")))\
                                  .cache()

            results_pd = results_sp.toPandas()
            # get the meidan smape for each different combinations of hyperparameters and select the best median
            avg_scores = results_pd.groupby('params', as_index = False)[['smape']].median()
            best_params = avg_scores.sort_values('smape').iloc[0].params.asDict()
            logging.info("Best hyperparameters found is {best_params}".format(best_params = best_params))
            # fit the model again using the best parameters with entire training set and evaluate the error
            # based on out of sample test set.
            with suppress_stdout_stderr():
                best_mod = Prophet(daily_seasonality = False, interval_width = 0.95, holidays = final_holiday_table, **best_params)
                forecast = best_mod.fit(train).predict(test)
            rs_mape = evaluation_smape(eval_level = self.eval_level,
                                       forecast_horizon = self.forecast_horizon,
                                       true_df = test, forecast_df = forecast)
            logging.info("The {eval_level} mape for the random grid search model is {rs_mape}".format(eval_level = self.eval_level,
                                                                                                      rs_mape = rs_mape))
        # 5.2 compare the default baseline with random grid search and choose which model to use
        if self.optimizer == "default" or default_mape <= rs_mape:
            logging.info("""Default prophet configuration is the default or it is better than random search
                        therefore re-forecast using the entire dataset with default prophet""")
            with suppress_stdout_stderr():
                final_mod = Prophet(holidays = final_holiday_table)
                future = final_mod.fit(perf).make_future_dataframe(periods=self.forecast_horizon)
                forecast = final_mod.predict(future).tail(self.forecast_horizon)
        else:
            logging.info("""Random search is better than default prophet configuration
                        therefore re-forecast using the entire dataset with the best hyperparameters""")

            # run the model with Random search or default, compare the default baseline with random grid search
            with suppress_stdout_stderr():
                final_mod = Prophet(daily_seasonality = False, interval_width = 0.95, holidays = final_holiday_table, **best_params)
                future = final_mod.fit(perf).make_future_dataframe(periods=self.forecast_horizon)
            if getattr(final_mod, 'growth') == 'logistic':
                future['cap'] = cap
                future['floor'] = floor
            forecast = final_mod.predict(future)

        # 6. TODO: adjust for the recent performance AR

        # 7. produce the components plots and output data needed. 
        components_fig = final_mod.plot_components(forecast)
        # get the predicted data
        prediction = forecast.tail(self.forecast_horizon)[['ds','yhat']].round({'yhat': 0})
        # generate the weekly data or weekly year-over-year data
        yoy = gen_weekly_aggregates_yoy(prediction = prediction, perf_last_year = perf,
                                        metric_name = self.metric_name)

        return (prediction, yoy, components_fig)

class MetricBase(object):
    """base class for getting data for relevant performance metrics

    Attributes:
        metric_name : string, performance metrics data
        start_date  : string, starting date for getting the performance data
        end_date    : string, ending date for getting the performance data
        pos         : string, point of sale defined by an account, e.g. in GHA, EMEA_other is a pos with multiple countries. 

    """
    def __init__(self,metric_name,start_date,end_date,pos):

        self.metric_name  = metric_name
        self.start_date   = start_date
        self.end_date     = end_date
        self.pos          = pos

    def compute(self):
        raise NotImplementedError

    @property
    def values(self):
        return self.compute()
