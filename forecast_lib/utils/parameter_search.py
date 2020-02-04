from sklearn.model_selection import ParameterSampler
import pandas as pd
import numpy as np
from fbprophet import Prophet
from pyspark.sql import functions as f, types as t

from forecast_lib.utils.gen_regressors import gen_regressors

def create_model_evaluation_udf(eval_level, perf_df, bc_df, pred_size, holiday_table, cap, floor, has_add_regressors):
    """
    create_model_evaluation_udf is spark udf to do hyperparameter search with random grid search,
    it returns the symmetric mean absolute percentage error for combinations of train/test and
    parameter set combinations based on the desired aggregation level

    """
    def model_evaluation(params, indices):
        params = params.asDict()
        model = Prophet(daily_seasonality = False, interval_width = 0.95,
                        holidays = holiday_table, **params)
        train = bc_df.value.iloc[indices]
        if has_add_regressors:
            model.add_regressor(train.columns[~train.columns.isin(['ds', 'y', 'cap', 'floor'])].values[0])
        model.fit(train);
        future = model.make_future_dataframe(periods = pred_size)
        future['cap'] = cap
        future['floor'] = floor
        if has_add_regressors:
            add_reg_df = gen_regressors(perf = future, metric_name = metric_name)
            if add_reg_df is not None:
                future = future.merge(add_reg_df, on = "ds")
        forecast = model.predict(future)
        pred_df = forecast[["ds","yhat"]].set_index("ds").tail(pred_size)
        pred_df["y"] = perf_df.set_index("ds")['y']

        if eval_level == "daily":
            smape = float(np.mean(np.abs(pred_df.y-pred_df.yhat)/(np.abs(pred_df.y) + np.abs(pred_df.yhat))/2) * 100)
        elif eval_level == "weekly":
            pred_df["week"] = pred_df.index.weekofyear
            pred_df = pred_df.groupby("week").agg({y: "sum", yhat:"sum"})
            smape = float(np.mean(np.abs(pred_df.y-pred_df.yhat)/(np.abs(pred_df.y) + np.abs(pred_df.yhat))/2) * 100)
        else:
            smape = float(np.abs(np.abs(np.sum(pred_df.y)) - np.abs(np.sum(pred_df.yhat)))/(np.abs(np.sum(pred_df.y))+ 
                                                                                            np.abs(np.sum(pred_df.yhat)))/2*100)
        return smape
    return f.udf(model_evaluation, t.FloatType())
