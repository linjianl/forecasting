import numpy as np

def evaluation_smape(true_df, forecast_df, forecast_horizon, eval_level = "daily"):
    """return the symmetric mean absolute percentage error given the desirable level of aggregation
    of data needed. note that for aggregation higher than weekly, just calculate
    the overall sum difference. the advantage of smape over mape is detailed here
    https://en.wikipedia.org/wiki/Mean_absolute_percentage_error
    https://en.wikipedia.org/wiki/Symmetric_mean_absolute_percentage_error
    
    params:
        eval_level: string, level of desired evaluation aggregation,
                    since we are sometimes interested in a weekly aggregate of forecasted
                    number, it seems to make sense to evaluate a model based on the weekly
                    mape rather than daily
        true_df:    pandas data frame, observed metrics
        pred_df:    pandas data frame, predicted metrics
    
    returns:
        float 
    """

    pred_df = forecast_df[["ds","yhat"]].set_index("ds").tail(forecast_horizon)
    pred_df["y"] = true_df.set_index("ds")['y']
    pred_df = pred_df.round({"yhat": 0})

    if eval_level == "daily":
        smape = np.mean(np.abs(pred_df.y-pred_df.yhat)/(np.abs(pred_df.y) + np.abs(pred_df.yhat))/2) * 100
    elif eval_level == "weekly":
        pred_df["week"] = pred_df.index.weekofyear
        pred_df = pred_df.groupby("week").agg({y: "sum", yhat:"sum"})
        smape = np.mean(np.abs(pred_df.y-pred_df.yhat)/(np.abs(pred_df.y) + np.abs(pred_df.yhat))/2) * 100
    else:
        smape = np.abs(np.abs(np.sum(pred_df.y)) - np.abs(np.sum(pred_df.yhat)))/(np.abs(np.sum(pred_df.y))+ np.abs(np.sum(pred_df.yhat)))/2*100
    return smape
