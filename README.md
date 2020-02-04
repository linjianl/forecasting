# Forecasting Daily Metrics

code for forecasting daily metrics.

Pipeline consists of
1. data preparation, possible data transformation using box-cox/log
2. holiday table preparation
3. add additional regressors for each corresponding metrics
4. set baseline model with default prophet modle hyperparameter configuration with holiday preparation
5. hyperparameter search using random grid search and compare the best parameters with baseline model
6. use the best model to predict using the entire dataset for future horizons of data  

### Forecasting using
specify the following parameters when you run the code for forecasting


**account**: trivago/tripadvisor/gha/kayak, note that there is no default for account, so one has to specify the account from command line<br/>
**run_date**: default to today, usually do not need to specify<br/>
**optimizer**: ***default*** or ***random_search***, for long horizon, use default configuration.
**forecast_horizon**: integer, ***in days***, how many days to be forecasted  
**eval_level**: daily, weekly, quarterly SMAPE (symmetric mean absolute percentage error).
**has_add_regressors**: boolean, True or False. It indicates whether additional regressor is needed or not.
**pos**, string, All or specific point of sales, default to all. When default to all, the model predicts at the account level
**metric_class**: string, names of metrics that you want to predict. options are: GrossBookings,RoomNights,CancelledRoomNights,Cancellations,GrossCommission,CancelledCommission
**cv_type**: string, default to ***backtesting*** (fixed origin increasing horizon) or ***rolling_window***

Below gives example for how to run the code. If you want to forecast for trivago grossBookings, with a forecast horizon of 13 weeks (7*13 = 91 days) with random grid search for hyperparameters and you have an additional regressor specified. 

```python
/opt/spark/current20/bin/spark-submit daily_metrics_pred.py  --account trivago --forecast_horizon 91 --metric_class GrossBookings --has_add_regressors True --optimizer random_search
2> /dev/null
```

