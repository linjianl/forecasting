import numpy as np
import pandas as pd
from sklearn.model_selection import ParameterSampler
from forecast_lib.utils.ts_cross_validation import ts_train_test_backtest_split, ts_train_test_split

from pyspark.sql import functions as f, types as t
from pyspark.sql import SparkSession, Window
spark = SparkSession.builder.getOrCreate()

def gen_params_indices(df,  pred_size, train_length = 730, n_iter = 30, random_seed = 1234, cv_type = "rolling_window"):

    # these hyperparameters are manually chosen given experience.
    # I tested a number of hyperparameters and find that these are the most important ones.

    param_dict = {'growth': ['linear','logistic'],
                  'changepoint_prior_scale' : [float(x) for x in (np.arange(0.001, 0.6,0.003))],
                  'n_changepoints' : range(15,40,1),
                  'seasonality_mode' : ['multiplicative', 'additive'],
                  'changepoint_range': [float(x) for x in (np.linspace(0.8, 0.99, 20))]}

    params = ParameterSampler(param_dict, n_iter = n_iter, random_state = random_seed)

    if cv_type == "backtesting":
        train_indices = ts_train_test_backtest_split(df, train_length, pred_size)
    elif cv_type == "rolling_window":
        train_indices = ts_train_test_split(df, train_length, pred_size, cv_size = None, random_seed = random_seed)

    param_ind = pd.DataFrame(data=zip(list(params), [[x[0] for x in train_indices] for i in range(len(params))]),
                          columns = ['params','train_indices'])
    param_ind['train_indices'] = param_ind['train_indices'].apply(lambda x: [map(int, i) for i in x])

    schema = t.StructType([
                t.StructField('params',
                    t.StructType([
                            t.StructField('changepoint_prior_scale', t.FloatType(), nullable=False),
                            t.StructField('growth', t.StringType(), nullable=False),
                            t.StructField('n_changepoints',t.IntegerType(), nullable=False),
                            t.StructField('changepoint_range', t.FloatType(), nullable=False),
                            t.StructField('seasonality_mode', t.StringType(), nullable=False),
                                ]), nullable=False),
                t.StructField('train_indices', t.ArrayType(t.ArrayType(t.IntegerType(),containsNull=False),containsNull=False),
                              nullable=False)
            ])
    params_sp = spark.createDataFrame(param_ind, schema = schema)

    return (params_sp)
