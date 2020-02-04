# /opt/spark/current20/bin/spark-submit daily_metrics_pred.py  --account gha 2> /dev/null
from __future__ import division, print_function
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import hashlib
import logging
import importlib
import numpy as np
import argparse

from pyspark.sql import functions as f, types as t
from pyspark.sql import SparkSession

NUM_PARTITIONS = 1024
spark = ( SparkSession
             .builder
             .config("spark.app.name","prophetPredictionConfig")
             .config("spark.yarn.queue","root.sp_analytics.spark")
             .config("spark.default.serializer","org.apache.spark.serializer.KryoSerializer")
             .config("spark.speculation","true")
             .config("spark.default.parallelism",str(NUM_PARTITIONS))
             .config("spark.sql.shuffle.partitions",str(NUM_PARTITIONS))
             .config("spark.yarn.executor.memoryOverhead", str(8192))
             .enableHiveSupport()
             .getOrCreate() )

sc = spark.sparkContext

def main():

    logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)-10s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--account", type=str) 
    parser.add_argument("--run_date", type=str, default=str(datetime.today().date()))
    parser.add_argument("--optimizer", type=str, default="random_search")
    parser.add_argument("--forecast_horizon", type=int, default=90) # 
    parser.add_argument("--eval_level", type=str, default="daily") 
    parser.add_argument("--has_add_regressors", type = bool, default = False)
    parser.add_argument("--pos", nargs = '+', type=str, default="All")
    parser.add_argument("--metric_class", type=str, default="GrossBookings") 
    parser.add_argument("--cv_type", type=str, default = "rolling_window")
    args = parser.parse_args()

    logging.info("Initialising with the following parameters: {args}".format(args = args))

    #  file zipping
    current_dir = os.path.dirname(os.path.realpath(__file__))
    zip_module_dir = current_dir[:current_dir.find('scripts')]
    mod_file = os.path.join(zip_module_dir, "forecast_lib/utils/zip_module.py")
    sc.addPyFile(mod_file)

    mod_zipping = importlib.import_module("zip_module")
    mod_zipping.zip_and_ship_module(zip_module_dir)
    zipped_file = os.path.join(current_dir,"forecast_lib.zip")
    sc.addPyFile(zipped_file)

    # dynamically load modules and metricBase class based on the account name, and relevant metrics
    mod = importlib.import_module("forecast_lib.account.{0}".format(args.account))
    Metrics = getattr(mod, args.metric_class)
   
    daily_metrics = mod.account(Metrics,
                                run_date = args.run_date,
                                forecast_horizon=args.forecast_horizon,
                                optimizer = args.optimizer,
                                eval_level = args.eval_level,
                                pos = args.pos,
                                has_add_regressors = args.has_add_regressors,
                                cv_type = args.cv_type)

    prediction, weekly_aggregates_yoy,components_fig = daily_metrics.use_prophet()

    prediction.to_csv("{account}_{metric_name}_daily_prediction.csv".format(account = args.account, metric_name = args.metric_class),
                     index = False)
    weekly_aggregates_yoy.to_csv("{account}_{metric_name}_weekly_aggregates_yoy.csv".format(account = args.account,
                                                                                            metric_name = args.metric_class),
                                index = False)
    components_fig.savefig("{account}_{metric_name}_components_fig.pdf".format(account = args.account, metric_name = args.metric_class))
    logging.info("Forecasting is completed. Please check your local directory for component plot and the forecasted daily and weekly data")
    # remove the zipped file from directory
    os.remove("forecast_lib.zip")

if __name__ == "__main__":
    main()
