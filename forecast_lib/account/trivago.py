from __future__ import division, print_function
import logging
import os
import sys
import pandas as pd
from pyspark.sql import functions as f, types as t
from pyspark.sql import SparkSession, Window

spark = ( SparkSession
             .builder
             .getOrCreate() )

sc = spark.sparkContext

from forecast_lib.account.base import ForecastBase, MetricBase
# add the module to access mysql database
sys.path.append(os.path.join(os.getenv("HOME"),"git_tree/metasearch/python/utils/"))
import MetaUtils as mu

class account(ForecastBase):
    """
    Trivago account, child class of ForecastBase class, the child class specifies the Trivago account specific get_booker_cc,
    every other attributes inherit from ForecastBase class

    """
    def __init__(self,Metrics,run_date,forecast_horizon,optimizer,eval_level,pos,has_add_regressors,cv_type):
        super(account, self).__init__(Metrics,run_date,forecast_horizon,optimizer,eval_level,pos,has_add_regressors,cv_type)

    def get_booker_cc(self, account_partner_id = 413084):
        booker_cc_df = spark.table("default.bp_b_affiliate")\
                .where("partner_id = {partner_id}".format(partner_id = account_partner_id))\
                .select("name")\
                .withColumn('pos', f.regexp_extract(f.col("name"), "Trivago_(.*?)\.",1))\
                .select('pos')\
                .distinct()\
                .where("pos != ''").toPandas()
        return booker_cc_df

class GrossBookings(MetricBase):
    """
    GrossBookings (per Property) class, child class of MetricBase class, the child class obtain the defined metrics for the relevant account

    Attributes:
        start_date  : string, the start date to obtain performance
        end_date    : string, the end date to obtain performance
        pos         : string, point of sale, booker_country
    """

    def __init__(self,start_date,end_date,pos):

        self.metric_name = "gross_bookings"
        super(GrossBookings,self).__init__(self.metric_name,start_date,end_date,pos)

    def compute(self):

        trivagoStats = TrivagoStats(start_date=self.start_date,end_date=self.end_date,pos=self.pos,agg_on = ["yyyy_mm_dd", "pos"])
        return ( trivagoStats.get_stats_summary()
                   .select("yyyy_mm_dd","pos",self.metric_name) )

class RoomNights(MetricBase):
    """
    RoomNights class, child class of MetricBase class, the child class obtain the defined metrics for the relevant account

    Attributes:
        start_date  : string, the start date to obtain performance
        end_date    : string, the end date to obtain performance
        pos         : string, point of sale, booker_country
    """

    def __init__(self,start_date,end_date,pos):

        self.metric_name = "roomnights"
        super(RoomNights,self).__init__(self.metric_name,start_date,end_date,pos)

    def compute(self):

        trivagoStats = TrivagoStats(start_date=self.start_date,end_date=self.end_date,pos=self.pos,agg_on = ["yyyy_mm_dd", "pos"])
        return ( trivagoStats.get_stats_summary()
                   .select("yyyy_mm_dd","pos",self.metric_name) )

class CancelledRoomNights(MetricBase):
    """
    RoomNights class, child class of MetricBase class, the child class obtain the defined metrics for the relevant account

    Attributes:
        start_date  : string, the start date to obtain performance
        end_date    : string, the end date to obtain performance
        pos         : string, point of sale, booker_country
    """

    def __init__(self,start_date,end_date,pos):

        self.metric_name = "cancelled_roomnights"
        super(CancelledRoomNights,self).__init__(self.metric_name,start_date,end_date,pos)

    def compute(self):

        trivagoStats = TrivagoStats(start_date=self.start_date,end_date=self.end_date,pos=self.pos,agg_on = ["yyyy_mm_dd", "pos"])
        return ( trivagoStats.get_cancellations()
                   .select("yyyy_mm_dd","pos",self.metric_name) )

class Cancellations(MetricBase):
    """
    NitsProfit (per Property) class, child class of MetricBase class, the child class obtain the defined metrics for the relevant account

    Attributes:
        start_date  : string, the start date to obtain performance
        end_date    : string, the end date to obtain performance
        pos         : string, point of sale, booker_country
    """

    def __init__(self,start_date,end_date,pos):

        self.metric_name = "cancellations"
        super(Cancellations,self).__init__(self.metric_name,start_date,end_date,pos)

    def compute(self):

        trivagoStats = TrivagoStats(start_date=self.start_date,end_date=self.end_date,pos=self.pos,agg_on = ["yyyy_mm_dd", "pos"])
        return ( trivagoStats.get_cancellations()
                   .select("yyyy_mm_dd","pos",self.metric_name) )


class GrossCommission(MetricBase):
    """
    GrossProfit (per Property) class, child class of MetricBase class, the child class obtain the defined metrics for the relevant account

    Attributes:
        start_date  : string, the start date to obtain performance
        end_date    : string, the end date to obtain performance
        pos         : string, point of sale, booker_country
    """

    def __init__(self,start_date,end_date,pos):

        self.metric_name = "gross_commission"
        super(GrossCommission,self).__init__(self.metric_name,start_date,end_date,pos)

    def compute(self):

        trivagoStats = TrivagoStats(start_date=self.start_date,end_date=self.end_date,pos=self.pos,agg_on = ["yyyy_mm_dd", "pos"])
        return ( trivagoStats.get_stats_summary()
                   .select("yyyy_mm_dd","pos",self.metric_name) )

class CancelledCommission(MetricBase):
    """
    GrossProfit (per Property) class, child class of MetricBase class, the child class obtain the defined metrics for the relevant account

    Attributes:
        start_date  : string, the start date to obtain performance
        end_date    : string, the end date to obtain performance
        pos         : string, point of sale, booker_country
    """

    def __init__(self,start_date,end_date,pos):

        self.metric_name = "cancelled_commission"
        super(CancelledCommission,self).__init__(self.metric_name,start_date,end_date,pos)

    def compute(self):

        trivagoStats = TrivagoStats(start_date=self.start_date,end_date=self.end_date,pos=self.pos,agg_on = ["yyyy_mm_dd", "pos"])
        return ( trivagoStats.get_cancellations()
                   .select("yyyy_mm_dd","pos",self.metric_name) )

class TrivagoStats(object):
    """
    TrivagoStats class, obtain data for relevant performance metrics

    Attributes:
        start_date  : string, the start date to obtain performance
        end_date    : string, the end date to obtain performance
        agg_on      : dimensions to aggregate on, default to hotel_id, yyyy_mm_dd for smart salt, the lowest
                      aggregation level is hotel_id, yyyy_mm_dd, pos
        pos         : list or string, point of sale, booker_country
        max_rpb     : float, max revenue per booking as cutoff point to remove bookings with uncommon values
                      (normal or fraudulent)
        partner_id  : int, partner_id for specific account
        performance_table: string, performance table for an account
        reservation_table: string, reservation table in mysql for cancellation data source
        affiliate_table:   string, affiliate table which is joined with reservation table on afilliate id
    """

    def __init__(self,start_date,end_date,pos=['All'],
                max_rpb = 3000.0, partner_id = 413084,
                performance_table = 'spmeta.trivago_performance',
                reservation_table = 'default.dw_reservation',
                affiliate_table = 'default.bp_b_affiliate',
                agg_on = ['hotel_id', 'yyyy_mm_dd']):

        self.start_date = start_date
        self.end_date   = end_date
        self.pos        = [pos] if isinstance(pos, str) else pos
        self.max_rpb    = max_rpb
        self.partner_id = partner_id
        self.performance_table = performance_table
        self.reservation_table = reservation_table
        self.affiliate_table = affiliate_table
        self.agg_on = agg_on


    def get_stats_summary(self):
        """function to obtain performance stats for trivago at desired aggregated dimensions

        Returns:
            spark dataframe with performance metrics of nits_bookings,gross_bookings,nits_profit,gross_profit,
            cost, clicks, nits and gross commission at desired aggregated dimensions.
        """
        perf_table = spark.table(self.performance_table)\
                            .where("yyyy_mm_dd between '{start_date}' and '{end_date}'"
                                     .format(start_date = self.start_date, end_date = self.end_date))\
                            .where("clicks > 0")\
                            .where("commission_expected_euro <= {max_rpb}".format(max_rpb = self.max_rpb))

        if self.pos == ['All']:
            perf_table = perf_table.groupBy(*self.agg_on)\
                                .agg(f.sum("nits_bookings").alias("nits_bookings")
                                    ,f.sum("commission_expected_euro").alias("nits_commission")
                                    ,f.sum("bookings").alias("gross_bookings")
                                    ,f.sum("commission_amount_euro").alias("gross_commission")
                                    ,f.sum("cost_euro").alias("cost")
                                    ,f.sum("clicks").alias("clicks")
                                    ,f.sum("roomnights").alias("roomnights"))\
                               .withColumn("nits_profit",f.expr("nits_commission-cost"))\
                               .withColumn("gross_profit", f.expr("gross_commission-cost"))
        else:
            filtered_pos = spark.createDataFrame(pd.DataFrame(data = self.pos,
                                                              columns = ["pos"]))

            perf_table = perf_table.join(filtered_pos, on = "pos", how = "inner")\
                                .groupBy(*self.agg_on)\
                                .agg(f.sum("nits_bookings").alias("nits_bookings")
                                    ,f.sum("commission_expected_euro").alias("nits_commission")
                                    ,f.sum("bookings").alias("gross_bookings")
                                    ,f.sum("commission_amount_euro").alias("gross_commission")
                                    ,f.sum("cost_euro").alias("cost")
                                    ,f.sum("clicks").alias("clicks")
                                    ,f.sum("roomnights").alias("roomnights"))\
                               .withColumn("nits_profit",f.expr("nits_commission-cost"))\
                               .withColumn("gross_profit", f.expr("gross_commission-cost"))

        return (perf_table)

    def get_cancellations(self):
        """get cancellation data from mysql at desired aggregated dimension and filter for the selected point of
        sales.

        Returns: spark dataframe with cancelled commission, cancelled bookings and cancelled roomnights at desired
                 aggregated dimension
        """
        cancellation_query = """
            SELECT r.date_cancelled AS yyyy_mm_dd
                 , r.hotel_id
                 , regexp_extract(a.affiliate_name, '(.*)[_](.*)[.](.*)[.].*', 2) AS pos
                 , SUM(1) AS cancellations
                 , SUM(r.commission_amount_euro) AS commission_cancelled
                 , SUM(roomnights) AS roomnights
            FROM {reservation_table} r
            JOIN (SELECT id affiliate_id
                       , name affiliate_name
                  FROM {affiliate_table}
            WHERE partner_id = {partner_id}
                  ) a
            ON r.affiliate_id = a.affiliate_id
            WHERE  date_cancelled >= '{start_date}'
            AND  date_cancelled <= '{end_date}'
            AND  status not in ('fraudulent', 'test', 'unknown')
            GROUP BY r.date_cancelled
                   , r.hotel_id
                   , regexp_extract(a.affiliate_name, '(.*)[_](.*)[.](.*)[.].*', 2)
            """.format(reservation_table = self.reservation_table,
                        affiliate_table = self.affiliate_table,
                        start_date = self.start_date,
                        end_date = self.end_date,
                        trivago_partner_id = self.partner_id)

        cancellations = spark.sql(cancellation_query)

        cancellations_agg = cancellations.groupBy(*self.agg_on)\
                            .agg(f.sum("cancelled_commission").alias("cancelled_commission")
                                ,f.sum("cancelled_roomnights").alias("cancelled_roomnights")
                                ,f.sum("cancellations").alias("cancellations"))

        if self.pos == ['All']:
            return cancellations_agg
        else:
            filtered_pos = spark.createDataFrame(pd.DataFrame(data = self.pos, columns = ["pos"]))
            cancellations_agg = cancellations_agg.join(filtered_pos, on = "pos", how = "inner")
            return cancellations_agg

    def get_agg_stats(self):
        """function that join cancellations data with performance metrics at desired aggregated dimensions

        Returns:
            spark dataframe of performance stats at desired aggregated dimensions
        """
        cancellations_agg = self.get_cancellations()

        perf_table = self.get_stats_summary()

        agg_perf_table = perf_table.join(cancellations_agg, how = "outer", on = self.agg_on)\
                              .withColumn("asbooked_commission",f.expr("gross_commission-cancelled_commission"))\
                              .withColumn("asbooked_roomnights", f.expr("roomnights - cancelled_roomnights"))

        agg_perf_table = agg_perf_table.na.fill(0, subset = list(set(agg_perf_table.columns) -
                                                                 set([x for x in self.agg_on])))

        return (agg_perf_table)
