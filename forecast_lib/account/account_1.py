from __future__ import division, print_function
import logging
import os
import pandas as pd
import sys
from pyspark.sql import functions as f, types as t
from pyspark.sql import SparkSession, Window

spark = ( SparkSession
             .builder
             .getOrCreate() )

sc = spark.sparkContext

from forecast_lib.account.base import ForecastBase, MetricBase
sys.path.append(os.path.join(os.getenv("HOME"),"git_tree/partners/account_1/utils/"))
from utils import AccountOneStats
# add the module to access mysql database
sys.path.append(os.path.join(os.getenv("HOME"),"git_tree/partners/python/utils/"))
import MetaUtils as mu

class account(ForecastBase):
    """account_1 account, child class of Forecast class, the child class specifies the account_1 account specific get_booker_cc function
       every other attributes inherit from forecastBase class
    """
    def __init__(self,Metrics,run_date,forecast_horizon,optimizer,eval_level,pos,has_add_regressors,cv_type):
        super(account, self).__init__(Metrics,run_date,forecast_horizon,optimizer,eval_level,pos,has_add_regressors,cv_type)


    def get_booker_cc(self):
        """function to get the booker country for an account

        Returns:
            a panda dataframe with booker country
        """
        # get the campaign id, campaign name correspondence
        campaign_query = """
        SELECT id AS campaign_id,
               name AS campaign_name
          FROM mrktctl.account_1Bid_Campaign
        """
        msql = mu.getConnSql()
        msql.execute("USE mrktctl")
        campaigns = pd.read_sql_query(campaign_query, con= msql)
        msql.close()

        # get the pos, campaign id correspondence because in account_1, campaign could be
        # one pos or multiple pos
        cid = spark.table("spmeta.account_1_cc_campaign").withColumnRenamed("cc", "pos")
        pd_cid = cid.toPandas()
        pd_cid.loc[pd_cid.pos == "TR","campaign_id"] = 105
        booker_cc_df = campaigns.merge(pd_cid)

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

        accountOneStats = AccountOneStatsExtended(start_date=self.start_date,end_date=self.end_date,pos=self.pos,
                                   agg_on = ["yyyy_mm_dd", "pos"])
        return ( accountOneStats.get_booking_summary_extended()
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

        accountOneStats = AccountOneStatsExtended(start_date=self.start_date,end_date=self.end_date,pos=self.pos,
                               agg_on = ["yyyy_mm_dd", "pos"])
        return ( accountOneStats.get_booking_summary_extended()
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

        accountOneStats = AccountOneStatsExtended(start_date=self.start_date,end_date=self.end_date,pos=self.pos,
                               agg_on = ["yyyy_mm_dd", "pos"])
        return ( accountOneStats.get_cancellations()
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

        accountOneStats = AccountOneStatsExtended(start_date=self.start_date,end_date=self.end_date,pos=self.pos,
                               agg_on = ["yyyy_mm_dd", "pos"])
        return ( accountOneStats.get_cancellations()
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

        accountOneStats = AccountOneStatsExtended(start_date=self.start_date,end_date=self.end_date,pos=self.pos,
                               agg_on = ["yyyy_mm_dd", "pos"])
        return ( accountOneStats.get_booking_summary_extended()
                   .select("yyyy_mm_dd","pos",self.metric_name))

class CancelledCommission(MetricBase):
    """
    CancelledCommission class, child class of MetricBase class, the child class obtain the defined metrics for the relevant account

    Attributes:
        start_date  : string, the start date to obtain performance
        end_date    : string, the end date to obtain performance
        pos         : string, point of sale, booker_country
    """

    def __init__(self,start_date,end_date,pos):

        self.metric_name = "cancelled_commission"
        super(GrossCommission,self).__init__(self.metric_name,start_date,end_date,pos)

    def compute(self):

        accountOneStats = AccountOneStatsExtended(start_date=self.start_date,end_date=self.end_date,pos=self.pos,
                               agg_on = ["yyyy_mm_dd", "pos"])
        return ( accountOneStats.get_cancellations()
                   .select("yyyy_mm_dd","pos",self.metric_name) )


class AccountOneStatsExtended(AccountOneStats):
    """extended class of account_1 stats from partners/account_1/utils/util.py to ensure consistency.
    This is necessary in case changes in how account_1 performance stats are calculated,
    for example, when there is a new cost table

    Attributes:
        start_date  : string, the start date to obtain performance
        end_date    : string, the end date to obtain performance
        agg_on      : dimensions to aggregate on, default to hotel_id, yyyy_mm_dd for smart salt, the lowest
                      aggregation level is hotel_id, yyyy_mm_dd, pos
        pos         : list or string, point of sale, booker_country
        max_rpb     : float, max revenue per booking as cutoff point to remove bookings with uncommon values
                      (normal or fraudulent)
        partner_id  : int, partner_id for specific account
        reservation_table: string, reservation table in hive for cancellation data source
        affiliate_table:   string, affiliate table which is joined with reservation table on afilliate id
    """
    def __init__(self,start_date,end_date,alpha=1.0,partner_id=423463,max_rpb=3000.,
                 debug=False,effective_clicks=True,use_pst=True,filter_noclicks=True,
                 agg_on = ['hotel_id', 'yyyy_mm_dd'],
                 reservation_table = 'default.dw_reservation',
                 affiliate_table = "default.bp_b_affiliate",
                 pos=['All']):

        self.pos = pos if isinstance(pos, list) else [pos]
        self.agg_on  = agg_on
        self.reservation_table  = reservation_table
        self.affiliate_table    = affiliate_table
        self.cid_map = sc.broadcast(super(AccountOneStatsExtended, self)._get_cc2cid_mapping())
        super(AccountOneStatsExtended, self).__init__(start_date,end_date,alpha,partner_id,max_rpb,
                                      debug,effective_clicks,use_pst,filter_noclicks)

    def get_id_pos(self):
        """get campaign id, campaign correspondence from mysql database and join with the filtered campaign lists

        returns: spark dataframe
        """
        # get the campaign id and campaign name correspondence from mysql
        msql = mu.getConnSql()
        msql.execute('USE mrktctl')
        qs = """
        SELECT id campaign_id,
               name pos
          FROM account_1Bid_Campaign"""
        account_1_campaign = pd.read_sql_query(qs, con=msql)
        msql.close()
        account_1_campaign  = spark.createDataFrame(account_1_campaign)

        # filter for the campaign list
        if self.pos == ['All']:
            return account_1_campaign
        else:
            filtered_pos = spark.createDataFrame(pd.DataFrame(data = self.pos, columns = ["pos"]))
            account_1_campaign = account_1_campaign.join(filtered_pos, on = "pos", how = "inner")
            return account_1_campaign

    def get_booking_summary_extended(self):
        """get reservation related metrics for account_1 account, inherit the method from accountOneStats.get_booking_summary()
        and join with filtered campaigns and obtain the relevant metrics

        returns: spark dataframe
        """
        # first inherit from the parent class
        perf_table =  super(AccountOneStatsExtended, self).get_booking_summary()
        # join with campaign table
        account_1_campaign = self.get_id_pos()
        perf_table_filtered = perf_table.join(account_1_campaign, on = "campaign_id", how = "inner")\
                                .groupBy(*self.agg_on)\
                                .agg(f.sum("bookings").alias("gross_bookings")
                                    ,f.sum("nits_bookings").alias("nits_bookings")
                                    ,f.sum("commission").alias("gross_commission")
                                    ,f.sum("nits_commission").alias("nits_commission")
                                    ,f.sum("roomnights").alias("roomnights"))

        return perf_table_filtered

    def get_stats_summary(self):
        """get stats summary for account_1 account, inherit the method from accountOneStats.get_stats_report()
        and join with filtered campaigns and obtain the relevant metrics

        returns: spark dataframe
        """
        #if not agg_on: agg_on = self.agg_on
        # first inherit from the parent class
        perf_table =  super(AccountOneStatsExtended, self).get_stats_report()
        # join with campaign table
        account_1_campaign = self.get_id_pos()
        perf_table_filtered = perf_table.join(account_1_campaign, on = "campaign_id", how = "inner")\
                                .groupBy(*self.agg_on)\
                                .agg(f.sum("bookings").alias("gross_bookings")
                                    ,f.sum("nits_bookings").alias("nits_bookings")
                                    ,f.sum("nits_profit").alias("nits_profit")
                                    ,f.sum("commission").alias("gross_commission")
                                    ,f.sum("nits_commission").alias("nits_commission")
                                    ,f.sum("cost").alias("cost")
                                    ,f.sum("clicks").alias("clicks")
                                    ,f.sum("roomnights").alias("roomnights"))\
                                .withColumn("gross_profit", f.expr("gross_commission - cost"))\
                                .withColumn("nits_profit", f.expr("nits_commission - cost"))

        return perf_table_filtered

    def get_cancellations(self):
        """get cancellation data, lowest dimension is at campaign level, so only
        the campaign id is parsed from label. The hotel id is not parsed because
        we are interested in which hotels that got booked in the end get cancelled,
        not those hotels that get clicked (parsing from the label for hotel id correspond
        to notion of last click attribution)

        returns: spark dataframe
        """

        def extract_aff_label(aff_name,info_type):
            """ function copied from the account_1stats, this is not ideal as this is a function
            embedded in another function
            # udf to obtain the cc, device and placement from the affiliate name
            # note that the cc from this table contains options like AOW and ROW
            # very likely there's no match here
            """
            try:
                if info_type == "cc":
                    return aff_name.split(".")[0].split("_")[1]
                elif info_type == "placement":
                    placement = aff_name.split(".")[1]
                    if placement == "LU":
                        return "localuniversal"
                    elif placement == "MR":
                        return "mapresults"
                    else:
                        return None
                elif info_type == "device":
                    device = aff_name.split(".")[2]
                    if device == "PC":
                        return "desktop"
                    elif device in ("Mobile","Tablet"):
                        return device.lower()
                    else:
                        return None
                else:
                    return None
            except:
                return None

        spark.udf.register("extract_aff_label",extract_aff_label,
                           returnType=t.StringType())

        # return everything as StringType first,
        # will correct for this later on
        def extract_res_label(label):
            """function copied from the account_1stats, this is not ideal as this is a function
            embedded in another function, this is a udf to extract relevant information from label
            of reservations
            """
            temp = label.split("_")
            info_dict = {}
            for x in temp:
                data = x.split("-")
                if len(data) == 2:
                    info_dict[data[0]] = data[1]
                else:
                    if "mapresults" in x.lower():
                        info_dict["placement"] = "mapresults"
                    elif "localuniversal" in x.lower():
                        info_dict["placement"] = "localuniversal"

                    if "hotel-" in x.lower():
                        try:
                            info_dict["hotel_id"] = x.split("hotel-")[1]
                        except:
                            info_dict["hotel_id"] = None

            return info_dict

        spark.udf.register("extract_res_label",extract_res_label,
                           returnType=t.MapType(t.StringType(),t.StringType()))

        aff_id = spark.table(self.affiliate_table)\
                     .where("partner_id = 423463")\
                     .selectExpr("id as affiliate_id"
                     ,"extract_aff_label(name,'cc') aff_cc"
                     ,"extract_aff_label(name,'placement') aff_placement"
                     ,"extract_aff_label(name,'device') aff_device")

        # get cancelled reservation between start and end_date
        cancelled_reservations = ( spark.table(self.reservation_table)
            .withColumn("date_cancelled",f.expr("to_date(date_cancelled)"))
            .withColumnRenamed("id","hotelreservation_id")
            .where("date_cancelled between '{start_date}' and '{end_date}'"
                   .format(start_date=self.start_date,end_date=self.end_date))
            .join(spark.table("fpa.device_class_lookup").select("hotelreservation_id","device_class"),
                    on="hotelreservation_id",how="inner")
            .where("status not in ('fraudulent', 'test', 'unknown')")
            .join(f.broadcast(aff_id),on="affiliate_id",how="inner")
            .selectExpr("date_cancelled","label","upper(booker_cc1) booker_cc1",
                        "hotelreservation_id", "hotel_id", "aff_cc", "roomnights",
                        "commission_amount_euro") )

        # grab information from the label
        cancelled_reservations_label = ( cancelled_reservations
            .withColumn("label_map",f.expr("extract_res_label(label)"))
            .withColumn("label_cid",f.expr("cast(coalesce(label_map['cid'],get_cid(label_map['ucc'])) as int)"))
            .drop("label_map")
            .cache() )

        # only keep the coalescing of campaign id and select only relevant columns
        can_res_cleaned = ( cancelled_reservations_label.selectExpr(
            "hotelreservation_id"
            ,"to_date(date_cancelled) yyyy_mm_dd"
            ,"hotel_id"
            ,"coalesce(label_cid,get_cid(aff_cc),get_cid(booker_cc1),66) campaign_id"
            ,"commission_amount_euro cancelled_commission"
            ,"roomnights cancelled_roomnights"
            , "1 cancellations") )

        # filter for relevant campaigns
        account_1_campaign = self.get_id_pos()
        can_res_cleaned = can_res_cleaned.join(account_1_campaign, on = "campaign_id", how = "inner")

        cancellations_agg = can_res_cleaned.groupBy(*self.agg_on)\
                    .agg(f.sum("cancelled_commission").alias("cancelled_commission")
                        ,f.sum("cancelled_roomnights").alias("cancelled_roomnights")
                        ,f.sum("cancellations").alias("cancellations"))

        return cancellations_agg

    def get_agg_stats(self):
        """function that join cancellations data with performance metrics at
        desired aggregated dimensions, the current aggregated dimension remove the
        hotel_id, this can be easily adapted by just group by self.agg_on

        Returns:
            spark dataframe
        """
        cancellations_agg = self.get_cancellations()\
                    .groupBy(*self.agg_on)\
                     .agg(f.sum("cancellations").alias("cancellations"),
                          f.sum("cancelled_commission").alias("cancelled_commission"),
                          f.sum("cancelled_roomnights").alias("cancelled_roomnights"))

        perf_table = self.get_stats_summary()\
                        .groupBy(*self.agg_on)\
                        .agg(f.sum("nits_bookings").alias("nits_bookings")
                             ,f.sum("nits_commission").alias("nits_commission")
                             ,f.sum("gross_bookings").alias("gross_bookings")
                             ,f.sum("gross_commission").alias("gross_commission")
                             ,f.sum("cost").alias("cost")
                             ,f.sum("clicks").alias("clicks")
                             ,f.sum("nits_profit").alias("nits_profit")
                             ,f.sum("gross_profit").alias("gross_profit")
                             ,f.sum("roomnights").alias("roomnights"))

        agg_perf_table = perf_table.join(cancellations_agg, how = "outer",
                                             on = self.agg_on)\
                                    .withColumn("asbooked_commission",f.expr("gross_commission-cancelled_commission"))\
                                    .withColumn("asbooked_roomnights", f.expr("roomnights-cancelled_roomnights"))

        agg_perf_table = agg_perf_table.na.fill(0, subset = list(set(agg_perf_table.columns) -
                                                                 set([x for x in self.agg_on])))

        return agg_perf_table
