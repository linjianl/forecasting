from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

def get_holidays_table(start_date):
    """get the cs holiday table from start date of the forecasting data until last availability
    
    Args: 
        start_date: string, start date of the forecasting data 
     
    Returns:
        panda dataframe with holidays
    """
    
    query = """
    SELECT 
        yyyy_mm_dd AS ds, 
        name AS holiday,
        upper(cc1) AS pos
    FROM 
        csa.holidays_2000_2030_cc1 
    WHERE 
        yyyy_mm_dd >= '{start_date}'
    AND 
        level = 1 

    """.format(start_date = start_date)
    holiday_table = spark.sql(query).toPandas()
    
    return holiday_table 
