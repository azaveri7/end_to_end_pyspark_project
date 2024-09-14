"""
### City Report
# Transform logic:
# 1. Calculate the number of zips in each state
# 2. Calculate the number of distinct prescribers assigned for each city
# 3. Calculate total TRX_CNT prescribed for each city
# 4. Do not report a city in the final report if no prescriber is assigned to it

# Layout:
    # City Name
    # State name
    # country name
    # City population
    # number of zips
    # prescriber counts
    # total trx counts
"""

from pyspark.sql.functions import upper, size, countDistinct, sum
from pyspark.sql.window import Window
from udfs import column_split_cnt

import logging
import logging.config

### Load the logging configuration file
logging.config.fileConfig(fname='../util/logging_config.conf')
logger = logging.getLogger(__name__)


def city_report(df_city_sel, df_fact_sel):
    logger.info("city_report is started...")
    try:
        logger.info(f"Transform - city_report() is started...")
        df_city_split = df_city_sel.withColumn('zip_counts', column_split_cnt(df_city_sel.zips))
        df_fact_grp = df_fact_sel.groupBy(df_fact_sel.presc_state, df_fact_sel.presc_city).agg(
            countDistinct("presc_id").alias("presc_counts"), sum("trx_cnt").alias("trx_counts"))
        df_city_join = df_city_split.join(df_fact_grp, (df_city_split.state_id == df_fact_grp.presc_state) & (
                    df_city_split.city == df_fact_grp.presc_city), 'inner')
        df_city_final = df_city_join.select("city", "state_name", "county_name", "population", "zip_counts",
                                            "trx_counts", "presc_counts")
    except Exception as exp:
        logger.error("city_report is failed with exception " + str(exp), exc_info=True )
        raise
    else:
        logger.info("city_report is complete...")
    return df_city_final
