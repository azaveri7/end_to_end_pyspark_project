import logging
import logging.config
from pyspark.sql.functions import upper, lit, regexp_extract, col, cast, concat_ws, count, when, isnull, isnan, avg, round, coalesce
from pyspark.sql.window import Window

### Load the logging configuration file
logging.config.fileConfig(fname='../util/logging_config.conf')
logger = logging.getLogger(__name__)


def perform_data_clean(df1, df2):
    ### Clean df_city dataframe
    # select only required columns
    # Apply upper function for columns city, state and county_name

    try:
        logger.info("perform_data_clean() is started for df_city_select ...")
        df_city_select = df1.select(upper(df1.city).alias("city"),
                                    df1.state_id,
                                    upper(df1.state_name).alias("state_name"),
                                    upper(df1.county_name).alias("county_name"),
                                    df1.population,
                                    df1.zips)

        # clean df_fact dataframe
        # select only required columns
        # rename columns
        logger.info("perform_data_clean() is started for df_fact_select ...")
        df_fact_select = df2.select(df2.npi.alias("presc_id"),
                                    df2.nppes_provider_last_org_name.alias("presc_lname"),
                                    df2.nppes_provider_first_name.alias("presc_fname"),
                                    df2.nppes_provider_city.alias("presc_city"),
                                    df2.nppes_provider_state.alias("presc_state"),
                                    df2.specialty_description.alias("presc_spclt"),
                                    df2.year_exp,
                                    df2.drug_name,
                                    df2.total_claim_count.alias("trx_cnt"),
                                    df2.total_day_supply,
                                    df2.total_drug_cost)
        # add country field 'USA'
        df_fact_select = df_fact_select.withColumn('country_name', lit("USA"))
        # clean years_of_exp field
        pattern = '\d+'
        index = 0
        df_fact_select = df_fact_select.withColumn("year_exp", regexp_extract(col("year_exp"), pattern, index))
        # convert years_of_exp datatype from string to number
        df_fact_select = df_fact_select.withColumn("year_exp", col("year_exp").cast("int"))
        # combine first and last name
        df_fact_select = df_fact_select.withColumn("presc_fullname", concat_ws(" ", "presc_fname", "presc_lname"))
        df_fact_select = df_fact_select.drop("presc_fname", "presc_lname")
        # check and clean all the null / NaN values
        #df_fact_select = df_fact_select\
            #.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_fact_select.columns]).show()
        # delete the records where the presc_id is null
        df_fact_select = df_fact_select.dropna(subset="presc_id")
        # delete the records where the drug_name is null
        df_fact_select = df_fact_select.dropna(subset="drug_name")
        # impute TRX_CNT where it is null as avg of trx_cnt for the prescriber
        spec = Window.partitionBy("presc_id")
        df_fact_select = df_fact_select.withColumn('trx_cnt', coalesce("trx_cnt", round(avg("trx_cnt").over(spec))))
        df_fact_select = df_fact_select.withColumn("trx_cnt", col("trx_cnt").cast('integer'))
        # check and clean all the null / NaN values
        df_fact_select.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_fact_select.columns]).show()
    except Exception as exp:
        logger.error("Error in the method - perform_data_clean(). Please check the stack trace. " \
                     + str(exp), exc_info=True)
        df_city_select = df1.limit(0)
        df_fact_select = df2.limit(0)
    else:
        logger.info("perform_data_clean() is completed...")
    return df_city_select, df_fact_select
