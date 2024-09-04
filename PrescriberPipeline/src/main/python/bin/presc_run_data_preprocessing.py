import logging
import logging.config
from pyspark.sql.functions import upper

### Load the logging configuration file
logging.config.fileConfig(fname='../util/logging_config.conf')
logger = logging.getLogger(__name__)


def perform_data_clean(df1, df2):
    ### Clean df_city dataframe
    # select only required columns
    # Apply upper function for columns city, state and county_name

    try:
        logger.info("perform_data_clean() is started...")
        df_city_select = df1.select(upper(df1.city).alias("city"),
                                   df1.state_id,
                                   upper(df1.state_name).alias("state_name"),
                                   upper(df1.county_name).alias("county_name"),
                                   df1.population,
                                   df1.zips)


    # clean df_fact dataframe
        # select only required columns
        # rename columns
        # add country field 'USA'
        # clean years_of_exp field
        # convert years_of_exp datatype from string to number
        # combine first and last name
        # check and clean all the null / NaN values
        # impute TRX_CNT where it is null as avg of trx_cnt for the prescriber
        df_fac_select = df2.select(df2.npi.alias("presc_id"),
                                   df2.nppes_provider_last_org_name.alias("presc_lname"),
                                   df2.nppes_provider_first_name.alias("presc_fname"),
                                   df2.nppes_provider_city.alias("presc_city"),
                                   df2.nppes_provider_state.alias("presc_state"),
                                   df2.speciality_description.alias("presc_spclt"),
                                   df2.years_of_exp,
                                   df2.drug_name,
                                   df2.total_claim_count.alias("trx_cnt"),
                                   df2.total_day_supply,
                                   df2.total_drug_cost)
    except Exception as exp:
        logger.error("Error in the method - perform_data_clean(). Please check the stack trace. " \
                     + str(exp), exc_info=True)
        df_city_select = df1.limit(0)
        df_fact_select = df2.limit(0)
    else:
        logger.info("perform_data_clean() is completed...")
    return df_city_select, df_fact_select
