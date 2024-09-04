import logging
import logging.config

# Load the Logging configuration file
logging.config.fileConfig(fname='../util/logging_config.conf')
logger = logging.getLogger(__name__)


def get_curr_date(spark):
    try:
        opDF = spark.sql(""" select current_date """)
        logger.info("Validate the Spark object by printing Current Date - " + str(opDF.collect()))
    except Exception as exp:
        logger.error("Error in the method - get_curr_date. Please check the Stack Trace. " + str(exp), exc_info=True)
    else:
       logger.info("Spark object is validated and ready to use. ")


def df_count(df, dfName):
    try:
        logger.info(f"The dataframe validation by count df_count() for dataframe {dfName} has started... ")
        df_count = df.count()
        logger.info(f"The dataframe count is {df_count}")
    except Exception as exp:
        logger.error("Error in the method df_count(). Please check stack trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("The dataframe validation by df_count() is completed... ")


def df_top10_rec(df, dfName):
    try:
        logger.info(f"The dataframe validation by top 10 records df_top10_rec() for dataframe {dfName} has started... ")
        logger.info("The dataframe top 10 records are: ")
        df_pandas=df.limit(10).toPandas()
        logger.info('\n \t' + df_pandas.to_string(index=False))
    except Exception as exp:
        logger.error("Error in the method df_top10_rec(). Please check stack trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("The dataframe validation by df_top10_rec() is completed... ")