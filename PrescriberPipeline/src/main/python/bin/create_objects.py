from pyspark.sql import SparkSession
import logging
import logging.config

# Load the Logging configuration file
logging.config.fileConfig(fname='../util/logging_config.conf')
logger = logging.getLogger("create_objects")


def get_spark_object(envn, appName):
    try:
        logger.info(f"get_spark_object() is started. The '{envn} environment is used. ")
        if envn == 'TEST':
            master = 'local'
        else:
            master = 'yarn'
        spark = SparkSession \
            .builder \
            .master(master) \
            .appName(appName) \
            .getOrCreate()
    except Exception as exp:
        logger.error("Error in the method get_spark_object. Please check the stack trace. " + str(exp), exc_info=True)
        raise
    return spark
