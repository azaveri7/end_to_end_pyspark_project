import logging.config

# Load the Logging configuration file
logging.config.fileConfig(fname='../util/logging_config.conf')
logger = logging.getLogger(__name__)


def load_files(spark, file_dir, file_format, header, inferSchema):
    try:
        logger.info("The load_files() is started...")
        if file_format == 'parquet':
            df = spark \
                    .read \
                    .format(file_format) \
                    .load(file_dir)
        elif file_format == 'csv':
            df = spark \
                    .read \
                    .format(file_format) \
                    .options(header=header) \
                    .options(inferSchema=inferSchema) \
                    .load(file_dir)

    except Exception as exp:
        logger.error("Error in the method - load_files(). Please check the stack trace. ", exc_info=True)
        raise
    else:
        logger.info(f"The input file {file_dir} is loaded to the data frame. The load_files() function is completed. ")
    return df
