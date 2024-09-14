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
        logger.error(f"Error in the method df_count() for dataframe {dfName}. Please check stack trace. "
                     + str(exp), exc_info=True)
        raise
    else:
        logger.info(f"The dataframe {dfName} validation by df_count() is completed... ")


def df_top10_rec(df, dfName):
    try:
        logger.info(f"The dataframe validation by top 10 records df_top10_rec() for dataframe {dfName} has started... ")
        logger.info("The dataframe top 10 records are: ")
        #df_pandas=df.limit(10).toPandas()
        #logger.info('\n \t' + df_pandas.to_string(index=False))

        # Collect the top 10 records
        top10_records = df.limit(10).collect()

        # Extract column names
        columns = df.columns

        # Determine the maximum width for each column
        col_widths = [max(len(str(row[col])) if row[col] is not None else 0 for row in top10_records) for col in
                      columns]
        col_widths = [max(width, len(col)) for width, col in zip(col_widths, columns)]

        # Format header
        header = " | ".join(f"{col:{width}}" for col, width in zip(columns, col_widths))
        separator = "-+-".join('-' * width for width in col_widths)

        # Format rows
        rows = "\n".join(
            " | ".join(f"{row[col] if row[col] is not None else '':{width}}" for col, width in zip(columns, col_widths))
            for row in top10_records
        )

        # Combine header, separator, and rows
        table_str = f"{header}\n{separator}\n{rows}"

        # Log the top 10 records in tabular format
        logger.info("The dataframe top 10 records are: ")
        logger.info("\n" + table_str)
    except Exception as exp:
        logger.error(f"Error in the method df_top10_rec() for dataframe {dfName}. Please check stack trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info(f"The dataframe {dfName} validation by df_top10_rec() is completed... ")


def df_print_schema(df, dfName):
    try:
        logger.info(f"The dataframe print by df_print_schema has started... ")
        sch = df.schema.fields
        logger.info(f"The dataframe {dfName} schema is: ")
        for i in sch:
            logger.info(f"\t{i}")
    except Exception as exp:
        logger.error("Error in the method df_print_schema(). Please check stack trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info(f"The dataframe print by df_print_schema has completed... ")