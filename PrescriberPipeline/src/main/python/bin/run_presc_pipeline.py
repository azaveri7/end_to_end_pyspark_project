import os
import sys

import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_curr_date, df_count, df_top10_rec, df_print_schema
from presc_run_data_ingest import load_files
from presc_run_data_preprocessing import perform_data_clean
from presc_run_data_transform import city_report, top_5_Prescribers
import logging
import logging.config

### Load the logging configuration file
logging.config.fileConfig(fname='../util/logging_config.conf')


def main():
    logging.info("main() is started...")
    try:
        # Get spark object
        spark = get_spark_object(gav.envn, gav.appName)
        logging.info("spark object is created")
        # validate spark object
        get_curr_date(spark)
        ### Initiate run_presc_data ingest script
        # Load the City file

        for file in os.listdir(gav.staging_dim_city):
            logging.info("file is " + file)
            # Assuming staging_dim_city is already correctly set
            file_dir = os.path.join(gav.staging_dim_city, file)
            # Normalize the path to ensure correct format
            file_dir = os.path.normpath(file_dir)
            logging.info("file_dir is " + file_dir)
            if file.split('.')[1] == 'csv':
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
            elif file.split('.')[1] == 'parquet':
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

        df_city = load_files(spark=spark, file_dir=file_dir, \
                             file_format=file_format, header=header, inferSchema=inferSchema)
        ### Validate run_data_ingest script for city dimention dataframe
        df_count(df_city, 'df_city')
        df_top10_rec(df_city, 'df_city')

        # Load the Prescriber Fact file
        for file in os.listdir(gav.staging_fact):
            logging.info("file is " + file)
            # Assuming staging_dim_city is already correctly set
            file_dir = os.path.join(gav.staging_fact, file)
            # Normalize the path to ensure correct format
            file_dir = os.path.normpath(file_dir)
            logging.info("file_dir is " + file_dir)
            if file.split('.')[1] == 'csv':
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
            elif file.split('.')[1] == 'parquet':
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
        df_fact = load_files(spark=spark, file_dir=file_dir, \
                             file_format=file_format, header=header, inferSchema=inferSchema)
        ### Validate run_data_ingest script for city fact dataframe
        df_count(df_fact, 'df_fact')
        df_top10_rec(df_fact, 'df_fact')

        ### Initiate run_presc_data_preprocessing script
        # Perform data cleaning operation for df_city
        # select only required columns
        # convert city, state and country fields to upper case
        df_city_sel, df_fact_sel = perform_data_clean(df_city, df_fact)
        # validate
        df_count(df_city_sel, 'df_city_sel')
        df_top10_rec(df_city_sel, 'df_city_sel')
        df_count(df_fact_sel, 'df_fact_sel')
        df_top10_rec(df_fact_sel, 'df_fact_sel')

        df_print_schema(df_fact_sel, 'df_fact_sel')

        ### Initiate run_presc_data_transform script
        # Develop city and prescriber reports
        df_city_final = city_report(df_city_sel, df_fact_sel)
        df_presc_final = top_5_Prescribers(df_fact_sel)
        # validate
        df_top10_rec(df_city_final, 'df_city_final')
        df_print_schema(df_city_final, 'df_city_final')
        df_top10_rec(df_presc_final, 'df_presc_final')
        df_print_schema(df_presc_final, 'df_presc_final')
        # setup logging mechanism configuration



        logging.info("run_presc_pipelie is Completed...")

    except Exception as exp:
        print(str(exp))
        logging.error("Error occurred in the main() method. Please check the stack "
                      "Trace to go to the respective module. " + str(exp), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    logging.info("run_presc_pipeline is started...")
    main()
