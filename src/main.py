import sys
import os
import yaml
import logging
from dotenv import load_dotenv, find_dotenv

from spark_session import create_spark_session
from schemas import addresses_schema, products_schema, clients_schema
from functions import *


# Constants
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yml')
ENV_PATH = ".env"


def setup_logging():
    """
    Set up logging configuration.
    """
    logging.basicConfig(
        level=logging.INFO,  # Log level can be adjusted (DEBUG, INFO, WARNING, ERROR)
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),  # Log to console
            logging.FileHandler("app.log", mode='a')  # Optionally log to a file
        ]
    )
    

def load_config():
    """
    Load the YAML configuration file.
    :return: Dictionary containing configuration
    """
    try:
        with open(CONFIG_PATH, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except FileNotFoundError as e:
        logging.error(f"Configuration file not found: {e}")
        sys.exit(1)
    except yaml.YAMLError as e:
        logging.error(f"Error parsing configuration file: {e}")
        sys.exit(1)


def load_env_variables():
    """
    Load environment variables from .env file.
    :return: aws_access_key_id, aws_secret_access_key
    """
    try:
        env_path = find_dotenv(filename=ENV_PATH, raise_error_if_not_found=True)
        load_dotenv(dotenv_path=env_path)

        logging.info("Successfully loaded environment variables from .env file.")
    except Exception as e:
        logging.error(f"Error loading .env file: {e}")
        sys.exit(1)

    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not aws_access_key_id or not aws_secret_access_key:
        logging.error("AWS credentials not found in environment variables.")
        sys.exit(1)

    return aws_access_key_id, aws_secret_access_key


def process_bronze_layer(spark, config, bucket_name):
    """
    Read raw data from S3, transform to bronze, and write back to S3.
    :param spark: Spark session object
    :param config: Configuration dictionary
    :param bucket_name: S3 bucket name
    """
    try:
        raw_paths = {
            "address": os.path.join(bucket_name, config["paths"]["RAW"], config["raw_data"]["ADDRESS_DATA"]),
            "clients": os.path.join(bucket_name, config["paths"]["RAW"], config["raw_data"]["CLIENTS_DATA"]),
            "products": os.path.join(bucket_name, config["paths"]["RAW"], config["raw_data"]["PRODUCTS_DATA"]),
        }

        # Read JSON from S3 and transform to bronze
        df_address_raw = read_json_to_df(spark, raw_paths["address"], addresses_schema)
        df_clients_raw = read_json_to_df(spark, raw_paths["clients"], clients_schema)
        df_products_raw = read_json_to_df(spark, raw_paths["products"], products_schema)
        
        logging.info("Raw data successfully read from S3.")

        # Define bronze paths
        bronze_paths = {
            "address": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["BRONZE"], config["table_names"]["ADDRESS_TABLE"]),
            "clients": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["BRONZE"], config["table_names"]["CLIENTS_TABLE"]),
            "products": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["BRONZE"], config["table_names"]["PRODUCTS_TABLE"]),
        }

        # Write bronze data to S3
        write_df_to_metastore(df_address_raw, bronze_paths["address"], "bronze_address_table")
        write_df_to_metastore(df_clients_raw, bronze_paths["clients"], "bronze_clients_table")
        write_df_to_metastore(df_products_raw, bronze_paths["products"], "bronze_products_table")

        logging.info("Bronze layer successfully written to S3.")
    except Exception as e:
        logging.error(f"Error processing bronze layer: {e}")
        sys.exit(1)
        
        
def process_silver_layer(spark, config, bucket_name):
    """
    Read bronze data from S3, transform to silver, and write back to S3.
    :param spark: Spark session object
    :param config: Configuration dictionary
    :param bucket_name: S3 bucket name
    """
    try:
        # Read JSON from S3 and transform to bronze
        df_address_bronze = spark.table("bronze_address_table")
        df_clients_bronze = spark.table("bronze_clients_table")
        df_products_bronze = spark.table("bronze_products_table")
        logging.info("Bronze data successfully read from S3.")

        # Define silver paths
        silver_paths = {
            "address": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["SILVER"], config["table_names"]["ADDRESS_TABLE"]),
            "clients": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["SILVER"], config["table_names"]["CLIENTS_TABLE"]),
            "products": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["SILVER"], config["table_names"]["PRODUCTS_TABLE"]),
        }

        # Write bronze data to S3
        write_df_to_metastore(transform_addresses_bronze_to_silver(df_address_bronze), silver_paths["address"], "silver_address_table")
        write_df_to_metastore(transform_clients_bronze_to_silver(df_clients_bronze), silver_paths["clients"], "silver_clients_table")
        write_df_to_metastore(transform_products_bronze_to_silver(df_products_bronze), silver_paths["products"], "silver_products_table")

        logging.info("Silver layer successfully written to S3.")
    except Exception as e:
        logging.error(f"Error processing silver layer: {e}")
        sys.exit(1)
        

def process_gold_layer(spark, config, bucket_name):
    """
    Read silver data from S3, transform to gold, and write back to S3.
    :param spark: Spark session object
    :param config: Configuration dictionary
    :param bucket_name: S3 bucket name
    """
    try:
        # Read JSON from S3 and transform to bronze
        df_address_silver = spark.table("silver_address_table")
        df_clients_silver = spark.table("silver_clients_table")
        df_products_silver = spark.table("silver_products_table")
        logging.info("Silver data successfully read from S3.")

        # Define silver paths
        gold_paths = {
            "clients_address": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["GOLD"], config["table_names"]["CLIENTS_ADDRESS_TABLE"]),
            "products": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["GOLD"], config["table_names"]["PRODUCTS_TABLE"]),
            "packages": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["GOLD"], config["table_names"]["PACKAGE_TABLE"]),
        }

        # Write bronze data to S3
        write_df_to_metastore(transform_clients_addresses_silver_to_gold(df_clients_silver, df_address_silver), gold_paths["clients_address"], "gold_clients_address_table")
        write_df_to_metastore(transform_products_silver_to_gold(df_products_silver), gold_paths["products"], "gold_products_table", _format="delta")
        write_df_to_metastore(transform_packages_silver_to_gold(df_products_silver), gold_paths["packages"], "gold_packages_table", _format="delta")
        logging.info("Gold layer successfully written to S3.")
    except Exception as e:
        logging.error(f"Error processing Gold layer: {e}")
        sys.exit(1)



def main():
    # Setup logging
    logger = logging.getLogger(__name__)

    # Load environment variables and configuration
    aws_access_key_id, aws_secret_access_key = load_env_variables()
    config = load_config()
    
    bucket_name = config["paths"]["BUCKET_NAME"]
    
    # Create Spark session
    spark = create_spark_session(aws_access_key_id, aws_secret_access_key)

    # Process bronze layer
    process_bronze_layer(spark, config, bucket_name)
    
    # Process silver layer
    process_silver_layer(spark, config, bucket_name)
    
    # process gold layer
    process_gold_layer(spark, config, bucket_name)
        
        
if __name__ == "__main__":
    setup_logging()
    main()