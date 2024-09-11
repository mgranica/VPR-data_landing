import sys
import configparser
import os
import yaml
import logging
# from dotenv import load_dotenv, find_dotenv

from spark_session import create_spark_session
from schemas import *
from functions import *


# Constants
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yml')
AWS_CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), '..', '.aws', 'credentials')

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
        

def load_aws_credentials(profile_name="default"):
    try:
        credentials = configparser.ConfigParser()
        credentials.read(AWS_CREDENTIALS_PATH)
        
        logging.info("Successfully loaded credentials variables from .aws file.")
    except Exception as e:
        logging.error(f"Error loading .aws file: {e}")
        sys.exit(1)

    aws_access_key_id = credentials[profile_name]["aws_access_key_id"]
    aws_secret_access_key = credentials[profile_name]["aws_secret_access_key"]

    if not aws_access_key_id or not aws_secret_access_key:
        logging.error("AWS credentials not found.")
        sys.exit(1)

    return aws_access_key_id, aws_secret_access_key


def extract_filepath(config, bucket_name, layer):
    
    paths = {
        # Define raw paths
        "raw_paths": {
            "address": os.path.join(bucket_name, config["paths"]["RAW"], config["raw_data"]["ADDRESS_DATA"]),
            "clients": os.path.join(bucket_name, config["paths"]["RAW"], config["raw_data"]["CLIENTS_DATA"]),
            "products": os.path.join(bucket_name, config["paths"]["RAW"], config["raw_data"]["PRODUCTS_DATA"]),
        },
        # Define bronze paths
        "bronze_paths": {
            "address": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["BRONZE"], config["table_names"]["ADDRESS_TABLE"]),
            "clients": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["BRONZE"], config["table_names"]["CLIENTS_TABLE"]),
            "products": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["BRONZE"], config["table_names"]["PRODUCTS_TABLE"]),
        },
        # Define silver paths
        "silver_paths": {
            "address": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["SILVER"], config["table_names"]["ADDRESS_TABLE"]),
            "clients": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["SILVER"], config["table_names"]["CLIENTS_TABLE"]),
            "products": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["SILVER"], config["table_names"]["PRODUCTS_TABLE"]),
        },
        # Define gold paths
        "gold_paths": {
            "clients_address": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["GOLD"], config["table_names"]["CLIENTS_ADDRESS_TABLE"]),
            "products": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["GOLD"], config["table_names"]["PRODUCTS_TABLE"]),
            "packages": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["GOLD"], config["table_names"]["PACKAGE_TABLE"]),
        }
    }
    
    return paths[layer]


def process_bronze_layer(spark, config, bucket_name):
    """
    Read raw data from S3, transform to bronze, and write back to S3.
    :param spark: Spark session object
    :param config: Configuration dictionary
    :param bucket_name: S3 bucket name
    """
    try:
        # Extract paths
        raw_paths = extract_filepath(config, bucket_name, "raw_paths")
        bronze_paths = extract_filepath(config, bucket_name, "bronze_paths")

        # Read JSON from S3 and transform to bronze
        df_address_raw = read_file(spark, raw_paths["address"], config["format"]["json"], addresses_schema)
        df_clients_raw = read_file(spark, raw_paths["clients"], config["format"]["json"], clients_schema)
        df_products_raw = read_file(spark, raw_paths["products"], config["format"]["json"], products_schema)
        logging.info("Raw data successfully read from S3.")

        # Write bronze data to S3
        write_df(df_address_raw, bronze_paths["address"])
        write_df(df_clients_raw, bronze_paths["clients"])
        write_df(df_products_raw, bronze_paths["products"])
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
        # Extract paths
        bronze_paths = extract_filepath(config, bucket_name, "bronze_paths")
        silver_paths = extract_filepath(config, bucket_name, "silver_paths")
        
        # Read JSON from S3 and transform to bronze
        df_address_bronze = read_file(spark, bronze_paths["address"], config["format"]["parquet"], addresses_schema)
        df_clients_bronze = read_file(spark, bronze_paths["clients"], config["format"]["parquet"], clients_schema)
        df_products_bronze = read_file(spark, bronze_paths["products"], config["format"]["parquet"], products_schema)
        logging.info("Bronze data successfully read from S3.")

        # Write silver data to S3
        write_df(transform_addresses_bronze_to_silver(df_address_bronze), silver_paths["address"])
        write_df(transform_clients_bronze_to_silver(df_clients_bronze), silver_paths["clients"])
        write_df(transform_products_bronze_to_silver(df_products_bronze), silver_paths["products"])
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
        # Extract paths
        silver_paths = extract_filepath(config, bucket_name, "silver_paths")
        gold_paths = extract_filepath(config, bucket_name, "gold_paths") 
        
        # Read Parquet from S3 and transform to gold
        df_address_silver = read_file(spark, silver_paths["address"], config["format"]["parquet"], silver_address_schema)
        df_clients_silver = read_file(spark, silver_paths["clients"], config["format"]["parquet"], silver_clients_schema)
        df_products_silver = read_file(spark, silver_paths["products"], config["format"]["parquet"], silver_products_schema)
        logging.info("Silver data successfully read from S3.")

        # Write bronze data to S3
        write_df(transform_clients_addresses_silver_to_gold(df_clients_silver, df_address_silver), gold_paths["clients_address"])
        write_df(transform_products_silver_to_gold(df_products_silver), gold_paths["products"], file_type="delta")
        write_df(transform_packages_silver_to_gold(df_products_silver), gold_paths["packages"], file_type="delta")
        logging.info("Gold layer successfully written to S3.")
    
    except Exception as e:
        logging.error(f"Error processing Gold layer: {e}")
        sys.exit(1)



def main():
    # Setup logging
    logger = logging.getLogger(__name__)

    # Load credentials and configuration
    aws_access_key_id, aws_secret_access_key = load_aws_credentials()
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