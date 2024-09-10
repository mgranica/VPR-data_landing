from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.types as t
import pyspark.sql.functions as f
import logging



def read_csv_to_df(spark, file_path, schema=None):
    """
    Read CSV file into DataFrame.
    
    :param spark: SparkSession instance.
    :param file_path: Path to the JSON file.
    :param schema: Optional schema to enforce while reading.
    :return: DataFrame
    """
    # Validate file_path
    if not isinstance(file_path, str) or not file_path:
        raise ValueError("Invalid file path provided.")

    # Read DataFrame from CSV
    reader = (
        spark
        .read
        .format("csv")
        .option("header", "true")
        .option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .option("delimiter", ",")
        .option("escape", '"')
    )
        
    if schema:
        reader = reader.schema(schema)
    else:
        reader = reader.option("inferSchema", "true")
    
    try:
        df = reader.load(file_path)
        return df
    except Exception as e:
        raise IOError(f"Error reading CSV file: {str(e)}")

def read_json_to_df(spark, file_path, schema=None):
    """
    Read JSON file into DataFrame.
    
    :param spark: SparkSession instance.
    :param file_path: Path to the JSON file.
    :param schema: Optional schema to enforce while reading.
    :return: DataFrame
    """
    # Validate file_path
    if not isinstance(file_path, str) or not file_path:
        raise ValueError("Invalid file path provided.")

    # Read DataFrame from JSON
    reader = spark.read.format("json").option("multiLine", "true").option("mode", "PERMISSIVE")
    
    if schema:
        reader = reader.schema(schema)
    else:
        reader = reader.option("inferSchema", "true")
    
    try:
        df = reader.load(file_path)
        return df
    except Exception as e:
        raise IOError(f"Error reading JSON file: {str(e)}")

def write_df_to_metastore(df, file_path, table_name, partition_by=None, _format="parquet", mode="overwrite"):
    """
    Write DataFrame to Parquet file and save it as a table in the metastore.
    
    :param df: DataFrame to be written.
    :param file_path: Path where the Parquet file will be saved.
    :param table_name: Name of the table to save.
    :param partition_by: Column(s) to partition by.
    :param mode: Write mode, default is 'overwrite'. Other options are 'append', 'ignore', 'error'.
    """
    # Validate parameters
    if not file_path or not isinstance(file_path, str):
        raise ValueError("Invalid file path provided.")
    if not table_name or not isinstance(table_name, str):
        raise ValueError("Invalid table name provided.")
    if partition_by and not isinstance(partition_by, (str, list)):
        raise ValueError("Partition by should be a string or a list of strings.")

    writer = df.write.format(_format).mode(mode).option("path", file_path)
    
    if partition_by:
        writer = writer.partitionBy(partition_by)
    
    writer.saveAsTable(table_name)

def read_file(spark: SparkSession, file_path: str, file_type: str, schema: t.StructType, options: dict = None):
    """
    Reads a file into a PySpark DataFrame using a specified schema.

    :param spark: SparkSession object
    :param file_path: Path to the file
    :param file_type: Type of the file (csv, json, parquet, orc)
    :param schema: Schema to enforce
    :param options: Optional dictionary of read options (default is None)
    :return: DataFrame containing the file data
    """
    
    # Validate file_path
    try:
        if not isinstance(file_path, str) or not file_path:
            raise ValueError("Invalid file path provided.")

        if options is None:
            options = {"multiLine": "true", "mode": "PERMISSIVE"}
        df = (
            spark
            .read
            .format(file_type.lower())
            .options(**options)
            .schema(schema)
            .load(file_path)
        )
        return df
    except Exception as e:
        raise IOError(f"Error reading {file_path}: {str(e)}") 

def write_df(df: DataFrame, file_path: str, file_type: str="parquet", mode: str = "overwrite", options: dict = None):
    """
    Writes a DataFrame to a specified file format with error handling.

    :param df: The DataFrame to write
    :param file_path: Path to write the file
    :param file_type: Type of the file (csv, json, parquet, orc)
    :param mode: Save mode (default is 'overwrite', other options: 'append', 'ignore', 'error')
    :param options: Optional dictionary of write options (default is None)
    """
    
    if options is None:
        options = {}
    
    try:
        # Initialize the writer
        writer = df.write.format(file_type.lower()).mode(mode).options(**options)
        
        # Attempt to save the DataFrame
        writer.save(file_path)
        logging.info(f"DataFrame successfully written to {file_path} as {file_type.upper()}")
    
    except Exception as e:
        logging.error(f"Error writing DataFrame to {file_path} as {file_type.upper()}: {str(e)}")
        raise  # Re-raise the exception after logging it

def transform_clients_bronze_to_silver(clients_df):
    """
    Transform Bronze (raw) Clients DataFrame to Silver (cleaned) DataFrame.
    
    :param clients_df: Clients DataFrame.
    :return: Cleaned Clients DataFrame.
    """
    # Example transformations: Filtering active clients, renaming columns, etc.
    clients_silver_df = (
        clients_df
        .filter(f.col("status") == "active")
    )
    return clients_silver_df

def transform_addresses_bronze_to_silver(addresses_df):
    """
    Transform Bronze (raw) Addresses DataFrame to Silver (cleaned) DataFrame.
    
    :param addresses_df: Addresses DataFrame.
    :return: Cleaned Addresses DataFrame.
    """
    # Cast coordenates format to float
    addresses_silver_df = (
        addresses_df
        #.filter(col("house_number") != "")
        .withColumn("lat", f.col("lat").cast("float"))
        .withColumn("lon", f.col("lon").cast("float"))
    )
    return addresses_silver_df

def transform_clients_addresses_silver_to_gold(clients_silver_df, addresses_silver_df):
    """
    Transform Silver (cleaned) Clients and Addresses DataFrames to Gold (aggregated/enriched) DataFrame.
    
    :param clients_silver_df: Cleaned Clients DataFrame.
    :param addresses_silver_df: Cleaned Addresses DataFrame.
    :return: Enriched DataFrame combining both clients and addresses.
    """
    # Example aggregation: Joining clients with their addresses
    gold_df = (
        clients_silver_df
        .join(addresses_silver_df, on="client_id", how="right")
        .dropna(subset=['client_id'])
    )
    return gold_df

def transform_products_bronze_to_silver(products_bronze_df):
    """
    Transform Bronze (raw) Products DataFrame to Silver (cleaned) DataFrame.

    :param products_bronze_df: Products DataFrame.
    :return: Cleaned Products DataFrame.
    """
    products_silver_df = (
        products_bronze_df
        # Set product ID column 
        .withColumn("product_id", f.concat(f.lit("prod-"), f.expr("uuid()")))
        # Explode package annidations
        .withColumn("package_explode", f.explode(f.col("packages")))
        # Set prod components array
        .withColumn("product_components", f.array(
                f.struct(
                    f.col("package_explode.itemNo").alias("package_id"),
                    f.col("package_explode.quantity").alias("package_quantity")
                )
            )
        )
        # Select active columns
        .select(
            f.col("product_id"),
            f.col("package_explode.name").alias("name"),
            f.col("category"),
            f.col("package_explode.typeName").alias("type_name"),
            f.col("package_explode.itemNo").alias("item_number"),
            f.col("package_explode.articleNumber").alias("article_number"),
            f.col("package_explode.measurements.dimensions.width").alias("width"),
            f.col("package_explode.measurements.dimensions.height").alias("height"),
            f.col("package_explode.measurements.dimensions.length").alias("length"),
            f.col("package_explode.measurements.weight.value").alias("weight"),
            f.col("package_explode.measurements.volume.value").alias("volume"),       
            f.col("package_explode.quantity").alias("package_quantity"),
            f.col("price"),
            f.col("currency"),
            f.col("url"),
            f.col("product_components")
        )
    )

    return products_silver_df

def transform_products_silver_to_gold(products_silver_df):
    """
    Transform Silver (cleaned) Products DataFrames to Gold (aggregated/enriched) DataFrame.
    
    :param products_silver_df: Cleaned Productss DataFrame.
    :return: Enriched Products DataFrame.
    """
    products_gold_df = (
        products_silver_df
        .groupBy("product_id")
        .agg(
            f.first("name").alias("name"),
            f.first("category").alias("category"),
            #first("measurement_ensembled_text").alias("measurement_ensembled_text"),
            f.first("url").alias("url"),
            f.first("price").alias("price"),
            f.first("currency").alias("currency"),
            f.collect_list("product_components").alias("product_components"),
        )
    )

    return products_gold_df

def transform_packages_silver_to_gold(products_silver_df, min_stock=100):
    """
    Transform Silver (cleaned) Products DataFrames to Gold (aggregated/enriched) Packages DataFrame.
    
    :param products_silver_df: Cleaned Products DataFrame.
    :param min_stock: Available stock - ToDo: Randomize values
    :return: Enriched Packages DataFrame.
    """
    packages_gold_df = (
        products_silver_df
        .withColumn("stock_quantity", f.lit(min_stock))
        .dropDuplicates(["item_number"])
        .select(
            f.col("item_number").alias("package_id"),
            f.col("name"),
            f.col("width"),
            f.col("height"),
            f.col("length"),
            f.col("volume"),
            f.col("stock_quantity")
        )
    )

    return packages_gold_df
                    
               
