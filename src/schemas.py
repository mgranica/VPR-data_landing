import pyspark.sql.types as t
import pyspark.sql.functions as f

# Define the schema for the clients JSON structure
clients_schema = t.StructType([
    t.StructField("client_id", t.StringType(), False),
    t.StructField("first_name", t.StringType(), False),
    t.StructField("last_name", t.StringType(), False),
    t.StructField("email", t.StringType(), True),
    t.StructField("phone_number", t.StringType(), True),
    t.StructField("date_of_birth", t.DateType(), True),
    t.StructField("gender", t.StringType(), True),
    t.StructField("occupation", t.StringType(), True),
    t.StructField("created_at", t.DateType(), True),
    t.StructField("updated_at", t.DateType(), True),
    t.StructField("status", t.StringType(), True)
])

# Define the schema for the addresses JSON structure
addresses_schema = t.StructType([
    t.StructField("client_id", t.StringType(), False),
    t.StructField("address_id", t.StringType(), False),
    t.StructField("neighborhood", t.StringType(), True),
    t.StructField("coordinates", t.ArrayType(t.DoubleType()), True),
    t.StructField("road", t.StringType(), True),
    t.StructField("house_number", t.StringType(), True),
    t.StructField("suburb", t.StringType(), True),
    t.StructField("city_district", t.StringType(), True),
    t.StructField("state", t.StringType(), True),
    t.StructField("postcode", t.StringType(), True),
    t.StructField("country", t.StringType(), True),
    t.StructField("lat", t.StringType(), True),
    t.StructField("lon", t.StringType(), True)
])

# Define the schema for the products JSON structure
products_schema = t.StructType([
    t.StructField("price", t.FloatType(), True),
    t.StructField("currency", t.StringType(), True),
    t.StructField("measurement_ensembled_text", t.StringType(), True),
    t.StructField("url", t.StringType(), True),
    t.StructField("name", t.StringType(), True),
    t.StructField("category", t.StringType(), True),
    t.StructField("packages", t.ArrayType(t.StructType([
        t.StructField("name", t.StringType(), True),
        t.StructField("typeName", t.StringType(), True),
        t.StructField("itemNo", t.StringType(), True),
        t.StructField("articleNumber", t.StringType(), True),
        t.StructField("measurements", t.StructType([
            t.StructField("dimensions", t.StructType([
                t.StructField("width", t.IntegerType(), True),
                t.StructField("height", t.IntegerType(), True),
                t.StructField("length", t.IntegerType(), True),
                t.StructField("unit", t.StringType(), True)
            ]), True),
            t.StructField("weight", t.StructType([
                t.StructField("value", t.FloatType(), True),
                t.StructField("unit", t.StringType(), True)
            ]), True),
            t.StructField("volume", t.StructType([
                t.StructField("value", t.IntegerType(), True),
                t.StructField("unit", t.StringType(), True)
            ]), True)
        ]), True),
        t.StructField("quantity", t.IntegerType(), True),
    ])), True)
])