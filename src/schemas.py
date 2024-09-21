import pyspark.sql.types as t

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
    t.StructField("id", t.IntegerType(), True),
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
                t.StructField("width", t.FloatType(), True),
                t.StructField("height", t.FloatType(), True),
                t.StructField("length", t.FloatType(), True),
                t.StructField("unit", t.StringType(), True)
            ]), True),
            t.StructField("weight", t.StructType([
                t.StructField("value", t.FloatType(), True),
                t.StructField("unit", t.StringType(), True)
            ]), True),
            t.StructField("volume", t.StructType([
                t.StructField("value", t.FloatType(), True),
                t.StructField("unit", t.StringType(), True)
            ]), True)
        ]), True),
        t.StructField("quantity", t.IntegerType(), True),
        t.StructField("subitemNo", t.IntegerType(), True),
    ])), True)
])


silver_address_schema = t.StructType([
    t.StructField('client_id', t.StringType(), True), 
    t.StructField('address_id', t.StringType(), True), 
    t.StructField('neighborhood', t.StringType(), True), 
    t.StructField('coordinates', t.ArrayType(t.DoubleType(), True), True), 
    t.StructField('road', t.StringType(), True), 
    t.StructField('house_number', t.StringType(), True), 
    t.StructField('suburb', t.StringType(), True),
    t.StructField('city_district', t.StringType(), True), 
    t.StructField('state', t.StringType(), True), 
    t.StructField('postcode', t.StringType(), True), 
    t.StructField('country', t.StringType(), True), 
    t.StructField('lat', t.FloatType(), True), 
    t.StructField('lon', t.FloatType(), True)
])

silver_clients_schema = t.StructType([
    t.StructField('client_id', t.StringType(), True), 
    t.StructField('first_name', t.StringType(), True), 
    t.StructField('last_name', t.StringType(), True), 
    t.StructField('email', t.StringType(), True), 
    t.StructField('phone_number', t.StringType(), True), 
    t.StructField('date_of_birth', t.DateType(), True), 
    t.StructField('gender', t.StringType(), True), 
    t.StructField('occupation', t.StringType(), True), 
    t.StructField('created_at', t.DateType(), True), 
    t.StructField('updated_at', t.DateType(), True), 
    t.StructField('status', t.StringType(), True)
])

silver_products_schema = t.StructType([
    t.StructField('product_id', t.StringType(), False), 
    t.StructField('name', t.StringType(), True), 
    t.StructField('category', t.StringType(), True), 
    t.StructField('type_name', t.StringType(), True), 
    # t.StructField('item_number', t.StringType(), True), 
    # t.StructField('article_number', t.StringType(), True), 
    t.StructField('width', t.FloatType(), True), 
    t.StructField('height', t.FloatType(), True), 
    t.StructField('length', t.FloatType(), True), 
    t.StructField('weight', t.FloatType(), True), 
    t.StructField('volume', t.FloatType(), True), 
    t.StructField('package_quantity', t.IntegerType(), True), 
    t.StructField('price', t.FloatType(), True), 
    t.StructField('currency', t.StringType(), True), 
    t.StructField('url', t.StringType(), True),
    t.StructField(
        'product_components',t.StructType([
                t.StructField('package_id', t.StringType(), True), 
                t.StructField('subpackage_id', t.IntegerType(), True), 
                t.StructField('package_quantity', t.IntegerType(), True)
        ]), False)
])