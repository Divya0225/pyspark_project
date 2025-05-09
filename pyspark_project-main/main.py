import json
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType

def load_config(file_path):
    """
    Load configuration file (JSON format).
    """
    with open(file_path, 'r') as f:
        return json.load(f)

def parse_schema(schema_str):
    """
    Convert a string-based schema into a PySpark StructType schema.
    """
    if not schema_str:
        return None
    
    fields = []
    for field in schema_str.split(","):
        field_name, field_type = field.strip().rsplit(maxsplit=1)
        if field_type.upper() == "INT":
            fields.append(StructField(field_name, IntegerType(), True))
        elif field_type.upper() == "LONG":
            fields.append(StructField(field_name, LongType(), True))    
        elif field_type.upper() == "STRING":
            fields.append(StructField(field_name, StringType(), True))
        # Add more type mappings as needed

    return StructType(fields)

def read_data(spark, config):
    """
    Read data from various file formats based on the config file.
    """
    source_path = config['source_path']
    source_type = config['source_type']
    schema_str = config.get('schema', None)
    header_str = config.get('header', None)
    
    # Parse schema if provided
    schema = parse_schema(schema_str) if schema_str else None
    header = header_str.split(",") if header_str else None

    # Read data based on the file type
    if source_type == 'csv':
        df = spark.read.csv(source_path, header=True, schema=schema, inferSchema=schema is None)
        if header:
            df = df.toDF(*[col.strip() for col in header])
    elif source_type == 'parquet':
        df = spark.read.schema(schema).parquet(source_path) if schema else spark.read.parquet(source_path)
    elif source_type == 'json':
        df = spark.read.schema(schema).json(source_path) if schema else spark.read.json(source_path)
    else:
        raise ValueError(f"Unsupported source type: {source_type}")
    
    return df


def read_data_to_mysql(spark, df, sql_con):
    mysql_url = "jdbc:mysql://{}:{}/{}".format(sql_con['host_name'],sql_con['port_name'],sql_con['db_name'])
    df.write \
        .format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", mysql_url) \
        .option("dbtable", sql_con['table_name']) \
        .option("user", sql_con['user']) \
        .option("password", sql_con['password']) \
        .mode("overwrite") \
        .save()


def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Generic PySpark Data Reader')
    parser.add_argument('config_file', help='Path to the configuration file')
    args = parser.parse_args()

    # Create Spark session
    spark = SparkSession.builder.appName("GenericReader") \
    .config("spark.jars", "/Users/bharath/Downloads/mysql-connector-j-9.1.0/mysql-connector-j-9.1.0.jar") \
    .getOrCreate()

    # Load configuration
    config = load_config(args.config_file)

    sql_con = {
        "host_name" : "localhost",
        "port_name" : "3306",
        "db_name" : "cheese_db",
        "table_name" : config['table_name'],
        "user" : "root",
        "password" : "Mudundibv@54"
    }

    # Read data using the configuration
    df = read_data(spark, config)

    read_data_to_mysql(spark, df, sql_con)

    # Show the data
    df.show()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()



