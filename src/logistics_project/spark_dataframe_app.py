from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("SimpleDataFrameApp") \
        .getOrCreate()

    # Define schema for the DataFrame
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer", StringType(), True),
        StructField("status", StringType(), True)
    ])

    # Create sample data
    data = [
        (1, "Alice", "processing"),
        (2, "Bob", "accepted"),
        (3, "Charlie", "in_transit"),
        (4, "David", "delivered")
    ]

    # Create DataFrame
    df = spark.createDataFrame(data, schema)

    # Show DataFrame
    df.show()

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
