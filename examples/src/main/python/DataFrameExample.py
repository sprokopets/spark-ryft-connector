from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, BooleanType, DoubleType, ByteType

sparkConf = SparkConf().setAppName("DataFrameExample").setMaster("local[2]")

sc = SparkContext(conf=sparkConf)
sqlContext = SQLContext(sc)

schema = StructType([
    StructField("Arrest", BooleanType()),
    StructField("Beat", StringType()),
    StructField("Block", StringType()),
    StructField("CaseNumber", IntegerType()),
    StructField("CommunityArea", IntegerType()),
    StructField("Date", StringType()),
    StructField("Description", StringType()),
    StructField("District", IntegerType()),
    StructField("Domestic", BooleanType()),
    StructField("FBICode", IntegerType()),
    StructField("ID", StringType()),
    StructField("IUCR", IntegerType()),
    StructField("Latitude", DoubleType()),
    StructField("Location", StringType()),
    StructField("LocationDescription", StringType()),
    StructField("Longitude", DoubleType()),
    StructField("PrimaryType", StringType()),
    StructField("UpdatedOn", StringType()),
    StructField("Ward", IntegerType()),
    StructField("XCoordinate", IntegerType()),
    StructField("YCoordinate", IntegerType()),
    StructField("Year", IntegerType()),
    StructField("_index", StructType([
        StructField("file", StringType()),
        StructField("offset", StringType()),
        StructField("length", IntegerType()),
        StructField("fuzziness", ByteType())
    ]))
])

df = sqlContext.read.format("com.ryft.spark.connector.sql").schema(schema).option("files", "*.pcrime").load()
df.registerTempTable("temp_table")

df = sqlContext.sql("select Date, ID, Description, Arrest from temp_table\
       where Description LIKE '%VEHICLE%'\
          AND (Date LIKE '%04/15/2015%' OR Date LIKE '%04/14/2015%' OR Date LIKE '%04/13/2015%')\
          AND Arrest = true\
       ORDER BY Date").collect()
print df