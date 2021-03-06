# ============= Ryft-Customized BSD License ============
# Copyright (c) 2015, Ryft Systems, Inc.
# All rights reserved.
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
#   this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation and/or
#   other materials provided with the distribution.
# 3. All advertising materials mentioning features or use of this software must display the following acknowledgement:
#   This product includes software developed by Ryft Systems, Inc.
# 4. Neither the name of Ryft Systems, Inc. nor the names of its contributors may be used
#   to endorse or promote products derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY RYFT SYSTEMS, INC. ''AS IS'' AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL RYFT SYSTEMS, INC. BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
# ============

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

df = sqlContext.read.format("com.ryft.spark.connector.sql").schema(schema)\
        .option("partitioner", "com.ryft.spark.connector.partitioner.NoPartitioner") \ # Optional setting to specify custom partitioning rules
        .option("files", "*.pcrime")\
        .load()
df.registerTempTable("temp_table")

df = sqlContext.sql("select Date, ID, Description, Arrest from temp_table\
       where Description LIKE '%VEHICLE%'\
          AND (Date LIKE '%04/15/2015%' OR Date LIKE '%04/14/2015%' OR Date LIKE '%04/13/2015%')\
          AND Arrest = true\
       ORDER BY Date").first()
df.show(truncate=False)