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

Sys.setenv(SPARK_HOME="") #specify SPARK_HOME here
# This line loads SparkR from the installed directory
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
library(SparkR)

sparkRyftConnectorJar <- "" #specify spark-ryft-connector jar here

# Initialize SparkContext and SQLContext
sc <- sparkR.init(master="local[2]", sparkJars = sparkRyftConnectorJar)
sqlContext <- sparkRSQL.init(sc)

# Create Schema
pcrimeSchema <- structType(
    structField("Arrest", "boolean"), structField("Beat", "integer"),
    structField("Block", "string"), structField("CaseNumber", "string"),
    structField("CommunityArea", "integer"), structField("Date", "string"),
    structField("Description", "string"), structField("District", "integer"),
    structField("Domestic", "boolean"), structField("FBICode", "integer"),
    structField("ID", "string"), structField("IUCR", "integer"),
    structField("Latitude", "double"), structField("Location", "string"),
    structField("LocationDescription", "string"), structField("Longitude", "double"),
    structField("PrimaryType", "string"), structField("UpdatedOn", "string"),
    structField("Ward", "integer"), structField("XCoordinate", "integer"),
    structField("YCoordinate", "integer"), structField("Year", "integer"))

# Create a DataFrame using Ryft
customDF <- read.df(sqlContext, source = "com.ryft.spark.connector.sql", 
                                schema = pcrimeSchema, 
                                partitioner = "com.ryft.spark.connector.partitioner.NoPartitioner" # Optional setting to specify custom partitioning rules
                                files = "*.pcrime")

# Register this DataFrame as a table.
registerTempTable(customDF, "pcrime")

# SQL statements can be run by using the sql methods provided by sqlContext
result <- sql(sqlContext, "select ID, Description, Arrest from pcrime where Description LIKE '%VEHICLE%' AND Arrest = true")

# Call take to get first N element of DF
resultArray <- take(result, 10)

print("First 10 Result:")
print(resultArray)

sparkR.stop()
