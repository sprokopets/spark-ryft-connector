[![Build Status](https://travis-ci.org/sprokopets/spark-ryft-connector.svg?style=flat-square)](https://travis-ci.org/sprokopets/spark-ryft-connector)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.ryft/spark-ryft-connector_2.10/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.ryft/spark-ryft-connector_2.10)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.ryft/spark-ryft-connector_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.ryft/spark-ryft-connector_2.11)
# Spark Ryft Connector

This library lets you expose Ryft as Spark RDDs or Data Frames by consuming ryft rest api.

## Installation
```sh
sbt clean compile
```

## Build executable jar
```sh
sbt clean assembly
```
You can find jar at:
`../spark-ryft-connector/target/scala-2.10/`

## Ryft Query mechanism

There are two main types of RyftQuery:
 1. SimpleQuery
 2. RecordQuery

`SimpleQuery` represents RAW_TEXT search for one or more search queries. For this type of query used only CONTAINS relational operator. For two or more search queries used OR logical operator.
For example:
we need to do free text search for 3 words ‘query0’, ‘query1’, ‘query2’
```scala
SimpleQuery(List(“query0”,”query1”,”query2”))
```
It will be transformed to:

```
((RAW_TEXT CONTAINS “query0”)OR(RAW_TEXT CONTAINS ”query1”)OR(RAW_TEXT CONTAINS “query2"))
```
`RecordQuery` is a bit complex. It allows to use Ryft RECORD and RECORD.field queries. You can use method chaining mechanism to create nested queries.

For example:

- search for all records where field desc contains `VEHICLE`
```scala
RecordQuery(recordField("desc"), contains, "VEHICLE") -> (RECORD.desc CONTAINS “VEHICLE”) 
```

- search for all records which contains ‘VEHICLE’ in any field
```sh
RecordQuery(record, contains, "VEHICLE") -> (RECORD CONTAINS “VEHICLE”) 
```

RecordQuery allows you to combine two or more queries via method chaining:
```scala
RecordQuery(recordField("desc"), contains, "VEHICLE")
      .or(recordField("desc"), contains, "BIKE")  
```
producing the following query:
```     
((RECORD.desc CONTAINS “VEHICLE”)OR(RECORD.desc CONTAINS “BIKE”))
```

or even complex nested queries:

```scala
RecordQuery(
      RecordQuery(recordField("desc"), contains, "VEHICLE")
      .or(recordField("desc"), contains, "BIKE")
      .or(recordField("desc"), contains, "MOTO"))
    .and(RecordQuery(recordField("date"), contains, "04/15/2015"))  
```
producing following query:     
```    
(((RECORD.desc CONTAINS “VEHICLE”)OR(RECORD.desc CONTAINS “BIKE”)OR(RECORD.desc CONTAINS “MOTO”))AND
(RECORD.date CONTAINS “04/15/2015”))
```

## DataFrame support
Current connector supports registering as data frames in spark sql context so that they can be queried using SparkSQL. All that's need is a data schema described and single line to register list of file to be searched by the query. Here's example in `scala`:
```scala
  val schema = StructType(Seq(
    StructField("Arrest", BooleanType), StructField("CaseNumber", StringType),
    StructField("Date", StringType), StructField("Description", StringType), 
    ....
  ))
  
  sqlContext.read.ryft(schema, "*.pcrime", "temp_table")

  val df = sqlContext.sql(
    """select Date, Description, Arrest from temp_table
       where Description LIKE '%VEHICLE%'
          AND (Date LIKE '%04/15/2015%' OR Date LIKE '%04/14/2015%')
       ORDER BY Date
    """)
```

Same code in `python`:
```python
schema = StructType([
    StructField("Arrest", BooleanType()), StructField("CaseNumber", IntegerType()),
    StructField("Date", StringType()), StructField("Description", StringType()),
    ....
)
])

df = sqlContext.read.format("com.ryft.spark.connector.sql").schema(schema).option("files", "*.pcrime").load()
df.registerTempTable("temp_table")

df = sqlContext.sql("select Date, ID, Description, Arrest from temp_table\
       where Description LIKE '%VEHICLE%'\
          AND (Date LIKE '%04/15/2015%' OR Date LIKE '%04/14/2015%')\
          AND Arrest = true\
       ORDER BY Date")
```

For more examples in Scala, Java, PySpark an SparkR please look in [examples](examples/src/main) directory.

## Spark Config Options
The following options can be set via SparkConf, command line options or zeppelin spark interpreter settings:
- `spark.ryft.rest.url` - semicolon separated list of ryft rest endpoints for search. For example: http://ryftone-1:8765;http://ryftone-2:8765
- `spark.ryft.nodes` - number of ryft hardware nodes to be used by queries. corresponds to `nodes`  rest api query parameter.
- `spark.ryft.partitioner` - canonical class name that implements partitioning logic for data partitioning and collocation. See examples below.

## Data Partitioning and locality support

If setting `spark.ryft.rest.url` set to multiple endoiints, by default search request will be done to each of the servers and results combined. If one need to override this logic to either do round-robin requests or do requests depending on query value then partitioning function or class can be specified. 

The Partitioning class can be used with both RDD and DataFrame examples regarding if programming language. It is applied by loading jar file in spark context with the class that extends `com.ryft.spark.connector.partitioner.RyftPartitioner` and specifing its cannonical name via `spark.ryft.partitioner` configuration value. It can be done globally or on per query level. See [full example](examples/src/main/scala/com/ryft/spark/connector/examples/DataFrameExample.scala) or this code snippet:

```scala

  sqlContext.read.ryft(schema, "*.pcrime", "temp_table", classOf[ArrestPartitioner].getCanonicalName)
  ...
  
  
  class ArrestPartitioner extends RyftPartitioner {
  override def partitions(query: RyftQuery): Set[URL] = query match {
    case rq: RecordQuery =>
      partitionsAcc(rq.entries, List.empty[String]).filter(_.nonEmpty).map(new URL(_))
    case _ =>
      throw RyftSparkException(s"Unable to find partitions for such type of query: ${query.getClass}")
  }

  @tailrec
  private def partitionsAcc(entries: Set[(String,String)], acc: List[String]): Set[String] = {
    if(entries.isEmpty) acc.toSet
    else partitionsAcc(entries.tail, byFirsLetter(entries.head) :: acc)
  }

  private def byFirsLetter(entry: (String,String)) = {
    if (entry._1.equalsIgnoreCase("arrest")) {
      if (entry._2.equalsIgnoreCase("true")) "http://ryftone-1:8765"
      else "http://ryftone-2:8765"  
    } else ""
  }
}

```

The partitioning function works for RDD requests in java and scala and can be specified as a parameter to the RDD methods. For example:


```scala
// If country name starts with [a-n] go to ryftone-2:8765
// Otherwise go to ryftone-2:8765
def byCountry(query: RyftQuery): List[String] = {
    def byFirstLetter(country: String): List[String] = {
        if (('a' to 'n').contains(country.head.toLower)) 
            List("http://ryftone-1:8765")
        else 
            List("http://ryftone-2:8765")   
    }
    
    RyftPartitioner.byField(query, byFirstLetter, Seq("country")).toList
}
  
sc.ryftRDD(List(query), qoptions, byCountry)
    .asInstanceOf[RyftRDD[Map[String, String]]].map(r=> new Record(r))
    .toDF().cache().registerTempTable("query2")
    
println("query2: " + sqlc.table("query2").count)
```

In case of spark node running on ryftone box, one can use partitioning to implement collocation strategy. For example:

```scala
import java.net.URL

// This query uses preferred node selection
// All requests to ryft boxes will request execution on the corresponding node (with same ip)
// If no nodes availble execution will fallback to other free nodes

def sameNodeSelection(url: String) = {
    val preferredNodes = Set(new URL(url).getHost)
    println("preferred nodes: " + url + "->" + preferredNodes)
    preferredNodes
  }
  
sc.ryftRDD(List(query), qoptions, nopart, sameNodeSelection)
    .asInstanceOf[RyftRDD[Map[String, String]]].map(r=> new Record(r))
    .toDF().cache().registerTempTable("query3")
    
println("query3: " + sqlc.table("query3").count)
```

When multiple partitioning rules applied the one with most priority is used from low to high: `partition class in sparkConfig`, `partition class in query`, `partition function`.


##License
Ryft-Customized BSD License
Copyright (c) 2015, Ryft Systems, Inc.
All rights reserved.
Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation and/or
  other materials provided with the distribution.
3. All advertising materials mentioning features or use of this software must display the following acknowledgement:
  This product includes software developed by Ryft Systems, Inc.
4. Neither the name of Ryft Systems, Inc. nor the names of its contributors may be used
  to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY RYFT SYSTEMS, INC. ''AS IS'' AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL RYFT SYSTEMS, INC. BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
