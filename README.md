# Spark Ryft Connector

This library lets you expose Ryft as Spark RDDs

# Installation
```sh
sbt clean compile
```

# Build executable jar
```sh
sbt clean assembly
```
You can find jar:
../spark-ryft-connector/target/scala-2.10/

# Ryft Query mechanism

There are two main types of RyftQuery:
 1. SimpleQuery
 2. RecordQuery

SimpleQuery represents RAW_TEXT search for one or more search queries. For this type of query used only CONTAINS relational operator. For two or more search queries used OR logical operator.
For example:
we need to search for 3 SimpleQuery ‘query0’, ‘query1’, ‘query2’
```sh
SimpleQuery(List(“query0”,”query1”,”query2”))
```
It will be transformed to:

```sh
((RAW_TEXT CONTAINS “query0”)OR(RAW_TEXT CONTAINS ”query1”)OR(RAW_TEXT CONTAINS “query2"))
```
RecordQuery is a bit complex. It allows to use Ryft RECORD and RECORD.field queries. You can use method chaining mechanism to create nested queries.

For example:

- search for all records where field desc contains ‘VEHICLE’

```sh
RecordQuery(recordField("desc"), contains, "VEHICLE") -> (RECORD.desc CONTAINS “VEHICLE”) 
```
- search for all records which contains ‘VEHICLE’ in any field

```sh
RecordQuery(record, contains, "VEHICLE") -> (RECORD CONTAINS “VEHICLE”) 
```

RecordQuery allows you to combine two or more queries via method chaining:

```sh
RecordQuery(recordField("desc"), contains, "VEHICLE")
      .or(recordField("desc"), contains, "BIKE")  
     
    
((RECORD.desc CONTAINS “VEHICLE”)OR(RECORD.desc CONTAINS “BIKE”))
```

or even complex nested queries:

```sh
RecordQuery(
      RecordQuery(recordField("desc"), contains, "VEHICLE")
      .or(recordField("desc"), contains, "BIKE")
      .or(recordField("desc"), contains, "MOTO"))
    .and(RecordQuery(recordField("date"), contains, "04/15/2015"))  
     
    
(((RECORD.desc CONTAINS “VEHICLE”)OR(RECORD.desc CONTAINS “BIKE”)OR(RECORD.desc CONTAINS “MOTO”))AND
(RECORD.date CONTAINS “04/15/2015”))
```

#DataFrame support

#License
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
