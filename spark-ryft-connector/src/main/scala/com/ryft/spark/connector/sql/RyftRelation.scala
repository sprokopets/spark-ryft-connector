/*
 * ============= Ryft-Customized BSD License ============
 * Copyright (c) 2015, Ryft Systems, Inc.
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation and/or
 *   other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software must display the following acknowledgement:
 *   This product includes software developed by Ryft Systems, Inc.
 * 4. Neither the name of Ryft Systems, Inc. nor the names of its contributors may be used
 *   to endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY RYFT SYSTEMS, INC. ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL RYFT SYSTEMS, INC. BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * ============
 */

package com.ryft.spark.connector.sql

import com.ryft.spark.connector.domain.RyftQueryOptions
import com.ryft.spark.connector.partitioner.NoPartitioner
import com.ryft.spark.connector.rdd.{RDDQuery, RyftRDD}
import com.ryft.spark.connector.util.{RyftPartitioner, FilterConverter, TransformFunctions}
import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.joda.time.DateTime

import scala.annotation.tailrec

class RyftRelation(files: List[String],
    userProvidedSchema: StructType)(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan with Serializable with Logging {

  private var currentSchema = userProvidedSchema

  override def schema: StructType = currentSchema

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val columnsToSelect =
      if (requiredColumns.isEmpty) currentSchema.fieldNames
      else requiredColumns

    val ryftQuery = FilterConverter.filtersToRecordQuery(filters)
    val queryOptions = RyftQueryOptions(files, columnsToSelect.toList)

    val partitioner =
      if (sqlContext.getConf("spark.ryft.partitioner", "").nonEmpty) {
        sqlContext.getConf("spark.ryft.partitioner")
      } else if (sqlContext.sparkContext.getConf.get("spark.ryft.partitioner", "").nonEmpty) {
        sqlContext.sparkContext.getConf.get("spark.ryft.partitioner")
      } else {
        classOf[NoPartitioner].getCanonicalName
      }

    val sparkConf = sqlContext.sparkContext.getConf.set("spark.ryft.partitioner", partitioner)
    val ryftPartitions = RyftPartitioner.partitions(ryftQuery, sparkConf)

    val rddQuery = RDDQuery(ryftQuery, queryOptions, ryftPartitions)
    val ryftRDD = new RyftRDD(sqlContext.sparkContext, Seq(rddQuery), TransformFunctions.noTransform)

    //need fields for mapping in the same order as in schema
    val fieldsToMap = columnsToSelect.map(userProvidedSchema(_))
    currentSchema = StructType(fieldsToMap)

    if (ryftRDD.isEmpty()) sqlContext.sparkContext.emptyRDD[Row]
    else ryftRDD.map(dataMapToRow(fieldsToMap, currentSchema, _))
  }

  //Converts Map with data according to the provided schema into Row
  @tailrec
  private def dataMapToRow(fields: Array[StructField],
      currentSchema: StructType,
      data: Map[String,Any],
      acc: List[Any] = List.empty[Any]): Row = {
    if (fields.isEmpty) Row.fromSeq(acc.reverse)
    else {
      val field = fields.head
      field.dataType match {
        case st: StructType =>
          val nestedFields = st.fields
          val nestedMap = data(field.name).asInstanceOf[Map[String,Any]]
          val nestedValue = nestedMapToRow(st, data, field)
          dataMapToRow(nestedFields, st, nestedMap, nestedValue :: acc)
        case _ =>
          val value = readValue(currentSchema, field.name, data(field.name))
          dataMapToRow(fields.tail, currentSchema, data, value :: acc)
      }
    }
  }

  private def readValue(currentSchema: StructType, field: String, dataAny: Any) = {
    val data = dataAny.toString
    val elem = currentSchema(field)
    elem.dataType match {
      case StringType   => data
      case BooleanType  => data.toBoolean
      case IntegerType  => data.toInt
      case LongType     => data.toLong
      case ShortType    => data.toShort
      case DoubleType   => data.toDouble
      case FloatType    => data.toFloat
      case ByteType     => data.toByte
      case DateType     => DateTime.parse(data)
      case _ =>
        logWarning(s"Unable to recognize element type: ${elem.dataType}." +
          s"Will return as a string")
        data
    }
  }

  private def nestedMapToRow(st: StructType, data: Map[String,Any], field: StructField) = {
    val nestedFields = st.fields
    val nestedMap = data(field.name).asInstanceOf[Map[String,Any]]
    dataMapToRow(nestedFields, st, nestedMap, List.empty[Any])
  }

//  private def ryftRestUrls(sparkConf: SparkConf): Set[String] = {
//    val urlOption = sparkConf.getOption("spark.ryft.rest.url")
//    if (urlOption.nonEmpty) urlOption.get
//      .split(",")
//      .map(url => url.trim).toSet
//    else ConfigHolder.ryftRestUrl.toSet
//  }
}

