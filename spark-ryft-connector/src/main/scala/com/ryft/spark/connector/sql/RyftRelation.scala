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
import com.ryft.spark.connector.rdd.RyftRDD
import com.ryft.spark.connector.util.{RyftUtil, FilterConverter, TransformFunctions}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, StructType}

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

    val ryftPartitions = RyftUtil.ryftRestUrls(sqlContext.sparkContext.getConf)
    val queryOptions = RyftQueryOptions(files, columnsToSelect.toList, ryftPartitions)

    val query = FilterConverter.filtersToRecordQuery(filters)
    val ryftRDD = new RyftRDD(sqlContext.sparkContext, query, queryOptions,
      TransformFunctions.noTransform)

    //need fields for mapping in the same order as in schema
    val fieldsToMap = columnsToSelect.map(userProvidedSchema(_))
    currentSchema = StructType(fieldsToMap)

    if (ryftRDD.isEmpty()) sqlContext.sparkContext.emptyRDD[Row]
    else ryftRDD.map(dataMapToRow(fieldsToMap, _))
  }

  //Converts Map with data according to the provided schema into Row
  @tailrec
  private def dataMapToRow(fields: Array[StructField],
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
          dataMapToRow(nestedFields, nestedMap, nestedValue :: acc)
        case _ =>
          val value = data.getOrElse(field.name, "")
          dataMapToRow(fields.tail, data, value :: acc)
      }
    }
  }

  private def nestedMapToRow(st: StructType, data: Map[String,Any], field: StructField) = {
    val nestedFields = st.fields
    val nestedMap = data(field.name).asInstanceOf[Map[String,Any]]
    dataMapToRow(nestedFields, nestedMap, List.empty[Any])
  }
}

