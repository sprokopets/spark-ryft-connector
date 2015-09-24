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

package com.ryft.spark.connector.util

import com.ryft.spark.connector.RyftSparkException
import com.ryft.spark.connector.config.ConfigHolder
import com.ryft.spark.connector.query.{GenericQuery, SingleQuery, RecordQuery, SimpleQuery}
import org.apache.spark.SparkConf
import scala.annotation.tailrec
import scala.reflect.runtime.universe._
import scala.collection.JavaConversions._
import com.ryft.spark.connector.domain._

/**
 * Provides helper functions specific for Ryft
 */
private [connector] object RyftQueryHelper {

  def prepareQueries[A : TypeTag](queries: List[A],
                                  queryOptions: RyftQueryOptions,
                                  sparkConf: SparkConf,
                                  preferredLocations: Any => Set[String]):
  Iterable[(String, String, Set[String])] = {

    val urlOption = sparkConf.getOption("spark.ryft.rest.url")
    val ryftRestUrls =
      if (urlOption.nonEmpty) urlOption.get
        .split(",")
        .map(url => url.trim)
        .toList
      else ConfigHolder.ryftRestUrl.toList

    ryftRestUrls.flatMap(ryftRestUrl => {
      typeOf[A] match {
        case t if t =:= typeOf[SimpleQuery] =>
          queries.map(q => {
            val ryftQuery = queryToString(q.asInstanceOf[SimpleQuery])
            val simpleQuery = q.asInstanceOf[SimpleQuery]
            (simpleQuery.queries.mkString(","),
              ryftRestUrl + s"/search?query=($ryftQuery)"+ queryOptionsToString(queryOptions),
              preferredLocations(simpleQuery)) //FIXME: Nil now because partitioning not implemented yet
          })

        case t if t =:= typeOf[RecordQuery] =>
          queries.map(q => {
            val recordQuery = q.asInstanceOf[RecordQuery]
            val ryftQuery = queryToString(recordQuery)
            val files = new StringBuilder
            queryOptions.files.foreach(f => files.append(s"&files=$f"))
            s"$files"

            (ryftQuery,
              ryftRestUrl + s"/search?query=($ryftQuery)$files&format=xml",
              preferredLocations(recordQuery)) //FIXME: Nil now because partitioning not implemented yet
          })

        case _ =>
          throw new RyftSparkException("Unable to handle given type")
      }
    })
  }

  private def queryToString(query: SimpleQuery) = {
    val queries = query.queries
    val preparedQueries = new StringBuilder(s"(${rawText.value}%20${contains.value}%20%22${queries.head}%22)")
    if (queries.tail.nonEmpty) {
      queries.tail.foreach(q => preparedQueries.append(s"OR(${rawText.value}%20${contains.value}%20%22$q%22)"))
    }
  }

  private def queryToString(query: RecordQuery): String = {
    queryToString(query.queries, "")
  }

  private def queryToString(query: SingleQuery) = {
    s"(${query.is.value}%20${query.ro.value}%20%22${query.query}%22)"
  }

  @tailrec
  private def queryToString(queries: List[GenericQuery], acc: String): String = {
    if (queries.isEmpty) acc
    else {
      val head = queries.head
      head match {
        case sq: SingleQuery => queryToString(queries.tail,
          sq.lo.value + queryToString(sq) + acc)

        case rq: RecordQuery => queryToString(queries.tail,
          rq.lo.value + "(" + queryToString(rq) + ")" + acc)

        case _               => throw new RyftSparkException("Unexpected Ryft Query Type")
      }
    }
  }

  //TODO: need to think make it as lazy val, create once
  private def queryOptionsToString(queryOptions: RyftQueryOptions): String = {
    val files = new StringBuilder
    queryOptions.files.foreach(f => files.append(s"&files=$f"))
    s"$files" +
      s"&surrounding=${queryOptions.surrounding}" +
      s"&fuzziness=${queryOptions.fuzziness}"
  }
}