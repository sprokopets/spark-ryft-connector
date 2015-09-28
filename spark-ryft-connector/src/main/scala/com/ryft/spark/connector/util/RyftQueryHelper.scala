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
import com.ryft.spark.connector.query._
import org.apache.spark.{Logging, SparkConf}
import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.collection.JavaConversions._
import com.ryft.spark.connector.domain._

/**
 * Provides helper functions specific for Ryft
 */
private [connector] object RyftQueryHelper extends Logging{
  def queryAsString[RyftQuery: TypeTag](ryftQuery: RyftQuery, queryOptions: RyftQueryOptions) = {
    val ryftQueryS =
      ryftQuery match {
        case sq: SimpleQuery =>
          val queryString = queryToString(sq)
          (queryString, queryToString(sq) + queryOptionsToString(queryOptions))

        case rq: RecordQuery =>
          val queryString = queryToString(rq)
          val files = new StringBuilder
          queryOptions.files.foreach(f => files.append(s"&files=$f"))
          (queryString, queryToString(rq) + files + "&format=xml")

        case _ =>
          val msg = "Unable to convert RyftQuery to string. " +
            "Unrecognized RyftQuery subtype: " + typeOf[RyftQuery]
          logWarning(msg)
          throw new RyftSparkException(msg)
      }

    (ryftQueryS._1, s"/search?query=${ryftQueryS._2}")
  }

  private def queryToString(query: SimpleQuery): String = {
    val queries = query.queries
    val preparedQueries = new StringBuilder(s"(${rawText.value}%20${contains.value}%20%22${queries.head}%22)")
    if (queries.tail.nonEmpty) {
      queries.tail.foreach(q => preparedQueries.append(s"OR(${rawText.value}%20${contains.value}%20%22$q%22)"))
    }
    preparedQueries.toString()
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

  private def queryOptionsToString(queryOptions: RyftQueryOptions): String = {
    val files = new StringBuilder
    queryOptions.files.foreach(f => files.append(s"&files=$f"))
    s"$files" +
      s"&surrounding=${queryOptions.surrounding}" +
      s"&fuzziness=${queryOptions.fuzziness}"
  }
}