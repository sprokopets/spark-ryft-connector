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

import com.ryft.spark.connector.config.ConfigHolder
import com.ryft.spark.connector.query.{RyftRecordQuery, SimpleRyftQuery}
import org.apache.spark.SparkConf
import scala.reflect.runtime.universe._
import scala.collection.JavaConversions._
import com.ryft.spark.connector.domain.{contains, rawText, RyftQueryOptions, RyftData}

/**
 * Provides helper functions specific for Ryft
 */
private [connector] object RyftHelper {

  def prepareQueries[A : TypeTag](queries: List[A],
                           queryOptions: RyftQueryOptions,
                           sparkConf: SparkConf): Iterable[(String, String, List[String])] = {
    val urlOption = sparkConf.getOption("ryft.rest.url")
    val ryftRestUrls =
      if (urlOption.nonEmpty) urlOption.get
        .split(",")
        .map(url => url.trim)
        .toList
      else ConfigHolder.ryftRestUrl.toList


    ryftRestUrls.flatMap(ryftRestUrl => {
      typeOf[A] match {
        case t if t =:= typeOf[SimpleRyftQuery] =>
          queries.map(q => {
            (q.asInstanceOf[SimpleRyftQuery].queries.mkString(","),
              ryftRestUrl + "/search" + queryToString(q.asInstanceOf[SimpleRyftQuery], queryOptions),
              Nil)
          })

        case t if t =:= typeOf[RyftRecordQuery] =>
          queries.map(q => {
            val query = queryToString(q.asInstanceOf[RyftRecordQuery], queryOptions)
            (query,
              ryftRestUrl + "/search" + query + "&format=xml",
              Nil)
          })

        case _                                  =>
          throw new RuntimeException("Unexpected type of queries")
      }
    })
  }

  def queryToString(query: SimpleRyftQuery, metaInfo: RyftQueryOptions) = {
    //prepare Ryft specific queries
    val queries = query.queries
    val preparedQueries = new StringBuilder(s"(${rawText.value}%20${contains.value}%20%22${queries.head}%22)")
    if (queries.tail.nonEmpty) {
      queries.tail.foreach(q => preparedQueries.append(s"OR(${rawText.value}%20${contains.value}%20%22$q%22)"))
    }

    val files = new StringBuilder
    metaInfo.files.foreach(f => files.append(s"&files=$f"))

    s"?query=($preparedQueries)$files&surrounding=${metaInfo.surrounding}&fuzziness=${metaInfo.fuzziness}"
  }

  def queryToString(query: RyftRecordQuery, metaInfo: RyftQueryOptions) = {
    val queries = query.queries
    val h = queries.head
    val sb = new StringBuilder(s"${h.logicalOperator.value}" +
      s"(${h.inputSpecifier.value}%20${h.relationalOperator.value}%20%22${h.query}%22)")

    queries.tail.foreach(h => sb.append(s"${h.logicalOperator.value}" +
      s"(${h.inputSpecifier.value}%20${h.relationalOperator.value}%20%22${h.query}%22)"))

    val files = new StringBuilder
    metaInfo.files.foreach(f => files.append(s"&files=$f"))

    s"?query=($sb)$files&surrounding=${metaInfo.surrounding}&fuzziness=${metaInfo.fuzziness}"
  }
}