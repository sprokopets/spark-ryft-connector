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

package com.ryft.spark.connector

import com.ryft.spark.connector.config.ConfigHolder
import com.ryft.spark.connector.query.{GenericQuery, RyftQuery, RecordQuery, SimpleQuery}
import com.ryft.spark.connector.domain.RyftQueryOptions
import com.ryft.spark.connector.rdd.{RyftRDDSimple, RyftPairRDD}
import com.ryft.spark.connector.util.{TransformFunctions, RyftQueryHelper}
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * Provides Ryft-specific methods on [[org.apache.spark.SparkContext SparkContext]]
 *
 */
class SparkContextFunctions(@transient val sc: SparkContext) extends Logging {

  def ryftRDD(queries: List[RyftQuery],
              queryOptions: RyftQueryOptions,
              choosePartitions: RyftQuery => Set[String] = _ => Set.empty[String],
              preferredLocations: RyftQuery => Set[String] = _ => Set.empty[String]) = {
    val preparedQueries = prepareQueries(queries, queryOptions, choosePartitions, preferredLocations)
    queries match {
      case (sqList: SimpleQuery) :: tail =>
        new RyftRDDSimple(sc, preparedQueries, TransformFunctions.toRyftData)
      case (rqList: RecordQuery) :: tail =>
        new RyftRDDSimple(sc, preparedQueries, TransformFunctions.noTransform)
      case _                             =>
        val msg = "Unable to create RyftRDDSimple. Unrecognized type of Ryft queries"
        logWarning(msg)
        throw new RyftSparkException(msg)
    }
  }

  def ryftPairRDD(queries: List[RyftQuery],
                  queryOptions: RyftQueryOptions,
                  choosePartitions: RyftQuery => Set[String] = _ => Set.empty[String],
                  preferredLocations: RyftQuery => Set[String] = _ => Set.empty[String]) = {
    val preparedQueries = prepareQueries(queries, queryOptions, choosePartitions, preferredLocations)
    queries match {
      case (sqList: SimpleQuery) :: tail =>
        new RyftPairRDD(sc, preparedQueries, TransformFunctions.toRyftData)
      case (rqList: RecordQuery) :: tail =>
        new RyftPairRDD(sc, preparedQueries, TransformFunctions.noTransform)
      case _                             =>
        val msg = "Unable to create RyftRDDSimple. Unrecognized type of Ryft queries"
        logWarning(msg)
        throw new RyftSparkException(msg)
    }
  }

  private def prepareQueries(queries: List[RyftQuery],
                             queryOptions: RyftQueryOptions,
                             choosePartitions: RyftQuery => Set[String],
                             preferredLocations: RyftQuery => Set[String]) = {
    val restUrls = ryftRestUrls(sc.getConf)
    queries.flatMap(q => {
      val ryftQueryS = RyftQueryHelper.queryAsString(q, queryOptions)
      val pls = preferredLocations(q)
      val partitions = choosePartitions(q)

      val urls =
        if (partitions.nonEmpty) partitions
        else restUrls

      urls.map(url => (ryftQueryS._1, url + ryftQueryS._2, pls))
    })
  }

  private def ryftRestUrls(sparkConf: SparkConf): List[String] = {
    val urlOption = sparkConf.getOption("spark.ryft.rest.url")
    if (urlOption.nonEmpty) urlOption.get
      .split(",")
      .map(url => url.trim)
      .toList
    else ConfigHolder.ryftRestUrl.toList
  }
}