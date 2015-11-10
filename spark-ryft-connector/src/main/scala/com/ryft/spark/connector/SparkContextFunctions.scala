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
import com.ryft.spark.connector.domain.RyftQueryOptions
import com.ryft.spark.connector.exception.RyftSparkException
import com.ryft.spark.connector.query.{RecordQuery, RyftQuery, SimpleQuery}
import com.ryft.spark.connector.rdd.{RDDQuery, RyftPairRDD, RyftRDD}
import com.ryft.spark.connector.util.{RyftPartitioner, RyftQueryHelper, TransformFunctions}
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * Provides Ryft-specific methods on [[org.apache.spark.SparkContext SparkContext]]
 */
class SparkContextFunctions(@transient val sc: SparkContext) extends Logging {

  /**
   * Creates a view of search queries to Ryft as `RyftRDDSimple`
   *
   * This method is made available on [[org.apache.spark.SparkContext SparkContext]] by importing
   * `com.ryft.spark.connector._`
   *
   * @param queries Search query
   * @param queryOptions Query specific options
   * @param choosePartitions Function provides partitions for `RyftQuery`
   * @param preferredLocations Function provided spark preferred nodes for `RyftQuery`
   * @return
   */
  def ryftRDD(queries: Seq[RyftQuery],
              queryOptions: RyftQueryOptions,
              choosePartitions: RyftQuery => List[String] = _ => List.empty[String],
              preferredLocations: RyftQuery => Set[String] = _ => Set.empty[String]) = {
    val rddQueries = createRDDQueries(queries, queryOptions, choosePartitions, preferredLocations)
    queries match {
      case (sq: SimpleQuery) :: tail =>
        new RyftRDD(sc, rddQueries, queryOptions, TransformFunctions.toRyftData)
      case (rq: RecordQuery) :: tail =>
        new RyftRDD(sc, rddQueries, queryOptions, TransformFunctions.noTransform)
      case _                             =>
        val msg = "Unable to create RyftRDD. Unrecognized type of Ryft queries"
        logWarning(msg)
        throw new RyftSparkException(msg)
    }
  }

  /**
   * Creates a view of search queries to Ryft as `RyftPairRDD`
   *
   * This method is made available on [[org.apache.spark.SparkContext SparkContext]] by importing
   * `com.ryft.spark.connector._`
   *
   * @param queries Search queries
   * @param queryOptions Query specific options
   * @param choosePartitions Function provides partitions for `RyftQuery`
   * @param preferredLocations Function provided spark preferred nodes for `RyftQuery`
   */
  def ryftPairRDD(queries: Seq[RyftQuery],
                  queryOptions: RyftQueryOptions,
                  choosePartitions: RyftQuery => List[String] = _ => List.empty[String],
                  preferredLocations: RyftQuery => Set[String] = _ => Set.empty[String]) = {
    val rddQueries = createRDDQueries(queries, queryOptions, choosePartitions, preferredLocations)
    queries match {
      case (sq: SimpleQuery) :: tail =>
        new RyftPairRDD(sc, rddQueries, queryOptions, TransformFunctions.toRyftData)
      case (rq: RecordQuery) :: tail =>
        new RyftPairRDD(sc, rddQueries, queryOptions, TransformFunctions.noTransform)
      case _                             =>
        val msg = "Unable to create RyftPairRDD. Unrecognized type of Ryft queries"
        logWarning(msg)
        throw new RyftSparkException(msg)
    }
  }

  private def createRDDQueries(queries: Seq[RyftQuery],
      queryOptions: RyftQueryOptions,
      choosePartitions: RyftQuery  => List[String],
      preferredLocations: RyftQuery  => Set[String]): Seq[RDDQuery] = {
    queries.map { query =>
      val queryString = RyftQueryHelper.keyQueryPair(query, queryOptions)
      val ryftPartitions = partitions(sc.getConf, query, choosePartitions)
      RDDQuery(queryString._1, queryString._2, ryftPartitions, preferredLocations(query))
    }
  }

  private def partitions(sparkConf: SparkConf, ryftQuery: RyftQuery,
      choosePartitions: RyftQuery => List[String]): Set[String] = {
    val urlOption = sparkConf.getOption("spark.ryft.rest.url")
    val restUrls =
      if (urlOption.nonEmpty) urlOption.get
        .split(",")
        .map(url => url.trim)
        .toSet
      else ConfigHolder.ryftRestUrl.toSet

    val partitions = choosePartitions(ryftQuery)

    if (partitions.nonEmpty) partitions.toSet
    else restUrls
  }
}