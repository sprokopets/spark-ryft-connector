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

import com.ryft.spark.connector.domain.query.{RyftRecordQuery, SimpleRyftQuery}
import com.ryft.spark.connector.domain.RyftMetaInfo
import com.ryft.spark.connector.rdd.{RyftRDDSimple, RyftPairRDD}
import com.ryft.spark.connector.util.{TransformFunctions, PartitioningHelper, RyftHelper}
import org.apache.spark.SparkContext

/**
 * Provides Ryft-specific methods on [[org.apache.spark.SparkContext SparkContext]]
 *
 */
class SparkContextFunctions(@transient val sc: SparkContext) {
  def ryftRDDStructured[Map](queries: List[RyftRecordQuery],
                                 metaInfo: RyftMetaInfo) = {
    val preparedQueries = RyftHelper.prepareQueriesRecord(queries, metaInfo)
    new RyftRDDSimple(sc, preparedQueries, TransformFunctions.noTransform)
  }

  def ryftRDDSimple[RyftData](queries: List[RyftRecordQuery],
                              metaInfo: RyftMetaInfo) = {
    val preparedQueries = RyftHelper.prepareQueriesRecord(queries, metaInfo)
    new RyftRDDSimple(sc, preparedQueries, TransformFunctions.toRyftData)
  }

  def ryftPairRDDStructured[Map](queries: List[RyftRecordQuery],
                                 metaInfo: RyftMetaInfo) = {
    val preparedQueries = RyftHelper.prepareQueriesRecord(queries, metaInfo)
    new RyftPairRDD(sc, preparedQueries, TransformFunctions.noTransform)
  }

  def ryftPairRDD[RyftData](queries: List[SimpleRyftQuery],
                            metaInfo: RyftMetaInfo) = {
    val preparedQueries = RyftHelper.prepareQueriesSimple(queries, metaInfo)
    new RyftPairRDD(sc, preparedQueries, TransformFunctions.toRyftData)
  }
}