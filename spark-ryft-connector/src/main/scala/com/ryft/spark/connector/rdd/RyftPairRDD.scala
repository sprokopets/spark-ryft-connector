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

package com.ryft.spark.connector.rdd

import com.ryft.spark.connector.rest.RyftRestConnection
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{TaskContext, Partition, SparkContext}

import scala.reflect.ClassTag

class RyftPairRDD[T: ClassTag](@transient sc: SparkContext,
    override val rddQueries: Seq[RDDQuery],
    val transform: Map[String, Any] => T,
    @transient preferredLocations: String => Set[String] = _ => Set.empty[String])
  extends RyftAbstractRDD[(String,T), T](sc, rddQueries, preferredLocations) {

  @DeveloperApi override
  def compute(split: Partition, context: TaskContext): Iterator[(String, T)] = {
    val partition = split.asInstanceOf[RDDPartition]
    val ryftRestConnection = new RyftRestConnection(partition.ryftPartition,
      partition.rddQuery, requestProps)

    val key = partition.rddQuery.ryftQuery.key
    val idx = partition.idx

    logDebug(s"Compute partition, idx: ${partition.idx}")

    new NextIterator[T, (String, T)](partition, ryftRestConnection, transform) {
      logDebug(s"Start processing iterator for partition with idx: $idx")

      override def next(): (String, T) = {
        if (accumulator.isEmpty) {
          logWarning("Next element does not exist")
          throw new RuntimeException("Next element does not exist")
        }

        val elem = transform(accumulator)
        accumulator = Map.empty[String,String]
        (key, elem)
      }
    }
  }
}