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

import java.net.URL

import com.ryft.spark.connector.config.ConfigHolder
import com.ryft.spark.connector.preferred.location.{PreferredLocationsFactory, NoPreferredLocation}
import spray.json._
import com.ryft.spark.connector.domain.RDDCountProtocol._
import com.ryft.spark.connector.domain.{RDDCount, Count}
import com.ryft.spark.connector.rest.RyftRestConnection
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.Try

/**
 * RDD representing of a RyftQuery.
 *
 * This class is the main entry point for analyzing data using Ryft with Spark.
 */
abstract class RyftAbstractRDD [T: ClassTag, R](@transient sc: SparkContext,
    val rddQueries: Seq[RDDQuery],
    @transient preferredLocations: String => Set[String])
  extends RDD[T](sc, Nil) {

  //FIXME: sparkConf not serializable, for now keep request properties here
  protected val requestProps = ConfigHolder.requestProps(sc.getConf)

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T]

  override def count(): Long = {
    Try(ryftCount).getOrElse{
      logInfo("Ryft Rest count endpoint failed. Using default spark count function")
      super.count()
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val partitioner = new RDDPartitioner
    val partitions = partitioner.partitions(rddQueries)
    logInfo(s"Created total ${partitions.length} partitions.")
    logInfo("Partitions: \n" + partitions.mkString("\n"))
    partitions
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    logInfo("Looking for preferred locations")
    val partition = split.asInstanceOf[RDDPartition]
    val nodeLocator = PreferredLocationsFactory.byName(sc.getConf.get("spark.ryft.preferred.locations",
      classOf[NoPreferredLocation].getCanonicalName))
    val funPreferredLocations = preferredLocations(partition.ryftPartition.toString)

    val locations =
      if (funPreferredLocations.nonEmpty) funPreferredLocations
      else nodeLocator.nodes(partition.ryftPartition).map(_.toString)

    logInfo(("Preferred locations for partition:" +
      "\npartition idx: %s" +
      "\nlocations: %s")
      .format(partition.idx, locations.mkString("\n")))

    locations.toSeq
  }

  private def ryftCount: Long = {
    getPartitions.map { p =>
      val partition = p.asInstanceOf[RDDPartition]
      val countRDDQuery = partition.rddQuery.copy(action = Count)
      val connection = new RyftRestConnection(partition.ryftPartition, countRDDQuery)

      //FIXME: `toLowerCase` workaround, need to find more elegant solution
      val res = connection.result.trim.toLowerCase
      res.parseJson
        .convertTo[RDDCount].matches
    }.sum
  }
}

/**
 * Describes `RyftRDD` partition
 *
 * @param idx Identifier of the partition, used internally by Spark
 * @param ryftPartition Ryft rest URL
 * @param rddQuery RDD query
 */
case class RDDPartition(idx: Int,
    ryftPartition: URL,
    rddQuery: RDDQuery)
  extends Partition {
  override def index: Int = idx
  override def toString: String = {
    JsObject(Map(
      "idx" -> JsNumber(idx),
      "key" -> JsString(rddQuery.ryftQuery.key),
      "ryftQuery" -> JsString(rddQuery.ryftQuery.toRyftQuery),
      "ryftPartition" -> JsString(ryftPartition.toString)
    )).toJson.prettyPrint
  }
}

/**
 * Simple `RyftRDD` partitioner
 */
class RDDPartitioner {
  def partitions(rddQueries: Seq[RDDQuery]): Array[Partition] = {
    var index = 0
    (for(rddQuery <- rddQueries) yield {
      (for (ryftPartition <- rddQuery.ryftPartitions) yield {
        val partition = RDDPartition(index, ryftPartition, rddQuery)
        index += 1
        partition
      }).toArray[Partition]
    }).flatten.toArray[Partition]
  }
}