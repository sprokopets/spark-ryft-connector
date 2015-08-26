package com.ryft.spark.connector.rdd

import java.net.{InetAddress, URL}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.spark.rdd.RDD

//import com.ryft.spark.connector.util.JsonHelper
//import org.json4s.JsonAST.JObject
//import org.json4s.JsonDSL.WithBigDecimal._

import scala.io.Source
import scala.reflect.ClassTag

class RyftPairRDDPartition(val idx: Int,
                           val key: String,
                           val query: String,
                           val preferredLocations: Seq[String]) extends Partition {
  override def index: Int = idx
//  override def toString: String = {
//    JsonHelper.toJsonPretty(
//      JObject(
//        "idx" -> idx,
//        "key" -> key,
//        "query" -> query
//  ))}
}

class RyftPairRDDPartitioner {
  def partitions(queries: Iterable[(String,String, Seq[String])]): Array[Partition] = {
    (for((query,i) <- queries.zipWithIndex) yield {
      new RyftPairRDDPartition(i, query._1, query._2, query._3)
    }).toArray[Partition]
  }
}

case class RyftPairRDD [T: ClassTag](@transient sc: SparkContext,
                                queries: Iterable[(String,String,Seq[String])],
                                mapLine: String => Option[T])
  extends RDD[(String,T)](sc, Nil) {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(String, T)] = {
    val partition = split.asInstanceOf[RyftPairRDDPartition]
    val is = new URL(partition.query).openConnection().getInputStream
    val key = partition.key
    val lines = Source.fromInputStream(is)
      .getLines()
      .filter(l => mapLine(l).nonEmpty)

    new Iterator[(String,T)] {
      override def next(): (String,T) = {
        val line = mapLine(lines.next())
        (key, line.get)
      }

      override def hasNext: Boolean = {
        if (lines.hasNext) true
        else {
          is.close()
          false
        }
      }
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val partitioner = new RyftPairRDDPartitioner
    val partitions = partitioner.partitions(queries)
    logDebug(s"Created total ${partitions.length} partitions.")
    logDebug("Partitions: \n" + partitions.mkString("\n"))
    partitions
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val partition = split.asInstanceOf[RyftPairRDDPartition]
    logDebug("Preferred locations for partition: \n"+partition.preferredLocations.mkString("\n"))
    partition.preferredLocations
  }
}
