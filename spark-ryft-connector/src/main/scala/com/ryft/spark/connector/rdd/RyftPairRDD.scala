package com.ryft.spark.connector.rdd

import java.net.URL

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.spark.rdd.RDD

import scala.io.Source
import scala.reflect.ClassTag

class RyftPairRDDPartition(val idx: Int,
                           val key: String,
                           val query: String) extends Partition {
  override def index: Int = idx
}


class RyftPairRDDPartitioner {
  def partitions(queries: Iterable[(String,String)]): Array[Partition] = {
    (for((query,i) <- queries.zipWithIndex) yield {
      new RyftPairRDDPartition(i, query._1, query._2)
    }).toArray[Partition]
  }
}

class RyftPairRDD [T: ClassTag](sc: SparkContext,
                                queries: Iterable[(String,String)],
                                mapLine: String => T)
  extends RDD[(String,T)](sc, Nil) {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(String, T)] = {
    val partition = split.asInstanceOf[RyftPairRDDPartition]
    val is = new URL(partition.query).openConnection().getInputStream

    val lines = Source.fromInputStream(is).getLines()
    val key = partition.key

    new Iterator[(String,T)] {
      override def next() = {
        (key, mapLine(lines.next()))
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
    logTrace("Partitions: \n" + partitions.mkString("\n"))
    partitions
  }
}
