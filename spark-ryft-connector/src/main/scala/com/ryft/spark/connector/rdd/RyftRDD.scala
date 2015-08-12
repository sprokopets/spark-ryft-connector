package com.ryft.spark.connector.rdd

import java.net.URL

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.spark.rdd.RDD

import scala.io.Source
import scala.reflect.ClassTag


class RyftRDDPartition(val idx: Int, val query: String) extends Partition {
  override def index: Int = idx
}

class RyftRDDPartitioner {
  def partitions(queries: Iterable[String]): Array[Partition] = {
    (for((query,i) <- queries.zipWithIndex) yield {
      new RyftRDDPartition(i, query)
    }).toArray[Partition]
  }
}

class RyftRDD[T: ClassTag](sc: SparkContext,
                                       queries: Iterable[String],
                                       mapLine: String => T)
  extends RDD[T](sc, Nil) {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val partition = split.asInstanceOf[RyftRDDPartition]
    val is = new URL(partition.query).openConnection().getInputStream
    val lines = Source.fromInputStream(is).getLines()

    new Iterator[T] {
      override def next() = {
        mapLine(lines.next())
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
    val partitioner = new RyftRDDPartitioner
    val partitions = partitioner.partitions(queries)
    logDebug(s"Created total ${partitions.length} partitions.")
    logTrace("Partitions: \n" + partitions.mkString("\n"))
    partitions
  }
}
