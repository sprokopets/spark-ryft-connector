package com.ryft.spark.connector

import java.net.URL

import com.google.common.base.Charsets
import com.google.common.io.BaseEncoding
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.io.Source
import scala.util.Try

class RyftRDDPartition(val idx: Int) extends Partition {
  override val index: Int = idx
  override def hashCode(): Int = idx
}

/**
 * Custom Ryft RDD. Encapsulates REST url and mapping function for set of data produced by Ryft
 *
 * @param sc  SparkContext
 * @param url Ryft REST URL
 * @param mapLine function to map String -> T. Should throw [[com.ryft.spark.connector.JsonMappingException]]
 *                if String cannot be mapped to T
 * @tparam T Custom POJO
 */
class RyftRDD[T: ClassTag](sc: SparkContext,
                           url: String,
                           mapLine: String => T) extends RDD[T](sc, Nil) with Logging {
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val is = new URL(url).openConnection().getInputStream
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

  override protected def getPartitions: Array[Partition] =
  //FIXME: hardcoded, need to understand partitioning
    Array.tabulate[Partition](1)(i => new RyftRDDPartition(i))

  private def base64ToString(text: String) =  BaseEncoding.base64().encode(text.getBytes)
}
