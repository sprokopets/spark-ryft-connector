package com.ryft.spark.connector

import com.ryft.spark.connector.rdd.{RyftPairRDD, RyftRDD}
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

/** Provides Ryft-specific methods on [[org.apache.spark.SparkContext SparkContext]] */
class SparkContextFunctions(@transient val sc: SparkContext) {

  def ryftRDD[T: ClassTag] (queries: List[String],
                            files: List[String],
                            surrounding: Int,
                            fuzziness: Byte,
                            converter: String => T) = {
    val complexQueries = RyftHelper.splitToPartitions(queries)
      .map({
      case (endpoint, keys) => RyftHelper.fuzzyComplexQuery(keys, endpoint, files, surrounding, fuzziness)
    })

    new RyftRDD[T](sc, complexQueries, converter)
  }

  def ryftPairRDD[T: ClassTag] (queries: List[String],
                                files: List[String],
                                surrounding: Int,
                                fuzziness: Byte,
                                converter: String => T) = {
    val fuzzyQueries = RyftHelper.fuzzyQueries(queries, files, surrounding, fuzziness)
    new RyftPairRDD[T](sc, fuzzyQueries, converter)
  }
}