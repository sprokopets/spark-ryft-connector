package com.ryft.spark.connector

import com.ryft.spark.connector.rdd.RyftPairRDD
import com.ryft.spark.connector.util.RyftHelper
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

/** Provides Ryft-specific methods on [[org.apache.spark.SparkContext SparkContext]] */
class SparkContextFunctions(@transient val sc: SparkContext) {

  /**
   *
   * @param queries
   * @param files
   * @param surrounding
   * @param fuzziness
   * @param converter
   * @tparam T
   * @return
   */
  def ryftPairRDD[T: ClassTag] (queries: List[String],
                                files: List[String],
                                surrounding: Int,
                                fuzziness: Byte,
                                converter: String => Option[T]) = {
    val fuzzyQueries = RyftHelper.fuzzyQueries(queries, files, surrounding, fuzziness)
    new RyftPairRDD[T](sc, fuzzyQueries, converter)
  }
}