package com.ryft.spark.connector

import org.apache.spark.SparkContext

import scala.reflect.ClassTag

/** Provides Ryft-specific methods on [[org.apache.spark.SparkContext SparkContext]] */
class SparkContextFunctions(@transient val sc: SparkContext) {

  /**
   *  Creates custom RyftRDD
   *
   * This method is made available on [[org.apache.spark.SparkContext SparkContext]] by importing
   * `com.ryft.spark.connector._`
   *
   * @param query Search query
   * @param files Files to search
   * @param surrounding Width when generating results. For example, a value of 2 means that 2
      characters before and after a search match will be included with data result
   * @param fuzziness Specify the fuzzy search distance [0..255]s
   * @param converter Function to map String -> T
   * @return custom RyftRDD
   */
  def ryftRdd[T: ClassTag] (query: String,
                            files: List[String],
                            surrounding: Int,
                            fuzziness: Byte,
                            converter: String => T) = {
    val partitionUrl = RyftHelper.choosePartition(query)
    val fuzzyQuery = RyftHelper.prepareFuzzyQuery(partitionUrl, query, files, surrounding, fuzziness)
    new RyftRDD[T](sc, fuzzyQuery, converter)
  }
}