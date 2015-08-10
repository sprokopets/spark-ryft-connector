package com.ryft.spark.connector

import org.apache.spark.SparkContext

import scala.reflect.ClassTag

class RyftConnectorApi(sc: SparkContext) {
  def exact(text: String) = ???

  /**
   * Provides fuzzy hamming search on Ryft REST Api
   *
   * @param queries Text queries for search
   * @param files Files for search
   * @param surrounding Width when generating results. For example, a value of 2 means that 2
      characters before and after a search match will be included with data result
   * @param fuzziness Specify the fuzzy search distance [0..255]
   * @param converter Function to map String -> T
   * @return Sequence of pairs (String, RyftRDD[T]), each RDD contains values for specified query
   */
  def fuzzy[T: ClassTag](queries: Seq[String],
            files: List[String],
            surrounding: Int,
            fuzziness: Byte,
            converter: String => T): Seq[(String, RyftRDD[T])] = {
    queries.map(q => (q, sc.ryftRdd(q, files, surrounding, fuzziness, converter)))
  }}
