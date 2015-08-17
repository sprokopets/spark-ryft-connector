package com.ryft.spark.connector

import com.ryft.spark.connector.rdd.RyftPairRDD
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{RDD, PairRDDFunctions}

import scala.reflect.ClassTag


package object examples {
  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions =
    new SparkContextFunctions(sc)

//  implicit def toPairRDDFunctions[String, V: ClassTag](rdd: RDD[(String, V)]): PairRDDFunctions[String, V] =
//    new PairRDDFunctions(rdd)
//  implicit def toPairRDDFunctions[V](rdd: RyftPairRDD[V]): PairRDDFunctions[String, V] =
//    new PairRDDFunctions(rdd)
}
