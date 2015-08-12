package com.ryft.spark.connector

import org.apache.spark.SparkContext


package object examples {
  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions =
    new SparkContextFunctions(sc)
}
