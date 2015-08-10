package com.ryft.spark

import org.apache.spark.SparkContext

package object connector {
  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions =
    new SparkContextFunctions(sc)
}