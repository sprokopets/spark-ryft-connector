package com.ryft.spark.connector.examples

import java.io.{BufferedWriter, FileWriter}
import com.ryft.spark.connector.domain.RyftData
import com.ryft.spark.connector.util.RyftHelper
import org.apache.spark.{SparkContext, SparkConf}

object SimplePairRDDExample extends App {
  val sparkConf = new SparkConf()
    .setAppName("SimplePairRDDExample")

  val sc = new SparkContext(sparkConf)

  val fw = new FileWriter(args(0), true)
  val out = new BufferedWriter(fw)

  val ryftRDD = sc.ryftPairRDD[RyftData](List("Jones","alex","andrey","borys","anna"),
    List("reddit/*"), 10, 0, RyftHelper.mapToRyftData)

  val count = ryftRDD.countByKey()
  println("\n\ncount: "+count.mkString("\n"))
  out.write("Count: \n"+count.mkString("\n"))
  out.newLine()
  out.flush()
}
