package com.ryft.spark.connector.examples

import com.ryft.spark.connector.domain.RyftData
import com.ryft.spark.connector.util.RyftHelper
import org.apache.spark.{SparkContext, SparkConf}

object StreamExample extends App {
  val lines = scala.io.Source.fromURL(args(0)).getLines().toSeq

  val sparkConf = new SparkConf()
    .setAppName("StreamExample")
    .set("spark.locality.wait", "120s")
    .set("spark.locality.wait.node", "120s")

  val sc = new SparkContext(sparkConf)
  val r = scala.util.Random

  while(true) {
    val words = (0 until 5).map(_ => {
      lines(r.nextInt(lines.size))
    }).toList

    val ryftRDD = sc.ryftPairRDD[RyftData](words,
      List("reddit/*"), 10, 0, RyftHelper.mapToRyftData)
    val count = ryftRDD.countByKey()
    println("\n\ncount: "+count.mkString("\n"))
    Thread.sleep(10000)
  }
}
