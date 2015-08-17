package com.ryft.spark.connector.examples

import java.io.{BufferedWriter, FileWriter}
import com.ryft.spark.connector.domain.RyftData
import com.ryft.spark.connector.util.RyftHelper
import org.apache.spark.{SparkContext, SparkConf}

object SimplePairRDDExample extends App {

  val sparkConf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("TwitterSportTag")

  val sc = new SparkContext(sparkConf)

  val fw = new FileWriter("/result.log", true)
  val out = new BufferedWriter(fw)

  val ryftRDD = sc.ryftPairRDD[RyftData](List("Jones","Thomas", "Alex", "Victor", "SomethingDefinitlyNotExists"),
    List("reddit/*"), 10, 0, RyftHelper.mapToRyftData)

  val count = ryftRDD.countByKey()
  out.write("Count: \n"+count.mkString("\n"))
  out.newLine()
  out.flush()
}
