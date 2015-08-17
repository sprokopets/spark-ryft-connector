package com.ryft.spark.connector.util

import com.ryft.spark.connector.domain.RyftData
import com.typesafe.config.ConfigFactory
import org.apache.commons.codec.binary.Base64
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._

import scala.util.Try

/**
* Provides helper functions specific for Ryft
*/
object RyftHelper {
  private lazy val config =  ConfigFactory.load().getConfig("spark-ryft-connector")
  private lazy val a2mUrl = config.getString("ryft.url.a2m")
  private lazy val n2zUrl = config.getString("ryft.url.n2z")

  def splitToPartitions(queries: List[String]) = {
    queries.map(q => (choosePartition(q), q))
      .groupBy(_._1)
      .mapValues(_.map(_._2))
  }

  def fuzzyQueries(queries: List[String],
                 files: List[String],
                 surrounding: Int,
                 fuzziness: Byte): List[(String,String)] = {
    queries.map(query => {
      val queryWithQuotes = "\"" + query + "\""
      val queryPart = new StringBuilder(s"/search/" +
        s"?query=(RAW_TEXT%20CONTAINS$queryWithQuotes)")
      
      for(f <- files) {
        queryPart.append(s"&files=$f")
      }
      queryPart.append(s"&surrounding=$surrounding")
      queryPart.append(s"&fuzziness=$fuzziness")
      
      (query, choosePartition(query)+queryPart.toString)
    })
  }
  
  def mapToRyftData(line: String): Option[RyftData] = {
    implicit val formats = DefaultFormats

    val open = line.indexOf('{')
    if (open == -1) None
    else {
      val close = line.lastIndexOf('}')
      val jsonLine = line.substring(open, close)

      val ryftDataOption = Try(parse(jsonLine).extract[RyftData])
      if (ryftDataOption.isSuccess) {
        val ryftData = ryftDataOption.get
        Some(new RyftData(ryftData.file, ryftData.offset,
                          ryftData.length, ryftData.fuzziness,
                          new String(Base64.decodeBase64(ryftData.data))))
      } else None
    }
  }

  /**
   * Chooses Ryft partition according to search query
   * @param text Search query
   * @return Ryft URI
   */
  private def choosePartition(text: String): String = {
    if(('a' to 'm').indexOf(text.toLowerCase.charAt(0)) != -1) a2mUrl
    else if(('n' to 'z').indexOf(text.toLowerCase.charAt(0)) != -1) n2zUrl
    else throw new RuntimeException(("Search query starts with an unknown character: '%c'. " +
      "Unable to choose partition").format(text(0)))
  }
}