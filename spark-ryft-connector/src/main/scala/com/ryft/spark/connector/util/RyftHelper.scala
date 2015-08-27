package com.ryft.spark.connector.util

import com.ryft.spark.connector.domain.RyftData
import org.apache.commons.codec.binary.Base64
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._

import scala.util.Try

/**
* Provides helper functions specific for Ryft
*/
object RyftHelper {
  def fuzzyQueries(queries: List[String],
                 files: List[String],
                 surrounding: Int,
                 fuzziness: Byte): List[(String,String,Seq[String])] = {
    queries.flatMap(query => {
      val queryWithQuotes = "\"" + query + "\""
      val queryPart = new StringBuilder(s"/search" +
        s"?query=(RAW_TEXT%20CONTAINS$queryWithQuotes)")
      
      for(f <- files) {
        queryPart.append(s"&files=$f")
      }
      queryPart.append(s"&surrounding=$surrounding")
      queryPart.append(s"&fuzziness=$fuzziness")

      val partitions = PartitioningHelper.choosePartitions(query)
      val fuzzyQueries = partitions.map(p => {
        (query, p.endpoint+queryPart.toString, p.preferredLocations)
      })

      fuzzyQueries
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
}