package com.ryft.spark.connector

import com.typesafe.config.ConfigFactory

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
        s"fuzzy-hamming/?query=(RAW_TEXT%20CONTAINS$queryWithQuotes)")
      
      for(f <- files) {
        queryPart.append(s"&files=$f")
      }
      queryPart.append(s"&surrounding=$surrounding")
      queryPart.append(s"&fuzziness=$fuzziness")
      
      (query, choosePartition(query)+queryPart.toString)
    })
  }
  
  def fuzzyComplexQuery(queries: List[String],
                        endpoint: String,
                        files: List[String],
                        surrounding: Int,
                        fuzziness: Byte): String = {
    val queriesOR = (for ((q, i) <- queries.zipWithIndex)
      yield {
        val queryWithQuotes = "\"" + q + "\""
        val rawTextQuery = s"(RAW_TEXT%20CONTAINS$queryWithQuotes)"
        if (i+1 != queries.length) rawTextQuery+"OR"
        else rawTextQuery
      }).mkString("")

    val queryPart = new StringBuilder(s"/search/fuzzy-hamming/?query=$queriesOR")
    for(f <- files) {
      queryPart.append(s"&files=$f")
    }
    queryPart.append(s"&surrounding=$surrounding")
    queryPart.append(s"&fuzziness=$fuzziness")
    endpoint+queryPart.toString
  }

  /**
   * Chooses Ryft partition according to search query
   * @param text Search query
   * @return Ryft URI
   */
  def choosePartition(text: String): String = {
    //FIXME: return not scala aproach
    if(('a' to 'm').indexOf(text.toLowerCase.charAt(0)) != -1) return a2mUrl
    if(('n' to 'z').indexOf(text.toLowerCase.charAt(0)) != -1) return n2zUrl

    throw new RuntimeException(("Search query starts with an unknown character: '%c'. " +
      "Unable to choose partition").format(text(0)))
  }
}