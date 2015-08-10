package com.ryft.spark.connector

import com.typesafe.config.ConfigFactory

/**
* Provides helper functions specific for Ryft
*/
object RyftHelper {
  private lazy val config =  ConfigFactory.load().getConfig("spark-ryft-connector")
  private lazy val a2mUrl = config.getString("ryft.url.a2m")
  private lazy val n2zUrl = config.getString("ryft.url.n2z")

  /**
   * Ryft REST fuzzy hamming query
   *
   * @param url Base URI for Ryft REST service
   * @param query Query text
   * @param files Files for search
   * @param surrounding Width when generating results. For example, a value of 2 means that 2
      characters before and after a search match will be included with data result
   * @param fuzziness - Specify the fuzzy search distance [0..255]
   * @return Ryft REST fuzzy hamming query
   */
  def prepareFuzzyQuery(url: String,
                        query: String,
                        files: List[String],
                        surrounding: Int,
                        fuzziness: Byte): String = {

    //FIXME: \" escape does not work with string interpolation
    val queryWithQuotes = "\""+query+"\""
    val queryPart = new StringBuilder(s"/?query=(RAW_TEXT%20CONTAINS$queryWithQuotes)")
    for(f <- files) {
      queryPart.append(s"&files=$f")
    }
    queryPart.append(s"&surrounding=$surrounding")
    queryPart.append(s"&fuzziness=$fuzziness")
    url+queryPart
  }

  /**
   * Chooses Ryft partition according to search query
   * @param text Search query
   * @return Ryft URI
   */
  //TODO: Possible to add few partition strategies and customize them using properties
  //FIXME: Implemented simple logic, search query should start with letter of Eng alphabet
  def choosePartition(text: String): String = {
    a2mUrl
//    if(('a' to 'm').contains(text.toLowerCase.charAt(0))) a2mUrl
//    if(('n' to 'z').contains(text.toLowerCase.charAt(0))) n2zUrl

//    throw new RuntimeException(("Search query starts with an unknown character: '%c'. " +
//      "Unable to choose partition").format(text(0)))
  }
}