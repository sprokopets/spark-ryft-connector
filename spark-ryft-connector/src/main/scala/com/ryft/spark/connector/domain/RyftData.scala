package com.ryft.spark.connector.domain

//import com.ryft.spark.connector.util.JsonHelper
//import org.json4s.JsonAST.JObject
//import org.json4s.JsonDSL.WithBigDecimal._

/**
 * Represents pojo Ryft REST returns
 * @param file File where query was found
 * @param offset Offset in bytes from the beginning of the source file
 * @param length Length of the data found
 * @param fuzziness Fuzziness of the data
 * @param data Query with surrounding (if surrounding was specified)
 */
case class RyftData (file: String,
                     offset: Int,
                     length: Int,
                     fuzziness: Byte,
                     data: String) {
  def this() = this("", 0, 0, 0, "")

  def nonEmpty = data.nonEmpty

//  override def toString: String = {
//    JsonHelper.toJsonPretty(
//      JObject(
//        "file"      -> file,
//        "offset"    -> offset,
//        "length"    -> length,
//        "fuzziness" -> fuzziness,
//        "data"      -> data
//  ))}
}