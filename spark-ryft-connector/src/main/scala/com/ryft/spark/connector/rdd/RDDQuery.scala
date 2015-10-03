package com.ryft.spark.connector.rdd

import com.ryft.spark.connector.RyftSparkException
import org.apache.spark.Logging

/**
 * Represents entity to specify query to Ryft Rest service
 * and set of spark nodes preferred to use for this query.
 *
 * @param key Key for the query
 * @param query Query to Ryft Rest service
 * @param preferredLocation Set of preferred spark nodes
 */
case class RDDQuery(key: String, query: String, preferredLocation: Set[String])

object RDDQuery extends Logging {
  def apply(query: String) = {
    val ryftQuery = subStringBefore(subStringAfter(query, "query="), "&files")
    new RDDQuery(ryftQuery, query, Set.empty[String])
  }

  def apply(key: String, query: String) = new RDDQuery(key, query, Set.empty[String])

  private def subStringAfter(s:String, k:String) = {
    s.indexOf(k) match {
      case i => s.substring(i+k.length)
      case _ =>
        val msg = s"$k Does not exist in string: $s"
        logWarning(msg)
        throw new RyftSparkException(msg)
    }
  }

  private def subStringBefore(s: String, k: String) = {
    s.indexOf(k) match {
      case i => s.substring(0, i)
      case _ =>
        val msg = s"$k Does not exist in string: $s"
        logWarning(msg)
        throw new RyftSparkException(msg)
    }
  }

}