package com.ryft.spark.connector.util

import scala.reflect.ClassTag

object JavaApiHelper {
  /** Returns a `ClassTag` of a given runtime class. */
  def getClassTag[T](clazz: Class[T]): ClassTag[T] = ClassTag(clazz)

  /** Converts an array to a Scala `Seq`. */
  def toScalaSeq[T](array: Array[T]): Seq[T] = array
}
