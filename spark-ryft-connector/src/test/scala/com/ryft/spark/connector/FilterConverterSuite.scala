package com.ryft.spark.connector

import com.ryft.spark.connector.util.FilterConverter
import org.apache.spark.sql.sources.{Or, And, EqualTo}
import org.junit.Test
import org.scalatest.junit.JUnitSuite
import org.junit.Assert._
import com.ryft.spark.connector.query._

class FilterConverterSuite extends JUnitSuite {
  @Test def simpleFilterConversion() = {
    val f = EqualTo("attribute", "value")
    val rq = FilterConverter.filtersToRecordQuery(Array(f))
    assertNotNull(rq)
    assertNotNull(rq.filters)
    assert(rq.filters.size == 1)
    assertEquals(rq.filters.head, filter.EqualTo("RECORD.attribute", "value"))
  }

  @Test def simpleOrFilterConversion() = {
    val orF = Or(EqualTo("attribute0", "value0"), EqualTo("attribute1", "value1"))
    val rq = FilterConverter.filtersToRecordQuery(Array(orF))
    assertNotNull(rq)
    assertNotNull(rq.filters)
    assert(rq.filters.size == 1)
    val ryftOr = rq.filters.head.asInstanceOf[filter.Or]
    assertEquals(ryftOr.left, filter.EqualTo("RECORD.attribute0", "value0"))
    assertEquals(ryftOr.right, filter.EqualTo("RECORD.attribute1", "value1"))
  }

  @Test def simpleAndFilterConversion() = {
    val orF = And(EqualTo("attribute0", "value0"), EqualTo("attribute1", "value1"))
    val rq = FilterConverter.filtersToRecordQuery(Array(orF))
    assertNotNull(rq)
    assertNotNull(rq.filters)
    assert(rq.filters.size == 1)
    val ryftAnd = rq.filters.head.asInstanceOf[filter.And]
    assertEquals(ryftAnd.left, filter.EqualTo("RECORD.attribute0", "value0"))
    assertEquals(ryftAnd.right, filter.EqualTo("RECORD.attribute1", "value1"))
  }
}
