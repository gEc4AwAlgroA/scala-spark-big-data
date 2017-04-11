package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import TimeUsage._

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {

  test("'dfSchema' should work for made up assessment") {
    val cols = List("foo", "bar", "baz")
    val res = dfSchema(cols)
    val ans = StructType(
      StructField("foo",StringType,nullable = false) ::
        StructField("bar",DoubleType,nullable = false) ::
        StructField("baz",DoubleType,nullable = false) :: Nil)
    assert(res === ans, "'dfSchema' should work for made up assessment")
  }

  test("simple row with doubles") {
    val vals = List("foo", "1.0", "2.0")
    val res = row(vals)
    val ans = Row("foo", 1.0, 2.0)
    assert(res === ans, "simple row")
  }

}
