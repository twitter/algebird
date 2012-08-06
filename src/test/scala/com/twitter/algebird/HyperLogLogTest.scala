package com.twitter.algebird

import org.specs._

class HyperLogLogTest extends Specification {
  noDetailedDiffs()
  import HyperLogLog._ //Get the implicit int2bytes, long2Bytes

  val r = new java.util.Random

  def exactCount[T](it : Iterable[T]) : Int = it.toSet.size
  def approxCount[T <% Array[Byte]](bits : Int, it : Iterable[T]) = {
    val hll = new HyperLogLogMonoid(bits)
    hll.estimateSize(hll.sum(it.map { hll(_) }))
  }
  def aveErrorOf(bits : Int) : Double = 1.04/scala.math.sqrt(1 << bits)

  def test(bits : Int) {
    val data = (0 to 10000).map { i => r.nextInt(1000) }
    val exact = exactCount(data).toDouble
    scala.math.abs(exact - approxCount(bits, data)) / exact must be_<(2.5 * aveErrorOf(bits))
  }
  def testLong(bits : Int) {
    val data = (0 to 10000).map { i => r.nextLong }
    val exact = exactCount(data).toDouble
    scala.math.abs(exact - approxCount(bits, data)) / exact must be_<(2.5 * aveErrorOf(bits))
  }

  "HyperLogLog" should {
     "count with 4-bits" in {
        test(4)
        testLong(4)
     }
     "count with 5-bits" in {
        test(5)
        testLong(5)
     }
     "count with 6-bits" in {
        test(6)
        testLong(6)
     }
     "count with 7-bits" in {
        test(7)
        testLong(7)
     }
  }
}
