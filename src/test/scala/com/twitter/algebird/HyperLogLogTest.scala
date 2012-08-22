package com.twitter.algebird

import org.specs._

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Gen.choose

import java.util.Arrays

object HyperLogLogLaws extends Properties("HyperLogLog") with BaseProperties {
  import HyperLogLog._
  implicit val hllMonoid = new HyperLogLogMonoid(5) //5 bits
  implicit val hllGen = Arbitrary { for(
      v <- choose(0,10000)
    ) yield (hllMonoid(v))
  }

  property("HyperLogLog is a Monoid") = monoidLaws[HLLInstance]
}

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
     "sum different sized HLL instances properly" in {
        val larger = new HLLInstance((1L << 32) + 1)
        val smaller = new HLLInstance(2)
        Arrays.equals((smaller + larger).v, (1L << 32) + 2) must beTrue
        Arrays.equals((larger + smaller).v, (1L << 32) + 2) must beTrue
     }
  }
}
