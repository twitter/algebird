package com.twitter.algebird

import org.specs._

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Gen.choose

import java.lang.AssertionError
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

  def approxCountBuilder[T <% Array[Byte]](bits : Int, it : Iterable[T]) = {
    val hllBuilder = it.foldLeft(new HLLInstanceBuilder(bits)) { (left, right) => left.add(right) }
    val hll = new HyperLogLogMonoid(bits)
    hll.estimateSize(hllBuilder.build())
  }

  def aveErrorOf(bits : Int) : Double = 1.04/scala.math.sqrt(1 << bits)

  def exactIntersect[T](it : Seq[Iterable[T]]) : Int = {
    it.foldLeft(Set[T]()) { (old, newS) => old ++ (newS.toSet) }.size
  }
  def approxIntersect[T <% Array[Byte]](bits : Int, it : Seq[Iterable[T]]) : Double = {
    val hll = new HyperLogLogMonoid(bits)
    //Map each iterable to a HLL instance:
    val seqHlls = it.map { iter => hll.sum(iter.view.map { hll(_) }) }
    hll.estimateIntersectionSize(seqHlls)
  }

  def test(bits : Int) {
    val data = (0 to 10000).map { i => r.nextInt(1000) }
    val exact = exactCount(data).toDouble
    scala.math.abs(exact - approxCount(bits, data)) / exact must be_<(2.5 * aveErrorOf(bits))
    scala.math.abs(exact - approxCountBuilder(bits, data)) / exact must be_<(2.5 * aveErrorOf(bits))
  }
  def testLong(bits : Int) {
    val data = (0 to 10000).map { i => r.nextLong }
    val exact = exactCount(data).toDouble
    scala.math.abs(exact - approxCount(bits, data)) / exact must be_<(2.5 * aveErrorOf(bits))
    scala.math.abs(exact - approxCountBuilder(bits, data)) / exact must be_<(2.5 * aveErrorOf(bits))
  }
  def testLongIntersection(bits : Int, sets : Int) {
    val data : Seq[Iterable[Int]] = (0 until sets).map { idx =>
      (0 to 1000).map { i => r.nextInt(100) }
    }.toSeq
    val exact = exactIntersect(data)
    val errorMult = scala.math.pow(2.0, sets) - 1.0
    scala.math.abs(exact - approxIntersect(bits, data)) / exact must be_<(errorMult *
      aveErrorOf(bits))
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
     "count intersections of 2" in { testLongIntersection(10,2) }
     "count intersections of 3" in { testLongIntersection(10,3) }
     "count intersections of 4" in { testLongIntersection(10,4) }

     "throw error for differently sized HLL instances" in {
        val larger = new HLLInstance((1L << 32) + 1) // uses implicit long2Bytes to make 8 byte array
        val smaller = new HLLInstance(2) // uses implicit int2Bytes to make 4 byte array
        (larger + smaller) must throwA[AssertionError]
     }
  }
}
