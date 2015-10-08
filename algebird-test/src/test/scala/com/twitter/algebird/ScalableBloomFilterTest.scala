package com.twitter.algebird

import org.scalatest._
import scala.util.Random

class ScalableBloomFilterTest extends WordSpec {

  def compoundedErrorRate(fpProb: Double, tighteningRatio: Double, numFilters: Int) = {
    1 - (0 to numFilters).foldLeft(1.0)(_ * 1 - fpProb * math.pow(tighteningRatio, _))
  }

  "A ScalableBloomFilter" should {

    "not grow for repeated items" in {
      var sbf = ScalableBloomFilter(0.01, 256)
      assert(sbf.count == 1)
      assert(sbf.size.estimate == 0)

      (0 to 100).foreach { _ => sbf += "test" }
      assert(sbf.contains("test"))
      assert(sbf.count == 1)
      assert(sbf.size.estimate == 1)
    }

    "converge below the compounded false probability rate" in {
      val fpProb = 0.001
      val tr = 0.5
      var sbf = ScalableBloomFilter(fpProb, initialCapacity = 64, growthRate = 4, tighteningRatio = tr)
      val inserts = 500
      val trials = 100000

      // insert a bunch of random 16 character strings
      val random = new Random(42)
      (0 until inserts).foreach { _ => sbf += random.nextString(16) }

      // check for the presense of any 8 character strings
      val fpCount = (0 until trials).count(_ => sbf.contains(random.nextString(8))).toDouble
      assert(fpCount / trials <= compoundedErrorRate(fpProb, tr, sbf.count))
    }

    "grow at the given growth rate" in {
      val initialCapacity = 2
      var sbf = ScalableBloomFilter(0.001, initialCapacity, growthRate = 2, 1.0)
      assert(sbf.count == 1)

      (0 until 100).foreach { i => sbf += "item" + i }
      assert(sbf.count == 6) // filter sizes: 2 + 4 + 8 + 16 + 32 + 64 = 126 > 100

      var sbf2 = ScalableBloomFilter(0.001, initialCapacity, growthRate = 4, 1.0)
      assert(sbf2.count == 1)

      (0 until 100).foreach { i => sbf2 += "item" + i }
      assert(sbf2.count == 4) // filter sizes: 2 + 8 + 64 + 512 > 100
    }

    "provide size as the sum of underlying sizes" in {
      var sbf = ScalableBloomFilter(0.00001, 128, growthRate =  4, tighteningRatio = 0.9)
      val random = new Random(42)
      (0 to 1000).foreach { i => sbf += random.nextString(8) }
      val actual = sbf.size.estimate
      val expected = sbf.filters.foldLeft(0L)(_ + _.size.estimate)
      assert(actual == expected)
    }

    "be able to handle multiple batch adds" in {
      val trials = 10000
      val fpProb = 0.01
      val tighteningRatio = 0.5
      val initialCapacity = 64

      val random = new Random(42)
      val gids = (0 until trials / 2).map(i => random.nextString(8))
      val gids2 = (0 until trials / 2).map(i => random.nextString(16))

      val initialSbf = ScalableBloomFilter(fpProb, initialCapacity, 2, tighteningRatio)
      val populatedSbf = initialSbf ++ gids.iterator ++ gids2.iterator

      val expectedNumberOfFilters = (Math.log(trials) / Math.log(2) - Math.log(initialCapacity) / Math.log(2) + 1).toInt
      assert(populatedSbf.count == expectedNumberOfFilters)

      val fpCount =
        (gids ++ gids2).foldLeft(0) { (count: Int, gid: String) =>
          assert(populatedSbf.contains(gid))
          if (populatedSbf.contains(gid + "1")) count + 1 else count
        }
      assert(fpCount / trials <= compoundedErrorRate(fpProb, tighteningRatio, populatedSbf.count))
    }

    "++ and multiple + should result in functionality equivalent filters" in {
      val trials = 1000
      val r = new Random(42)
      val gids = (0 until trials).map(i => r.nextString(8))
      val fpProb = 0.01
      val initialCapacity = 64
      val tr = 0.9
      val sbf1 = gids.foldLeft(ScalableBloomFilter(fpProb, initialCapacity, 2, tr))(_ + _)
      val sbf2 = ScalableBloomFilter(fpProb, initialCapacity, 2, tr) ++ gids.iterator
      assert(compoundedErrorRate(fpProb, tr, sbf1.count) == compoundedErrorRate(fpProb, tr, sbf2.count))
      assert(sbf1.headCapacity == sbf2.headCapacity)
      assert(sbf1.fpProb == sbf2.fpProb)
      gids.foreach{ item => assert(sbf1.contains(item) && sbf2.contains(item)) }
    }
  }
}
