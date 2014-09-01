package com.twitter.algebird

import org.scalatest._

import org.scalatest.{ PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks
import org.scalacheck.{ Gen, Arbitrary }

import java.util.Arrays

class MinHasherTest extends PropSpec with PropertyChecks with Matchers {
  import BaseProperties._

  implicit val mhMonoid = new MinHasher32(0.5, 512)
  implicit val mhGen = Arbitrary {
    for (
      v <- Gen.choose(0, 10000)
    ) yield (mhMonoid.init(v))
  }

  property("MinHasher is a Monoid") {
    monoidLawsEq[MinHashSignature]{ (a, b) => a.bytes.toList == b.bytes.toList }
  }
}

class MinHasherSpec extends WordSpec with Matchers {
  val r = new java.util.Random

  def test[H](mh: MinHasher[H], similarity: Double, epsilon: Double) = {
    val (set1, set2) = randomSets(similarity)

    val exact = exactSimilarity(set1, set2)
    val sim = approxSimilarity(mh, set1, set2)
    val error: Double = math.abs(exact - sim)
    assert(error < epsilon)
  }

  def randomSets(similarity: Double) = {
    val s = 10000
    val uniqueFraction = if (similarity == 1.0) 0.0 else (1 - similarity) / (1 + similarity)
    val sharedFraction = 1 - uniqueFraction
    val unique1 = 1.to((s * uniqueFraction).toInt).map{ i => math.random }.toSet
    val unique2 = 1.to((s * uniqueFraction).toInt).map{ i => math.random }.toSet

    val shared = 1.to((s * sharedFraction).toInt).map{ i => math.random }.toSet
    (unique1 ++ shared, unique2 ++ shared)
  }

  def exactSimilarity[T](x: Set[T], y: Set[T]) = {
    (x & y).size.toDouble / (x ++ y).size
  }

  def approxSimilarity[T, H](mh: MinHasher[H], x: Set[T], y: Set[T]) = {
    val sig1 = x.map{ l => mh.init(l.toString) }.reduce{ (a, b) => mh.plus(a, b) }
    val sig2 = y.map{ l => mh.init(l.toString) }.reduce{ (a, b) => mh.plus(a, b) }
    mh.similarity(sig1, sig2)
  }

  "MinHasher32" should {
    "measure 0.5 similarity in 1024 bytes with < 0.1 error" in {
      test(new MinHasher32(0.5, 1024), 0.5, 0.1)
    }
    "measure 0.8 similarity in 1024 bytes with < 0.05 error" in {
      test(new MinHasher32(0.8, 1024), 0.8, 0.05)
    }
    "measure 1.0 similarity in 1024 bytes with < 0.01 error" in {
      test(new MinHasher32(1.0, 1024), 1.0, 0.01)
    }
  }
}
