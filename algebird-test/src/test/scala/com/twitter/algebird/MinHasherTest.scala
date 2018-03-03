package com.twitter.algebird

import com.twitter.algebird.BaseProperties._
import org.scalacheck.{ Arbitrary, Gen }
import org.scalatest.{ Matchers, _ }
import scala.math.Equiv

class MinHasherTest extends CheckProperties {
  implicit val mhMonoid = new MinHasher32(0.5, 512)
  implicit val mhGen = Arbitrary {
    for (
      v <- Gen.choose(0, 10000)
    ) yield (mhMonoid.init(v))
  }

  property("MinHasher is a commutative monoid") {
    implicit val equiv: Equiv[MinHashSignature] = Equiv.by(_.bytes.toList)
    commutativeMonoidLaws[MinHashSignature]
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

  def testMulti[H](mh: MinHasher[H], sets: Int, samples: Int, similarity: Double, epsilon: Double) = {
    val randomSets = randomMultiSets(sets, samples, similarity)

    val exact = exactSimilarityMulti(randomSets)
    val sim = approxSimilarityMulti(mh, randomSets)
    val error: Double = math.abs(exact - sim)
    assert(error < epsilon)
    info(s"sets: $sets, exact: $exact, sim: $sim, error: $error, epsion: $epsilon")
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

  def randomMultiSets(sets: Int, samples: Int, similarity: Double) = {
    val sharedSamples = similarity * samples
    val uniqueSamples = samples - sharedSamples

    val shared = 1.to(sharedSamples.toInt).map{ i => math.random }.toSet
    for {
      i <- 1 to sets
      unique = 1.to((uniqueSamples / sets).toInt).map{ i => math.random }.toSet
    } yield unique ++ shared
  }

  def exactSimilarity[T](x: Set[T], y: Set[T]) = {
    (x & y).size.toDouble / (x ++ y).size
  }

  def exactSimilarityMulti[T](sets: Seq[Set[T]]) = {
    sets.reduce(_ & _).size.toDouble / sets.reduce(_ | _).size.toDouble
  }

  def approxSimilarity[T, H](mh: MinHasher[H], x: Set[T], y: Set[T]) = {
    val sig1 = x.map{ l => mh.init(l.toString) }.reduce{ (a, b) => mh.plus(a, b) }
    val sig2 = y.map{ l => mh.init(l.toString) }.reduce{ (a, b) => mh.plus(a, b) }
    mh.similarity(sig1, sig2)
  }

  def approxSimilarityMulti[T, H](mh: MinHasher[H], sets: Seq[Set[T]]) = {
    val sigs = for {
      s <- sets
      sig = s.map{ l => mh.init(l.toString) }.reduce{ (a, b) => mh.plus(a, b) }
    } yield sig
    mh.similarityMulti(sigs: _*)
  }

  "MinHasher32" should {
    "measure 0.5 similarity in 1024 bytes with < 0.1 error" in {
      test(new MinHasher32(0.5, 1024), 0.5, 0.1)
    }
    "measure 0.8 similarity in 1024 bytes with < 0.1 error" in {
      test(new MinHasher32(0.8, 1024), 0.8, 0.1)
    }
    "measure 1.0 similarity in 1024 bytes with < 0.01 error" in {
      test(new MinHasher32(1.0, 1024), 1.0, 0.01)
    }
  }

  "MinHasher32 multiset similarity with sets = 2" should {
    // Repeating the above tests for multiset implementation (sets = 2)
    "measure 0.5 multiset similarity in 1024 bytes with < 0.1 error" in {
      testMulti(new MinHasher32(0.5, 1024), sets = 2, samples = 10000, similarity = 0.5, epsilon = 0.1)
    }
    "measure 0.8 multiset similarity in 1024 bytes with < 0.1 error" in {
      testMulti(new MinHasher32(0.8, 1024), sets = 2, samples = 10000, similarity = 0.8, epsilon = 0.1)
    }
    "measure 1.0 multiset similarity in 1024 bytes with < 0.01 error" in {
      testMulti(new MinHasher32(1.0, 1024), sets = 2, samples = 10000, similarity = 1.0, epsilon = 0.01)
    }
  }

  "MinHasher32 multiset similarity with sets = 10" should {
    // New tests for multiset similarity
    "measure 0.5 multiset similarity in 1024 bytes with < 0.1 error" in {
      testMulti(new MinHasher32(0.5, 1024), sets = 10, samples = 10000, similarity = 0.5, epsilon = 0.1)
    }
    "measure 0.8 multiset similarity in 1024 bytes with < 0.1 error" in {
      testMulti(new MinHasher32(0.8, 1024), sets = 10, samples = 10000, similarity = 0.8, epsilon = 0.1)
    }
    "measure 1.0 multiset similarity in 1024 bytes with < 0.01 error" in {
      testMulti(new MinHasher32(1.0, 1024), sets = 10, samples = 10000, similarity = 1.0, epsilon = 0.01)
    }
  }
}
