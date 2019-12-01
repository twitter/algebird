package com.twitter.algebird

import org.scalatest._

import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalacheck.{Arbitrary, Gen}
import Arbitrary.arbitrary
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

object Helpers {
  implicit def arbitraryBatched[A: Arbitrary]: Arbitrary[Batched[A]] = {
    val item = arbitrary[A].map(Batched(_))
    val items = arbitrary[(A, List[A])].map {
      case (a, as) =>
        Batched(a).append(as)
    }
    Arbitrary(Gen.oneOf(item, items))
  }
}

import Helpers.arbitraryBatched

class BatchedLaws extends CheckProperties {

  import BaseProperties._
  implicit val arbitraryBigDecimalsHere =
    BaseProperties.arbReasonableBigDecimals

  def testBatchedMonoid[A: Arbitrary: Monoid](name: String, size: Int): Unit = {
    implicit val m: Monoid[Batched[A]] = Batched.compactingMonoid[A](size)
    property(s"CountMinSketch[$name] batched at $size is a Monoid") {
      monoidLaws[Batched[A]]
    }
  }

  testBatchedMonoid[Int]("Int", 1)
  testBatchedMonoid[Int]("Int", 10)
  testBatchedMonoid[Int]("Int", 100)
  testBatchedMonoid[Int]("Int", 1000000)
  testBatchedMonoid[BigInt]("BigInt", 1)
  testBatchedMonoid[BigInt]("BigInt", 10)
  testBatchedMonoid[BigInt]("BigInt", 100)
  testBatchedMonoid[BigInt]("BigInt", 1000000)
  testBatchedMonoid[BigDecimal]("BigDecimal", 1)
  testBatchedMonoid[BigDecimal]("BigDecimal", 10)
  testBatchedMonoid[BigDecimal]("BigDecimal", 100)
  testBatchedMonoid[BigDecimal]("BigDecimal", 1000000)
  testBatchedMonoid[String]("String", 1)
  testBatchedMonoid[String]("String", 10)
  testBatchedMonoid[String]("String", 100)
  testBatchedMonoid[String]("String", 1000000)
}

class BatchedTests extends AnyPropSpec with Matchers with ScalaCheckPropertyChecks {
  property(".iterator works") {
    forAll { (x: Int, xs: List[Int]) =>
      Batched(x).append(xs).iterator.toList shouldBe (x :: xs)
    }
  }

  property(".iterator and .reverseIterator agree") {
    forAll { (b: Batched[Int]) =>
      b.iterator.toList.reverse shouldBe b.reverseIterator.toList
      b.iterator.sum shouldBe b.reverseIterator.sum
    }
  }

  property(".toList works") {
    forAll { (b: Batched[Int]) =>
      b.toList shouldBe b.iterator.toList
    }
  }
}
