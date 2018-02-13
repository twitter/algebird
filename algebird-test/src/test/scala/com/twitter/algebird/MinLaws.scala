package com.twitter.algebird

import com.twitter.algebird.BaseProperties._
import com.twitter.algebird.scalacheck.arbitrary._
import com.twitter.algebird.scalacheck.NonEmptyVector
import org.scalacheck.Arbitrary
import org.scalacheck.Prop.forAll

class MinLaws extends CheckProperties {
  def minTest[T: Arbitrary: Ordering] =
    forAll { (l: Min[T], r: Min[T]) =>
      val realMin = Min(Ordering[T].min(l.get, r.get))
      l + r == realMin && (l min r) == realMin
    }

  // Test equiv import.
  val equiv = implicitly[Equiv[Min[Int]]]

  property("Min.{ +, min } works on ints") { minTest[Int] }

  property("Min should work on non-monoid types like String") { minTest[String] }

  property("Min.aggregator returns the minimum item") {
    forAll { v: NonEmptyVector[Int] =>
      v.sorted.head == Min.aggregator[Int].apply(v.items)
    }
  }

  property("Min.semigroup returns the minimum item") {
    forAll { v: NonEmptyVector[Int] =>
      val minItems = v.items.map { Min(_) }
      v.sorted.head == Min.semigroup[Int].combineAllOption(minItems).get.get
    }
  }

  property("Min[String] is a commutative semigroup") {
    commutativeSemigroupLaws[Min[String]]
  }

  property("Min[Long] is a commutative monoid") {
    commutativeMonoidLaws[Min[Long]]
  }

  property("Min[Double] is a commutative monoid") {
    commutativeMonoidLaws[Min[Double]]
  }
}
