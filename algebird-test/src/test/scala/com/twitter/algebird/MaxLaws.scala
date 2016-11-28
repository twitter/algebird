package com.twitter.algebird

import com.twitter.algebird.BaseProperties._
import com.twitter.algebird.scalacheck.arbitrary._
import com.twitter.algebird.scalacheck.NonEmptyVector
import org.scalacheck.Arbitrary
import org.scalacheck.Prop.forAll

class MaxLaws extends CheckProperties {
  def maxTest[T: Arbitrary: Ordering] =
    forAll { (l: Max[T], r: Max[T]) =>
      val realMax = Max(Ordering[T].max(l.get, r.get))
      l + r == realMax && (l max r) == realMax
    }

  property("Max.{ +, max } works on ints") { maxTest[Int] }

  property("Max should work on non-monoid types like String") { maxTest[String] }

  property("Max.aggregator returns the maximum item") {
    forAll { v: NonEmptyVector[Int] =>
      v.sorted.last == Max.aggregator[Int].apply(v.items)
    }
  }

  property("Max[String] is a commutative semigroup") {
    commutativeSemigroupLawsEquiv[Max[String]]
  }

  property("Max[Long] is a commutative monoid") {
    commutativeMonoidLawsEquiv[Max[Long]]
  }

  property("Max[Double] is a commutative monoid") {
    commutativeMonoidLawsEquiv[Max[Double]]
  }

  property("Max[String] is a commutative monoid") {
    commutativeMonoidLawsEquiv[Max[String]]
  }

  property("Max[List[Int]] is a commutative monoid") {
    commutativeMonoidLawsEquiv[Max[List[Int]]]
  }

  property("Max[Vector[Int]] is a commutative monoid") {
    commutativeMonoidLawsEquiv[Max[Vector[Int]]]
  }

  property("Max[Stream[Int]] is a commutative monoid") {
    commutativeMonoidLawsEquiv[Max[Stream[Int]]]
  }
}
