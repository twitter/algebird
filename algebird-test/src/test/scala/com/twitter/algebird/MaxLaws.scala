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

  def maxSemiGroupTest[T: Arbitrary: Ordering] =
    forAll { v: NonEmptyVector[T] =>
      val maxItems = v.items.map { Max(_) }
      v.items.max == Max.semigroup[T].combineAllOption(maxItems).get.get
    }

  // Test equiv import.
  val equiv = implicitly[Equiv[Max[Int]]]

  // Testing that these ones can be found
  val sgInt = implicitly[Semigroup[Max[Int]]]
  val sgString = implicitly[Semigroup[Max[String]]]
  val monoidString = implicitly[Monoid[Max[String]]]

  property("Max.{ +, max } works on ints") { maxTest[Int] }

  property("Max.aggregator returns the maximum item") {
    forAll { v: NonEmptyVector[Int] =>
      v.items.max == Max.aggregator[Int].apply(v.items)
    }
  }

  property("Max.semigroup[Int] returns the maximum item") { maxSemiGroupTest[Int] }

  property("Max.semigroup[Char] returns the maximum item") { maxSemiGroupTest[Char] }

  property("Max[Long] is a commutative monoid") {
    commutativeMonoidLaws[Max[Long]]
  }

  property("Max[Double] is a commutative monoid") {
    commutativeMonoidLaws[Max[Double]]
  }

  property("Max[String] is a commutative monoid") {
    commutativeMonoidLaws[Max[String]]
  }

  property("Max[List[Int]] is a commutative monoid") {
    commutativeMonoidLaws[Max[List[Int]]]
  }

  property("Max[Vector[Int]] is a commutative monoid") {
    commutativeMonoidLaws[Max[Vector[Int]]]
  }

  property("Max[Stream[Int]] is a commutative monoid") {
    commutativeMonoidLaws[Max[Stream[Int]]]
  }
}
