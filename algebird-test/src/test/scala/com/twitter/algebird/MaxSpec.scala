package com.twitter.algebird

import com.twitter.algebird.BaseProperties._
import com.twitter.algebird.scalacheck.arbitrary._
import com.twitter.algebird.scalacheck.NonEmptyVector
import org.scalacheck.Arbitrary
import org.scalacheck.Prop.forAll

class MaxSpec extends CheckProperties {
  def maxTest[T: Arbitrary: Ordering] =
    forAll { v: NonEmptyVector[Max[T]] =>
      val min = Semigroup.sumOption[Max[T]](v.items).get
      min == v.sorted.last
    } && forAll { (l: Max[T], r: Max[T]) =>
      val realMax = Max(Ordering[T].max(l.get, r.get))
      l + r == realMax && (l max r) == realMax
    }

  property("Max.{ +, max, sumOption } works on ints") { maxTest[Int] }

  property("Max should work on non-monoid types like String") {
    maxTest[String]
  }

  property("Max.aggregator returns the maximum item") {
    forAll { v: NonEmptyVector[Int] =>
      v.sorted.last == Max.aggregator[Int].apply(v.items)
    }
  }

  property("Max[String] is a commutative semigroup") {
    commutativeSemigroupLaws[Max[String]]
  }

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
