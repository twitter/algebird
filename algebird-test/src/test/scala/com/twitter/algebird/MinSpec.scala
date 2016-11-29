package com.twitter.algebird

import com.twitter.algebird.BaseProperties._
import com.twitter.algebird.scalacheck.arbitrary._
import com.twitter.algebird.scalacheck.NonEmptyVector
import org.scalacheck.Prop.forAll

class MinSpec extends CheckProperties {
  property("Min should sum properly") {
    forAll { v: NonEmptyVector[Min[Int]] =>
      val min = Semigroup.sumOption[Min[Int]](v.items).get
      min == v.sorted.head
    }
  }

  property("Min.{ +, min } should work") {
    forAll { (l: Min[Int], r: Min[Int]) =>
      val realMin = Min(l.get min r.get)
      l + r == realMin && (l min r) == realMin
    }
  }

  property("Min.aggregator returns the minimum item") {
    forAll { v: NonEmptyVector[Int] =>
      v.sorted.head == Min.aggregator[Int].apply(v.items)
    }
  }

  property("Min[Int] is a commutative semigroup") {
    commutativeSemigroupLaws[Min[Int]]
  }

  property("Min[Long] is a commutative monoid") {
    commutativeMonoidLaws[Min[Int]]
  }

  property("Min[Double] is a commutative monoid") {
    commutativeMonoidLaws[Min[Int]]
  }
}
