package com.twitter.algebird

import com.twitter.algebird.BaseProperties._
import com.twitter.algebird.scalacheck.arbitrary._
import com.twitter.algebird.scalacheck.NonEmptyVector
import org.scalacheck.Prop.forAll

class MaxSpec extends CheckProperties {
  property("Max should sum properly") {
    forAll { v: NonEmptyVector[Max[Int]] =>
      val max = Semigroup.sumOption[Max[Int]](v.items).get
      max == v.sorted.last
    }
  }

  property("Max.{ +, max } should work") {
    forAll { (l: Max[Int], r: Max[Int]) =>
      val realMax = Max(l.get max r.get)
      l + r == realMax && (l max r) == realMax
    }
  }

  property("Max.aggregator returns the maximum item") {
    forAll { v: NonEmptyVector[Int] =>
      v.sorted.last == Max.aggregator[Int].apply(v.items)
    }
  }

  property("Max[Int] is a commutative semigroup") {
    commutativeSemigroupLaws[Max[Int]]
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
}
