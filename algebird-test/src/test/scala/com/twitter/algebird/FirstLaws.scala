package com.twitter.algebird

import com.twitter.algebird.BaseProperties._
import com.twitter.algebird.scalacheck.arbitrary._
import com.twitter.algebird.scalacheck.NonEmptyVector
import org.scalacheck.Prop.forAll

class FirstLaws extends CheckProperties {
  property("First should sum properly") {
    forAll { (v: NonEmptyVector[First[Int]]) =>
      val first = Semigroup.sumOption[First[Int]](v.items).get
      first == v.items.head
    }
  }

  property("First.+ should work") {
    forAll((l: First[Int], r: First[Int]) => l + r == l)
  }

  property("First.aggregator returns the first item") {
    forAll { (v: NonEmptyVector[Int]) => v.items.head == First.aggregator(v.items) }
  }

  property("First[Int] is a semigroup")(semigroupLaws[First[Int]])
}
