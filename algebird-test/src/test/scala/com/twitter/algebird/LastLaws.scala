package com.twitter.algebird

import com.twitter.algebird.BaseProperties._
import com.twitter.algebird.scalacheck.arbitrary._
import com.twitter.algebird.scalacheck.NonEmptyVector
import org.scalacheck.Prop.forAll

class LastLaws extends CheckProperties {
  property("Last should sum properly") {
    forAll { v: NonEmptyVector[Last[Int]] =>
      val last = Semigroup.sumOption[Last[Int]](v.items).get
      last == v.items.last
    }
  }

  property("Last.+ should work") {
    forAll { (l: Last[Int], r: Last[Int]) => l + r == r }
  }

  property("Last.aggregator returns the last item") {
    forAll { v: NonEmptyVector[Int] =>
      v.items.last == Last.aggregator(v.items)
    }
  }

  property("Last[Int] is a Semigroup") { semigroupLaws[Last[Int]] }
}
