package com.twitter.algebird

import com.twitter.algebird.BaseProperties._
import com.twitter.algebird.scalacheck.arbitrary._
import com.twitter.algebird.scalacheck.NonEmptyVector
import org.scalacheck.Prop.forAll

class LastSpec extends CheckProperties {
  property("Last should work properly") {
    forAll { v: NonEmptyVector[Last[Int]] =>
      val last = Semigroup.sumOption[Last[Int]](v.items).get
      last == v.items.last
    }
  }

  property("Last.aggregator returns the first item") {
    forAll { v: NonEmptyVector[Int] =>
      v.items.last == Last.aggregator(v.items)
    }
  }

  property("Last[Int] is a Semigroup") { semigroupLaws[Last[Int]] }
  property("Last[String] is a Semigroup") { semigroupLaws[Last[String]] }
}
