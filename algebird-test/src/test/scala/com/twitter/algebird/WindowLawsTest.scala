package com.twitter.algebird

import com.twitter.algebird.BaseProperties._
import com.twitter.algebird.scalacheck.arbitrary._
import com.twitter.algebird.scalacheck.NonEmptyVector
import org.scalacheck.Arbitrary
import org.scalacheck.Prop.forAll


class WindowLaws extends CheckProperties {
  property("We aggregate over only n items") {
    forAll { (ts0: List[Int], n: Int) =>
      val ts = ts0.takeRight(n)
      val mon = new WindowMonoid(n)
      assert(mon.sum(ts0.map( Window(_) )).total == ts.sum)
    }
  }

  property("Window[Double] is a monoid") { monoidLaws[Window[Double]] }
}
