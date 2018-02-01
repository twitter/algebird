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
      assert(mon.sum(ts0.map(Window(_))).total == ts.sum)
    }
  }

  property("We correctly create a window from traversable") {
    forAll { (ts0: List[Int], n: Int) =>
      val mon = new WindowMonoid(n)
      val right = Queue(ts0.takeRight(n): _*)
      val expected = Window(right.sum, right)
      val got = mon.fromTraversable(ts0)
      assert(got == expected)
    }
  }

  property("We correctly combine windows") {
    forAll { (left: List[Int], right: List[Int], n: Int) =>
      val mon = new WindowMonoid(n)
      val trunc = Queue((left ::: right).takeRight(n): _*)
      val expected = Window(trunc.sum, trunc)
      val got = mon.sum(mon.fromTraversable(left), mon.fromTraversable(right))
      assert(expected == got)
    }
  }

  property("We correctly overrode sumOption") {
    forAll { (ts0: List[Int], n: Int) =>
      val mon = new WindowMonoid(n)
      val got = mon.sumOption(ts0.map { Window(_) })
      val trunc = Queue(ts0.takeRight(n): _*)
      val expected = if (n == 0) None else Some(Window(trunc.sum, trunc))
      assert(expected == got)
    }
  }

  property("Window[Double] is a monoid") { monoidLaws[Window[Double]] }
}
