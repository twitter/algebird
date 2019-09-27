package com.twitter.algebird

import scala.collection.immutable.Queue

import com.twitter.algebird.BaseProperties._
import com.twitter.algebird.scalacheck.PosNum
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

class WindowLaws extends CheckProperties {

  implicit val mon = Window.monoid[Int](5)

  implicit def wGen[A: Arbitrary](implicit wm: WindowMonoid[A]): Arbitrary[Window[A]] =
    Arbitrary {
      for {
        as <- Gen.listOf(Arbitrary.arbitrary[A])
      } yield Window.fromIterable(as)
    }

  property("Window obeys monoid laws using a group") { monoidLaws[Window[Int]] }
  property("Window obeys monoid laws using a monoid") {
    implicit val mon = Window.monoid[String](5)
    monoidLaws[Window[String]]
  }
}

class WindowTest extends CheckProperties {
  property("We aggregate over only n items") {
    forAll { (ts0: List[Int], pn: PosNum[Int]) =>
      val n = pn.value
      val ts = ts0.takeRight(n)
      val mon = Window.monoid[Int](n)
      mon.sum(ts0.map(Window(_))).total == ts.sum
    }
  }

  property("We correctly create a window from iterable") {
    forAll { (ts0: List[Int], pn: PosNum[Int]) =>
      val n = pn.value
      val mon = Window.monoid[Int](n)
      val right = Queue(ts0.takeRight(n): _*)
      val expected = Window(right.sum, right)
      val got = mon.fromIterable(ts0)
      got == expected
    }
  }

  property("We correctly combine windows") {
    forAll { (left: List[Int], right: List[Int], pn: PosNum[Int]) =>
      val n = pn.value
      val mon = Window.monoid[Int](n)
      val trunc = Queue((left ::: right).takeRight(n): _*)
      val expected = Window(trunc.sum, trunc)
      val got = mon.plus(mon.fromIterable(left), mon.fromIterable(right))
      expected == got
    }
  }

  property("We correctly overrode sumOption") {
    forAll { (ts0: List[Int], pn: PosNum[Int]) =>
      val n = pn.value
      val mon = Window.monoid[Int](n)
      val got = mon.sumOption(ts0.map { Window(_) })
      val trunc = Queue(ts0.takeRight(n): _*)
      val expected = if (ts0.size == 0) None else Some(Window(trunc.sum, trunc))
      expected == got
    }
  }
}
