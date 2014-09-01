package com.twitter.algebird

import org.scalatest.{ DiagrammedAssertions, PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks
import org.scalacheck.{ Gen, Arbitrary }

class RightFoldedTest extends PropSpec with PropertyChecks with Matchers with DiagrammedAssertions {
  import BaseProperties._

  implicit def rightFoldedValue[Out: Arbitrary]: Arbitrary[RightFoldedValue[Out]] =
    Arbitrary {
      for (v <- implicitly[Arbitrary[Out]].arbitrary) yield RightFoldedValue(v)
    }
  implicit def rightFoldedToFold[In: Arbitrary]: Arbitrary[RightFoldedToFold[In]] =
    Arbitrary {
      for (v <- implicitly[Arbitrary[In]].arbitrary) yield RightFoldedToFold(List(v))
    }
  implicit def rightFolded[In: Arbitrary, Out: Arbitrary]: Arbitrary[RightFolded[In, Out]] =
    Arbitrary { Gen.oneOf(rightFoldedValue[Out].arbitrary, rightFoldedToFold[In].arbitrary) }

  implicit val rightFoldedMonoid = RightFolded.monoid[Int, Long] { (i, l) => l + i.toLong }

  property("RightFolded is a monoid") {
    monoidLaws[RightFolded[Int, Long]]
  }

}
