package com.twitter.algebird

import org.scalacheck.Arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalacheck.Gen.oneOf

object RightFoldedTest extends Properties("RightFoldedMonoid") {
  import BaseProperties._

  implicit def rightFoldedValue[Out:Arbitrary] : Arbitrary[RightFoldedValue[Out]] =
    Arbitrary {
     for(v <- implicitly[Arbitrary[Out]].arbitrary) yield RightFoldedValue(v)
    }
  implicit def rightFoldedToFold[In:Arbitrary] : Arbitrary[RightFoldedToFold[In]] =
    Arbitrary {
     for(v <- implicitly[Arbitrary[In]].arbitrary) yield RightFoldedToFold(List(v))
    }
  implicit def rightFolded[In:Arbitrary, Out:Arbitrary] : Arbitrary[RightFolded[In,Out]] =
     Arbitrary { oneOf(rightFoldedValue[Out].arbitrary, rightFoldedToFold[In].arbitrary) }

  implicit val rightFoldedMonoid = RightFolded.monoid[Int,Long] { (i,l) => l + i.toLong }

  property("RightFolded is a monoid") = monoidLaws[RightFolded[Int,Long]]
}
