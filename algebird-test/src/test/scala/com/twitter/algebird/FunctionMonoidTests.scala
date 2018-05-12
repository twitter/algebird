package com.twitter.algebird

import com.twitter.algebird.BaseProperties._
import org.scalacheck.{Arbitrary, Prop}

class FunctionMonoidTests extends CheckProperties {
  property("Linear functions over the integers form a monoid under function composition") {
    // TODO: switch the scope of the quantification?
    Prop.forAll { (n: Int) =>
      implicit val eq: Equiv[Function1[Int, Int]] =
        Equiv.fromFunction { (f1, f2) =>
          f1(n) == f2(n)
        }

      monoidLaws[Function1[Int, Int]]
    }
  }

  implicit def arbAffine: Arbitrary[AffineFunction[Int]] =
    Arbitrary[AffineFunction[Int]] {
      for {
        a <- Arbitrary.arbInt.arbitrary
        b <- Arbitrary.arbInt.arbitrary
      } yield AffineFunction(a, b)
    }

  property("AffineFunctions are monoids") {
    monoidLaws[AffineFunction[Int]]
  }
}
