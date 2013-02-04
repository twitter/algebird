package com.twitter.algebird

import org.scalacheck.Arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll

object FunctionMonoidTests extends Properties("FunctionMonoid") {
  import BaseProperties._

  // Generates an arbitrary linear function of the form f(x) = a * x + b,
  // where a and b are arbitrary integers.
  // TODO: add more types of functions (e.g., polynomials of degree two).
  implicit def arbLinearFunc1: Arbitrary[Function1[Int, Int]] = Arbitrary[Function[Int, Int]] {
    for (
      a <- Arbitrary.arbInt.arbitrary;
      b <- Arbitrary.arbInt.arbitrary
    ) yield ((x : Int) => a * x + b )
  }

  property("Linear functions over the integers form a monoid under function composition") =
    // TODO: switch the scope of the quantification?
    forAll { (n: Int) =>
      monoidLawsEq[Function1[Int, Int]] { (f1, f2) => f1(n) == f2(n) }
    }

  implicit def arbAffine: Arbitrary[AffineFunction[Int]] = Arbitrary[AffineFunction[Int]] {
    for (
      a <- Arbitrary.arbInt.arbitrary;
      b <- Arbitrary.arbInt.arbitrary
    ) yield AffineFunction(a,b)
  }
  property("AffineFunctions are monoids") = monoidLaws[AffineFunction[Int]]
}
