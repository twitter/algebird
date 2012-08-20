package com.twitter.algebird

import org.scalacheck.Arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll

object FunctionMonoidTests extends Properties("FunctionMonoid") with BaseProperties {

  // Generates an arbitrary linear function of the form f(x) = a * x + b,
  // where a and b are arbitrary integers.
  // TODO: add more types of functions (e.g., polynomials of degree two).
  implicit def arbLinearFunc1[Int] = Arbitrary { 
    for (
      a <- Arbitrary.arbInt.arbitrary;
      b <- Arbitrary.arbInt.arbitrary
    ) yield (new Function1[Int, Int] { def apply(x : Int): Int = x  })
  }
  
  property("Linear functions over the integers form a monoid under function composition") = 
    forAll { (n: Int) =>
      monoidLawsEq[Function1[Int, Int]] { (f1, f2) => f1(n) == f2(n) }
    }
}
