package com.twitter.algebird

import org.scalatest._
import com.twitter.algebird.BaseProperties._
import com.twitter.algebird.scalacheck.arbitrary._

class CorrelationLaws extends CheckProperties {
  val EPS = 1e-10

  property("Correlation semigroup laws") {
    implicit val equiv: Equiv[Correlation] =
      Equiv.fromFunction { (corr1, corr2) =>
        approxEq(EPS)(corr1.c2, corr2.c2) &&
          approxEq(EPS)(corr1.m2Left, corr2.m2Left) &&
          approxEq(EPS)(corr1.m2Right, corr2.m2Right) &&
          approxEq(EPS)(corr1.m1Left, corr2.m1Left) &&
          approxEq(EPS)(corr1.m1Right, corr2.m1Right) &&
          (corr1.m0 == corr2.m0)
      }
    semigroupLawsEquiv[Correlation]
  }
}

