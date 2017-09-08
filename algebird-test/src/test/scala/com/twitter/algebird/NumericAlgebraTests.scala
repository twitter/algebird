package com.twitter.algebird

import org.scalatest._

import org.scalatest.prop.PropertyChecks
import org.scalacheck.{ Gen, Arbitrary, Prop, Properties }

class NumericAlgebraTests extends CheckProperties {
  import BaseProperties._
  implicit val arbitraryBigDecimalsHere = BaseProperties.arbReasonableBigDecimals

  property(s"test int") {
    // check resolution:
    implicitly[Semigroup[Int]]
    implicitly[Monoid[Int]]
    implicitly[Group[Int]]
    // now test all the laws:
    ringLaws[Int]
  }

  property(s"test bigDecimal") {
    // check resolution:
    implicitly[Semigroup[BigDecimal]]
    implicitly[Monoid[BigDecimal]]
    implicitly[Group[BigDecimal]]
    // now test all the laws:
    ringLaws[BigDecimal]
  }
}
