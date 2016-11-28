package com.twitter.algebird

import com.twitter.algebird.BaseProperties._
import com.twitter.algebird.scalacheck.arbitrary._

class AveragedValueLaws extends CheckProperties {
  property("AveragedValue Group laws") {
    implicit val equiv: Equiv[AveragedValue] =
      Equiv.fromFunction { (avl, avr) =>
        ((avl.count == 0L) && (avr.count == 0L)) || {
          approxEq(1e-10)(avl.value, avr.value) && (avl.count == avr.count)
        }
      }
    groupLawsEquiv[AveragedValue]
  }
}
