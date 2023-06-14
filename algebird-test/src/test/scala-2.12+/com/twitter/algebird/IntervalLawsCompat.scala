package com.twitter.algebird

import org.scalacheck.Prop._

trait IntervalLawsCompat { self: CheckProperties =>
  import com.twitter.algebird.scalacheck.arbitrary._
  property("if boundedLeast is none, we are Universe, Upper or isEmpty is true") {
    forAll { (a: Interval[Long]) =>
      a.boundedLeast match {
        case Some(_) => true
        case None =>
          a.isEmpty || (a match {
            case _: Upper[?] => true
            case Universe()  => true
            case _           => false
          })
      }
    }
  }
  property("if boundedGreatest is none, we are Universe, Lower or isEmpty is true") {
    forAll { (a: Interval[Long]) =>
      a.boundedGreatest match {
        case Some(_) => true
        case None =>
          a.isEmpty || (a match {
            case _: Lower[?] => true
            case Universe()  => true
            case _           => false
          })
      }
    }
  }

}
