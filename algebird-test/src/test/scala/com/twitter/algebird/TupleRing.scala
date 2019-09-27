package com.twitter.algebird

import org.scalatest.{Matchers, PropSpec}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalacheck.Arbitrary

class TupleRingSpecification extends CheckProperties {
  import BaseProperties._

  property("(Int,Int) is a Ring") {
    ringLaws[(Int, Int)]
  }

  property("(Int,Long) is a Ring") {
    ringLaws[(Int, Long)]
  }

  property("(Long,Int,Int) is a Ring") {
    ringLaws[(Long, Int, Int)]
  }

  property("(Long,Int,Int,Long) is a Ring") {
    ringLaws[(Long, Int, Int, Long)]
  }
}
