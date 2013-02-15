package com.twitter.algebird

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Gen.choose

object TupleRingSpecification extends Properties("TupleRing") {
  import BaseProperties._

  property("(Int,Int) is a Ring") = ringLaws[(Int,Int)]
  property("(Int,Long) is a Ring") = ringLaws[(Int,Long)]
  property("(Long,Int,Int) is a Ring") = ringLaws[(Long,Int,Int)]
  property("(Long,Int,Int,Long) is a Ring") = ringLaws[(Long,Int,Int,Long)]
}
