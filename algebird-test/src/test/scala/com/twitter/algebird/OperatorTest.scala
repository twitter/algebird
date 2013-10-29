package com.twitter.algebird

import org.specs2.mutable._

import Operators._

class OperatorTest extends Specification {


  "Operators" should {
     "allow plus" in {
       List(1,2) + List(3,4) must be_==(List(1,2,3,4))
     }
     "allow -" in {
       (1,3) - (2,4) must be_==((-1,-1))
     }
     "allow *" in {
       Map(1 -> 3) * Map(2 -> 4) must be_==(Map[Int,Int]())
       Map(1 -> 3) * Map(1 -> 4) must be_==(Map(1 -> 12))
     }
     "allow /" in {
       true / true must be_==(true)
     }
  }
}
