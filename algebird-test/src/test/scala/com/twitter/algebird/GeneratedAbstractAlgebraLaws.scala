package com.twitter.algebird

import com.twitter.algebird.BaseProperties._

class GeneratedAbstractAlgebraLaws extends CheckProperties {
  property("tuple2 is a ring") {
    type T = (Int, Int)
    ringLaws[T] && isCommutative[T]
  }
  property("tuple3 is a ring") {
    type T = (Int, Int, Int)
    ringLaws[T] && isCommutative[T]
  }
  property("tuple4 is a ring") {
    type T = (Int, Int, Int, Int)
    ringLaws[T] && isCommutative[T]
  }
  property("tuple5 is a ring") {
    type T = (Int, Int, Int, Int, Int)
    ringLaws[T] && isCommutative[T]
  }
  property("tuple6 is a ring") {
    type T = (Int, Int, Int, Int, Int, Int)
    ringLaws[T] && isCommutative[T]
  }
  property("tuple7 is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int)
    ringLaws[T] && isCommutative[T]
  }
  property("tuple8 is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int, Int)
    ringLaws[T] && isCommutative[T]
  }
  property("tuple9 is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int, Int, Int)
    ringLaws[T] && isCommutative[T]
  }
  property("tuple10 is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    ringLaws[T] && isCommutative[T]
  }
  property("tuple11 is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    ringLaws[T] && isCommutative[T]
  }
  property("tuple12 is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    ringLaws[T] && isCommutative[T]
  }
  property("tuple13 is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    ringLaws[T] && isCommutative[T]
  }
  property("tuple14 is a ring") {
    type T =
      (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    ringLaws[T] && isCommutative[T]
  }
  property("tuple15 is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    ringLaws[T] && isCommutative[T]
  }
  property("tuple16 is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    ringLaws[T] && isCommutative[T]
  }
  property("tuple17 is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    ringLaws[T] && isCommutative[T]
  }
  property("tuple18 is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    ringLaws[T] && isCommutative[T]
  }
  property("tuple19 is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    ringLaws[T] && isCommutative[T]
  }
  property("tuple20 is a ring") {
    type T =
      (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    ringLaws[T] && isCommutative[T]
  }
  property("tuple21 is a ring") {
    type T = (
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int
    )
    ringLaws[T] && isCommutative[T]
  }
  property("tuple22 is a ring") {
    type T = (
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int
    )
    ringLaws[T] && isCommutative[T]
  }
}
