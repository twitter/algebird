package com.twitter.algebird

import com.twitter.algebird.BaseProperties._

class GeneratedProductAlgebraLaws extends CheckProperties {
  property("Product2Ring is a ring") {
    type T = (Int, Int)
    implicit val ring: Ring[T] = Ring[T, Int, Int](Tuple2.apply, Tuple2.unapply)
    ringLaws[T] && isCommutative[T]
  }
  property("Product3Ring is a ring") {
    type T = (Int, Int, Int)
    implicit val ring: Ring[T] = Ring[T, Int, Int, Int](Tuple3.apply, Tuple3.unapply)
    ringLaws[T] && isCommutative[T]
  }
  property("Product4Ring is a ring") {
    type T = (Int, Int, Int, Int)
    implicit val ring: Ring[T] =
      Ring[T, Int, Int, Int, Int](Tuple4.apply, Tuple4.unapply)
    ringLaws[T] && isCommutative[T]
  }
  property("Product5Ring is a ring") {
    type T = (Int, Int, Int, Int, Int)
    implicit val ring: Ring[T] =
      Ring[T, Int, Int, Int, Int, Int](Tuple5.apply, Tuple5.unapply)
    ringLaws[T] && isCommutative[T]
  }
  property("Product6Ring is a ring") {
    type T = (Int, Int, Int, Int, Int, Int)
    implicit val ring: Ring[T] =
      Ring[T, Int, Int, Int, Int, Int, Int](Tuple6.apply, Tuple6.unapply)
    ringLaws[T] && isCommutative[T]
  }
  property("Product7Ring is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int)
    implicit val ring: Ring[T] =
      Ring[T, Int, Int, Int, Int, Int, Int, Int](Tuple7.apply, Tuple7.unapply)
    ringLaws[T] && isCommutative[T]
  }
  property("Product8Ring is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int, Int)
    implicit val ring: Ring[T] =
      Ring[T, Int, Int, Int, Int, Int, Int, Int, Int](Tuple8.apply, Tuple8.unapply)
    ringLaws[T] && isCommutative[T]
  }
  property("Product9Ring is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int, Int, Int)
    implicit val ring: Ring[T] =
      Ring[T, Int, Int, Int, Int, Int, Int, Int, Int, Int](Tuple9.apply, Tuple9.unapply)
    ringLaws[T] && isCommutative[T]
  }
  property("Product10Ring is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    implicit val ring: Ring[T] =
      Ring[T, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int](Tuple10.apply, Tuple10.unapply)
    ringLaws[T] && isCommutative[T]
  }
  property("Product11Ring is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    implicit val ring: Ring[T] =
      Ring[T, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int](Tuple11.apply, Tuple11.unapply)
    ringLaws[T] && isCommutative[T]
  }
  property("Product12Ring is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    implicit val ring: Ring[T] =
      Ring[T, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int](Tuple12.apply, Tuple12.unapply)
    ringLaws[T] && isCommutative[T]
  }
  property("Product13Ring is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    implicit val ring: Ring[T] =
      Ring[T, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int](Tuple13.apply, Tuple13.unapply)
    ringLaws[T] && isCommutative[T]
  }
  property("Product14Ring is a ring") {
    type T =
      (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    implicit val ring: Ring[T] =
      Ring[T, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int](
        Tuple14.apply,
        Tuple14.unapply
      )
    ringLaws[T] && isCommutative[T]
  }
  property("Product15Ring is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    implicit val ring: Ring[T] =
      Ring[T, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int](
        Tuple15.apply,
        Tuple15.unapply
      )
    ringLaws[T] && isCommutative[T]
  }
  property("Product16Ring is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    implicit val ring: Ring[T] =
      Ring[T, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int](
        Tuple16.apply,
        Tuple16.unapply
      )
    ringLaws[T] && isCommutative[T]
  }
  property("Product17Ring is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    implicit val ring: Ring[T] =
      Ring[T, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int](
        Tuple17.apply,
        Tuple17.unapply
      )
    ringLaws[T] && isCommutative[T]
  }
  property("Product18Ring is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    implicit val ring: Ring[T] =
      Ring[T, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int](
        Tuple18.apply,
        Tuple18.unapply
      )
    ringLaws[T] && isCommutative[T]
  }
  property("Product19Ring is a ring") {
    type T = (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    implicit val ring: Ring[T] =
      Ring[T, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int](
        Tuple19.apply,
        Tuple19.unapply
      )
    ringLaws[T] && isCommutative[T]
  }
  property("Product20Ring is a ring") {
    type T =
      (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    implicit val ring: Ring[T] = Ring[
      T,
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
    ](Tuple20.apply, Tuple20.unapply)
    ringLaws[T] && isCommutative[T]
  }
  property("Product21Ring is a ring") {
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
    implicit val ring: Ring[T] = Ring[
      T,
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
    ](Tuple21.apply, Tuple21.unapply)
    ringLaws[T] && isCommutative[T]
  }
  property("Product22Ring is a ring") {
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
    implicit val ring: Ring[T] = Ring[
      T,
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
    ](Tuple22.apply, Tuple22.unapply)
    ringLaws[T] && isCommutative[T]
  }
}
