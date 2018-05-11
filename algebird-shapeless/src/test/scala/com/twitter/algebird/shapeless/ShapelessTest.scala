package com.twitter.algebird.shapeless

import com.twitter.algebird.{ Semigroup, Monoid, Group, Ring }
import org.scalacheck.ScalacheckShapeless._
import Shapeless._
import shapeless._
import EquivOrdering._

import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import com.twitter.algebird.BaseProperties.{ semigroupLaws, monoidLaws, groupLaws, ringLaws }

case class Row(x: Int, y: Long)

object ShapelessTest extends Properties("Shapeless Instances") {
  property("semigroup laws") =
    semigroupLaws[Int :: String :: HNil]

  property("monoid laws") =
    monoidLaws[Int :: String :: HNil]

  property("group laws") =
    groupLaws[Int :: Long :: HNil]

  property("ring laws") =
    ringLaws[Int :: Long :: HNil]

  property("ring laws on row") =
    ringLaws[Row]

  property("Semigroup matches tuple") = forAll { (a: Int :: String :: HNil, b: Int :: String :: HNil) =>
    Semigroup.plus(a, b) == ((a.head + b.head) :: (a.tail.head + b.tail.head) :: HNil)
  }

  property("Monoid matches tuple") = forAll { (a: Int :: String :: HNil, b: Int :: String :: HNil) =>
    Monoid.plus(a, b) == ((a.head + b.head) :: (a.tail.head + b.tail.head) :: HNil)
    Monoid.zero[Int :: String :: HNil] == (0 :: "" :: HNil)
  }

  property("Group matches tuple") = forAll { (a: Int :: Long :: HNil, b: Int :: Long :: HNil) =>
    Group.minus(a, b) == ((a.head - b.head) :: (a.tail.head - b.tail.head) :: HNil)
  }

  property("Ring matches tuple") = forAll { (a: Int :: Long :: HNil, b: Int :: Long :: HNil) =>
    Ring.times(a, b) == ((a.head * b.head) :: (a.tail.head * b.tail.head) :: HNil)
  }
}
