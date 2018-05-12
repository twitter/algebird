package com.twitter.algebird.generic

import com.twitter.algebird.{Group, Monoid, Ring, Semigroup}
import org.scalacheck.ScalacheckShapeless._
import Shapeless._
import shapeless._
import EquivOrdering._

import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import com.twitter.algebird.BaseProperties.{groupLaws, monoidLaws, ringLaws, semigroupLaws}

case class Row(x: Int, y: Long)

object ShapelessTest extends Properties("Shapeless Instances") {

  property("semigroup laws") = semigroupLaws[Int :: String :: HNil]

  property("monoid laws") = monoidLaws[Int :: String :: HNil]

  property("group laws") = groupLaws[Int :: Long :: HNil]

  property("ring laws") = ringLaws[Int :: Long :: HNil]

  implicit val rowRing: Ring[Row] = genericRing
  implicit val rowOrd: Ordering[Row] = genericOrdering
  property("ring laws on row") = ringLaws[Row]

  // check we can find generics
  genericSemigroup[Row, Int :: Long :: HNil]
  genericMonoid[Row, Int :: Long :: HNil]
  genericGroup[Row, Int :: Long :: HNil]
  genericRing[Row, Int :: Long :: HNil]

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

  property("HList.sum(1) works (1)") = forAll { (a: Int :: HNil) =>
    HListSum.sum(a) == a.head
    HListSum.sum1(a) == a.head
  }
  property("HList.sum(1) works (2)") = forAll { (a: Int :: Int :: HNil) =>
    HListSum.sum(a) == (a.head + a.tail.head)
    HListSum.sum1(a) == (a.head + a.tail.head)
  }
}
