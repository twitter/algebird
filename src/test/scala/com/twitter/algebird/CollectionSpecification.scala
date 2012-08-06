package com.twitter.algebird

import org.scalacheck.Arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll

object CollectionSpecification extends Properties("Collections") with BaseProperties {

  property("Option Monoid laws") = monoidLaws[Option[Int]] && monoidLaws[Option[String]]

  property("List plus") = forAll { (a : List[Int], b : List[Int]) =>
    val mon = implicitly[Monoid[List[Int]]]
    (a ++ b == mon.plus(a,b)) && (mon.zero == List[Int]())
  }

  property("List Monoid laws") = monoidLaws[List[Int]]

  property("Set plus") = forAll { (a : Set[Int], b : Set[Int]) =>
    val mon = implicitly[Monoid[Set[Int]]]
    (a ++ b == mon.plus(a,b)) && (mon.zero == Set[Int]())
  }
  property("Set Monoid laws") = monoidLaws[Set[Int]]

  property("Map plus/times keys") = forAll { (a : Map[Int,Int], b : Map[Int,Int]) =>
    val rng = implicitly[Ring[Map[Int,Int]]]
    (rng.zero == Map[Int,Int]()) &&
    // Subsets because zeros are removed from the times/plus values
    (rng.times(a,b)).keys.toSet.subsetOf((a.keys.toSet & b.keys.toSet)) &&
      (rng.plus(a,b)).keys.toSet.subsetOf((a.keys.toSet | b.keys.toSet)) &&
      (rng.plus(a,a).keys == (a.filter { kv => (kv._2 + kv._2) != 0 }).keys)
  }
  property("Map Monoid laws") = isAssociative[Map[Int,Int]] && weakZero[Map[Int,Int]]
  // We haven't implemented one yet for the Map, so skip the one property
  property("Map is distributive") = isDistributive[Map[Int,Int]]

  // Either is actually a semigroup, but we don't yet have a typeclass for that:
  property("Either is semigroup") = isAssociative[Either[String,Int]]
}
