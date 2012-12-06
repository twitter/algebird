package com.twitter.algebird

import org.scalacheck.Arbitrary
import org.scalacheck.Gen.choose
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll

object CollectionSpecification extends Properties("Collections") with BaseProperties {

  implicit def arbMin[T:Arbitrary] : Arbitrary[Min[T]] =
    Arbitrary { implicitly[Arbitrary[T]].arbitrary.map{ x => Min(x) } }
  implicit def arbMax[T:Arbitrary] : Arbitrary[Max[T]] =
    Arbitrary { implicitly[Arbitrary[T]].arbitrary.map{ x => Max(x) } }

  property("MinSemigroup is a commutative semigroup") = commutativeSemigroupLaws[Min[Int]]
  property("MaxSemigroup is a commutative semigroup") = commutativeSemigroupLaws[Max[Int]]

  property("Either is a Semigroup") = semigroupLaws[Either[String,Int]]
  property("Either is a Semigroup, with a Right non-monoid semigroup") = semigroupLaws[Either[String,Max[Int]]]

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

  implicit def mapArb[K : Arbitrary, V : Arbitrary : Monoid] = Arbitrary {
    val mv = implicitly[Monoid[V]]
    implicitly[Arbitrary[Map[K,V]]]
      .arbitrary
      .map { _.filter { kv => mv.isNonZero(kv._2) } }
  }

  property("Map plus/times keys") = forAll { (a : Map[Int,Int], b : Map[Int,Int]) =>
    val rng = implicitly[Ring[Map[Int,Int]]]
    (rng.zero == Map[Int,Int]()) &&
    // Subsets because zeros are removed from the times/plus values
    (rng.times(a,b)).keys.toSet.subsetOf((a.keys.toSet & b.keys.toSet)) &&
      (rng.plus(a,b)).keys.toSet.subsetOf((a.keys.toSet | b.keys.toSet)) &&
      (rng.plus(a,a).keys == (a.filter { kv => (kv._2 + kv._2) != 0 }).keys)
  }
  property("Map[Int,Int] Monoid laws") = isAssociative[Map[Int,Int]] && weakZero[Map[Int,Int]]
  property("Map[Int,Int] has -") = hasAdditiveInverses[Map[Int,Int]]
  property("Map[Int,String] Monoid laws") = isAssociative[Map[Int,String]] && weakZero[Map[Int,String]]
  // We haven't implemented ring.one yet for the Map, so skip the one property
  property("Map is distributive") = isDistributive[Map[Int,Int]]
  implicit def arbIndexedSeq[T:Arbitrary] : Arbitrary[IndexedSeq[T]] =
    Arbitrary { implicitly[Arbitrary[List[T]]].arbitrary.map { _.toIndexedSeq } }

  property("IndexedSeq is a pseudoRing") = pseudoRingLaws[IndexedSeq[Int]]

  property("Either is a Monoid") = monoidLaws[Either[String,Int]]

  property("sumByKey works") = forAll { (keys : List[Int], values: List[Int]) =>
    import Operators._
    val tupList = keys.zip(values)
    tupList.sumByKey.filter { _._2 != 0 } ==
      tupList.groupBy { _._1 }
        .mapValues { v => v.map { _._2 }.sum }
        .filter { _._2 != 0 }
  }

  property("MapAlgebra.dot works") = forAll { (m1: Map[Int,Int], m2: Map[Int,Int]) =>
    // .toList below is to make sure we don't remove duplicate values
    MapAlgebra.dot(m1, m2) ==
      (m1.keySet ++ m2.keySet).toList.map { k => m1.getOrElse(k,0) * m2.getOrElse(k,0) }.sum
  }
}
