package com.twitter.algebird

import org.scalacheck.Arbitrary
import org.scalacheck.Gen.choose
import org.scalacheck.Properties
import org.scalacheck.Prop._

import scala.collection.{Map => ScMap}

object CollectionSpecification extends Properties("Collections") {
  import BaseProperties._

  implicit def arbMin[T:Arbitrary] : Arbitrary[Min[T]] =
    Arbitrary { implicitly[Arbitrary[T]].arbitrary.map{ x => Min(x) } }
  implicit def arbMax[T:Arbitrary] : Arbitrary[Max[T]] =
    Arbitrary { implicitly[Arbitrary[T]].arbitrary.map{ x => Max(x) } }
  implicit def arbOrVal: Arbitrary[OrVal] =
    Arbitrary { implicitly[Arbitrary[Boolean]].arbitrary.map{ b => OrVal(b) } }
  implicit def arbAndVal: Arbitrary[AndVal] =
    Arbitrary { implicitly[Arbitrary[Boolean]].arbitrary.map{ b => AndVal(b) } }

  property("MinSemigroup is a commutative semigroup") = commutativeSemigroupLaws[Min[Int]]
  property("MaxSemigroup is a commutative semigroup") = commutativeSemigroupLaws[Max[Int]]
  property("OrValMonoid is a commutative monoid") = commutativeMonoidLaws[OrVal]
  property("AndValMonoid is a commutative monoid") = commutativeMonoidLaws[AndVal]
  property("Min[Int] is a monoid") = monoidLaws[Min[Int]]
  property("Max[String] is a monoid") = monoidLaws[Max[String]]
  property("Max[List[Int]] is a monoid") = monoidLaws[Max[List[Int]]]

  property("Either is a Semigroup") = semigroupLaws[Either[String,Int]]
  property("Either is a Semigroup, with a Right non-monoid semigroup") = semigroupLaws[Either[String,Max[Int]]]

  property("Option Monoid laws") = monoidLaws[Option[Int]] && monoidLaws[Option[String]]

  property("List plus") = forAll { (a : List[Int], b : List[Int]) =>
    val mon = implicitly[Monoid[List[Int]]]
    (a ++ b == mon.plus(a,b)) && (mon.zero == List[Int]())
  }

  property("List Monoid laws") = monoidLaws[List[Int]]

  implicit def arbSeq[T:Arbitrary] : Arbitrary[Seq[T]] =
    Arbitrary { implicitly[Arbitrary[List[T]]].arbitrary.map { _.toSeq } }

  property("Seq plus") = forAll { (a : Seq[Int], b : Seq[Int]) =>
    val mon = implicitly[Monoid[Seq[Int]]]
    (a ++ b == mon.plus(a,b)) && (mon.zero == Seq[Int]())
  }

  property("Seq Monoid laws") = monoidLaws[Seq[Int]]

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

  implicit def scMapArb[K : Arbitrary, V : Arbitrary : Monoid] = Arbitrary {
    mapArb[K, V]
      .arbitrary
      .map { map: Map[K,V] => map: ScMap[K,V] }
  }

  def mapPlusTimesKeys[M <: ScMap[Int, Int]]
      (implicit rng: Ring[M], arbMap: Arbitrary[M]) =
    forAll { (a: M, b: M) =>
      // Subsets because zeros are removed from the times/plus values
      (rng.times(a,b)).keys.toSet.subsetOf((a.keys.toSet & b.keys.toSet)) &&
        (rng.plus(a,b)).keys.toSet.subsetOf((a.keys.toSet | b.keys.toSet)) &&
        (rng.plus(a,a).keys == (a.filter { kv => (kv._2 + kv._2) != 0 }).keys)
    }

  property("Map plus/times keys") = mapPlusTimesKeys[Map[Int, Int]]
  property("ScMap plus/times keys") = mapPlusTimesKeys[ScMap[Int, Int]]
  property("Map[Int,Int] Monoid laws") = isAssociative[Map[Int,Int]] && weakZero[Map[Int,Int]]
  property("ScMap[Int,Int] Monoid laws") = isAssociative[ScMap[Int,Int]] && weakZero[ScMap[Int,Int]]
  property("Map[Int,Int] has -") = hasAdditiveInverses[Map[Int,Int]]
  property("ScMap[Int,Int] has -") = hasAdditiveInverses[ScMap[Int,Int]]
  property("Map[Int,String] Monoid laws") = isAssociative[Map[Int,String]] && weakZero[Map[Int,String]]
  property("ScMap[Int,String] Monoid laws") = isAssociative[ScMap[Int,String]] && weakZero[ScMap[Int,String]]
  // We haven't implemented ring.one yet for the Map, so skip the one property
  property("Map is distributive") = isDistributive[Map[Int,Int]]
  property("ScMap is distributive") = isDistributive[ScMap[Int,Int]]
  implicit def arbIndexedSeq[T:Arbitrary] : Arbitrary[IndexedSeq[T]] =
    Arbitrary { implicitly[Arbitrary[List[T]]].arbitrary.map { _.toIndexedSeq } }

  property("IndexedSeq (of a Semigroup) is a semigroup") = semigroupLaws[IndexedSeq[Max[Int]]]
  // TODO: this test fails sometimes due to the equiv not doing the right thing.
  // Fix by defining and Equiv and having all the properties use an implicit Equiv
  property("IndexedSeq is a pseudoRing") = pseudoRingLaws[IndexedSeq[Int]]

  property("Either is a Monoid") = monoidLaws[Either[String,Int]]

  property("MapAlgebra.removeZeros works") = forAll { (m: Map[Int,Int]) =>
    !MapAlgebra.removeZeros(m).values.toSet.contains(0)
  }

  property("Monoid.sum performs w/ or w/o MapAlgebra.removeZeros") =
    forAll { (m: Map[Int,Int]) =>
      Monoid.sum(m) == Monoid.sum(MapAlgebra.removeZeros(m))
  }

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
  property("MapAlgebra.toGraph is correct") = forAll { (l: Set[(Int,Int)]) =>
    MapAlgebra.toGraph(l).toIterable.flatMap { case (k,sv) => sv.map { v => (k,v) } }.toSet == l
  }

  property("MapAlgebra.sparseEquiv is correct") =
    forAll { (l: Map[Int, String], empties: Set[Int]) =>
      (!empties.isEmpty) ==> {
        val mapEq = MapAlgebra.sparseEquiv[Int, String]
        mapEq.equiv(l -- empties, l ++ empties.map(_ -> "").toMap) && !mapEq.equiv(
          l -- empties,
          l ++ empties.map(_ -> "not empty").toMap
        )
      }
    }

  property("MapAlgebra.invert works") = forAll { (m : Map[Int,Int]) =>
    val m2 = MapAlgebra.invert(m)
    val m3 = Monoid.sum( for((v,ks) <- m2.toIterable; k <- ks.toIterable) yield Map(k -> v))
    m3 == m
  }
  property("MapAlgebra.invertExact works") = forAll { (m : Map[Option[Int],Set[Int]]) =>
    MapAlgebra.invertExact(MapAlgebra.invertExact(m)) == m
  }
  property("MapAlgebra.join works") = forAll { (m1: Map[Int, Int], m2: Map[Int,Int]) =>
    val m3 = MapAlgebra.join(m1, m2)
    val m1after = m3.mapValues { vw => vw._1 }.filter { _._2.isDefined }.mapValues { _.get }
    val m2after = m3.mapValues { vw => vw._2 }.filter { _._2.isDefined }.mapValues { _.get }
    (m1after == m1) && (m2after == m2) && (m3.keySet == (m1.keySet | m2.keySet))
  }

  def square(x: Int) = if (x % 2 == 0) Some(x * x) else None
  def mapEq[K] = MapAlgebra.sparseEquiv[K, Int]

  property("MapAlgebra.mergeLookup works") =
    forAll { (items: Set[Int]) =>
      mapEq.equiv(
        MapAlgebra.mergeLookup[Int, Option[Int], Int](items)(square)(_ => None),
        Map(
          (None: Option[Int]) -> Monoid.sum(items.map(x => square(x).getOrElse(0)))
        )
      ) && mapEq.equiv(
        MapAlgebra.mergeLookup[Int, Int, Int](items)(square)(identity),
        MapAlgebra.sumByKey(
          items.map(x => x -> square(x).getOrElse(0))
        )
      )
    }

  implicit def arbAV[T:Arbitrary:Monoid] : Arbitrary[AdaptiveVector[T]] =
    Arbitrary {
      Arbitrary.arbitrary[List[T]]
        .map { l =>
        AdaptiveVector.fromVector(Vector(l :_*), Monoid.zero[T])
      }
    }
  property("AdaptiveVector[Int] has a semigroup") = semigroupLawsEq[AdaptiveVector[Int]](Equiv[AdaptiveVector[Int]].equiv)
  property("AdaptiveVector[Int] has a monoid") = monoidLawsEq[AdaptiveVector[Int]](Equiv[AdaptiveVector[Int]].equiv)
  property("AdaptiveVector[Int] has a group") = groupLawsEq[AdaptiveVector[Int]](Equiv[AdaptiveVector[Int]].equiv)
  property("AdaptiveVector[String] has a monoid") = monoidLawsEq[AdaptiveVector[String]](Equiv[AdaptiveVector[String]].equiv)

}
