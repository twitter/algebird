package com.twitter.algebird

import org.scalacheck.{ Arbitrary, Gen }

import scala.collection.mutable.{ Map => MMap }
import scala.collection.{ Map => ScMap }
import org.scalacheck.Prop._

class CollectionSpecification extends CheckProperties {
  import com.twitter.algebird.BaseProperties._

  implicit def arbMin[T: Arbitrary]: Arbitrary[Min[T]] =
    Arbitrary { implicitly[Arbitrary[T]].arbitrary.map{ x => Min(x) } }
  implicit def arbMax[T: Arbitrary]: Arbitrary[Max[T]] =
    Arbitrary { implicitly[Arbitrary[T]].arbitrary.map{ x => Max(x) } }
  implicit def arbOrVal: Arbitrary[OrVal] =
    Arbitrary { implicitly[Arbitrary[Boolean]].arbitrary.map{ b => OrVal(b) } }
  implicit def arbAndVal: Arbitrary[AndVal] =
    Arbitrary { implicitly[Arbitrary[Boolean]].arbitrary.map{ b => AndVal(b) } }

  property("MinHasAdditionOperator is a commutative semigroup") {
    commutativeHasAdditionOperatorLaws[Min[Int]]
  }

  property("MaxHasAdditionOperator is a commutative semigroup") {
    commutativeHasAdditionOperatorLaws[Max[Int]]
  }

  property("OrValHasAdditionOperatorAndZero is a commutative monoid") {
    commutativeHasAdditionOperatorAndZeroLaws[OrVal]
  }

  property("AndValHasAdditionOperatorAndZero is a commutative monoid") {
    commutativeHasAdditionOperatorAndZeroLaws[AndVal]
  }

  property("Min[Int] is a monoid") {
    monoidLaws[Min[Int]]
  }

  property("Max[String] is a monoid") {
    monoidLaws[Max[String]]
  }

  property("Max[List[Int]] is a monoid") {
    monoidLaws[Max[List[Int]]]
  }

  property("Either is a HasAdditionOperator") {
    semigroupLaws[Either[String, Int]]
  }

  property("Either is a HasAdditionOperator, with a Right non-monoid semigroup") {
    semigroupLaws[Either[String, Max[Int]]]
  }

  property("Option HasAdditionOperatorAndZero laws") {
    monoidLaws[Option[Int]] && monoidLaws[Option[String]]
  }

  property("Option Group laws") {
    groupLaws[Option[Int]] && groupLawsEq[Map[String, Option[Int]]]{
      (a: Map[String, Option[Int]], b: Map[String, Option[Int]]) =>
        val keys: Set[String] = a.keySet | b.keySet
        keys.forall { key: String =>
          val v1: Int = a.getOrElse(key, None).getOrElse(0)
          val v2: Int = b.getOrElse(key, None).getOrElse(0)
          v1 == v2
        }
    }
  }

  property("List plus") {
    forAll { (a: List[Int], b: List[Int]) =>
      val mon = implicitly[HasAdditionOperatorAndZero[List[Int]]]
      ((a ++ b == mon.plus(a, b)) && (mon.zero == List[Int]()))
    }
  }

  property("List HasAdditionOperatorAndZero laws") {
    monoidLaws[List[Int]]
  }

  implicit def arbSeq[T: Arbitrary]: Arbitrary[Seq[T]] =
    Arbitrary { implicitly[Arbitrary[List[T]]].arbitrary.map { _.toSeq } }

  property("Seq plus") {
    forAll { (a: Seq[Int], b: Seq[Int]) =>
      val mon = implicitly[HasAdditionOperatorAndZero[Seq[Int]]]
      ((a ++ b == mon.plus(a, b)) && (mon.zero == Seq[Int]()))
    }
  }

  property("Seq HasAdditionOperatorAndZero laws") {
    monoidLaws[Seq[Int]]
  }

  property("Array HasAdditionOperatorAndZero laws") {
    monoidLawsEq[Array[Int]]{
      case (a, b) => a.deep == b.deep
    }
  }

  property("Set plus") {
    forAll { (a: Set[Int], b: Set[Int]) =>
      val mon = implicitly[HasAdditionOperatorAndZero[Set[Int]]]
      ((a ++ b == mon.plus(a, b)) && (mon.zero == Set[Int]()))
    }
  }

  property("Set HasAdditionOperatorAndZero laws") {
    monoidLaws[Set[Int]]
  }

  implicit def mapArb[K: Arbitrary, V: Arbitrary: HasAdditionOperatorAndZero] = Arbitrary {
    val mv = implicitly[HasAdditionOperatorAndZero[V]]
    implicitly[Arbitrary[Map[K, V]]]
      .arbitrary
      .map { _.filter { kv => mv.isNonZero(kv._2) } }
  }

  implicit def scMapArb[K: Arbitrary, V: Arbitrary: HasAdditionOperatorAndZero] = Arbitrary {
    mapArb[K, V]
      .arbitrary
      .map { map: Map[K, V] => map: ScMap[K, V] }
  }

  implicit def mMapArb[K: Arbitrary, V: Arbitrary: HasAdditionOperatorAndZero] = Arbitrary {
    mapArb[K, V]
      .arbitrary
      .map { map: Map[K, V] => MMap(map.toSeq: _*): MMap[K, V] }
  }

  def mapPlusTimesKeys[M <: ScMap[Int, Int]](implicit rng: Ring[ScMap[Int, Int]], arbMap: Arbitrary[M]) = {
    forAll { (a: M, b: M) =>
      // Subsets because zeros are removed from the times/plus values
      ((rng.times(a, b)).keys.toSet.subsetOf((a.keys.toSet & b.keys.toSet)) &&
        (rng.plus(a, b)).keys.toSet.subsetOf((a.keys.toSet | b.keys.toSet)) &&
        (rng.plus(a, a).keys == (a.filter { kv => (kv._2 + kv._2) != 0 }).keys))
    }
  }

  property("Map plus/times keys") {
    mapPlusTimesKeys[Map[Int, Int]]
  }

  property("ScMap plus/times keys") {
    mapPlusTimesKeys[ScMap[Int, Int]]
  }

  property("MMap plus/times keys") {
    mapPlusTimesKeys[MMap[Int, Int]]
  }

  property("Map[Int,Int] HasAdditionOperatorAndZero laws") {
    isAssociative[Map[Int, Int]] && weakZero[Map[Int, Int]]
  }

  property("ScMap[Int,Int] HasAdditionOperatorAndZero laws") {
    isAssociative[ScMap[Int, Int]] && weakZero[ScMap[Int, Int]]
  }

  property("MMap[Int,Int] HasAdditionOperatorAndZero laws") {
    isAssociativeDifferentTypes[ScMap[Int, Int], MMap[Int, Int]] && weakZeroDifferentTypes[ScMap[Int, Int], MMap[Int, Int]]
  }

  property("Map[Int,Int] has -") {
    hasAdditiveInverses[Map[Int, Int]]
  }

  property("ScMap[Int,Int] has -") {
    hasAdditiveInverses[ScMap[Int, Int]]
  }

  property("MMap[Int,Int] has -") {
    hasAdditiveInversesDifferentTypes[ScMap[Int, Int], MMap[Int, Int]]
  }

  property("Map[Int,String] HasAdditionOperatorAndZero laws") {
    isAssociative[Map[Int, String]] && weakZero[Map[Int, String]]
  }

  property("ScMap[Int,String] HasAdditionOperatorAndZero laws") {
    isAssociative[ScMap[Int, String]] && weakZero[ScMap[Int, String]]
  }

  property("MMap[Int,String] HasAdditionOperatorAndZero laws") {
    isAssociativeDifferentTypes[ScMap[Int, Int], MMap[Int, Int]] && weakZeroDifferentTypes[ScMap[Int, Int], MMap[Int, Int]]
  }

  // We haven't implemented ring.one yet for the Map, so skip the one property
  property("Map is distributive") {
    isDistributive[Map[Int, Int]]
  }

  property("ScMap is distributive") {
    isDistributive[ScMap[Int, Int]]
  }

  property("MMap is distributive") {
    isDistributiveDifferentTypes[ScMap[Int, Int], MMap[Int, Int]]
  }

  implicit def arbIndexedSeq[T: Arbitrary]: Arbitrary[IndexedSeq[T]] =
    Arbitrary { implicitly[Arbitrary[List[T]]].arbitrary.map { _.toIndexedSeq } }

  property("IndexedSeq (of a HasAdditionOperator) is a semigroup") {
    semigroupLaws[IndexedSeq[Max[Int]]]
  }

  // TODO: this test fails sometimes due to the equiv not doing the right thing.
  // Fix by defining and Equiv and having all the properties use an implicit Equiv
  property("IndexedSeq is a pseudoRing") {
    pseudoRingLaws[IndexedSeq[Int]]
  }

  property("Either is a HasAdditionOperatorAndZero") {
    monoidLaws[Either[String, Int]]
  }

  property("MapAlgebra.removeZeros works") {
    forAll { (m: Map[Int, Int]) =>
      (MapAlgebra.removeZeros(m).values.toSet.contains(0) == false)
    }
  }

  property("HasAdditionOperatorAndZero.sum performs w/ or w/o MapAlgebra.removeZeros") {
    forAll { (m: Map[Int, Int]) =>
      (HasAdditionOperatorAndZero.sum(m) == HasAdditionOperatorAndZero.sum(MapAlgebra.removeZeros(m)))
    }
  }

  property("sumByKey works") {
    forAll { (keys: List[Int], values: List[Int]) =>
      import com.twitter.algebird.Operators._
      val tupList = keys.zip(values)
      (tupList.sumByKey.filter { _._2 != 0 } ==
        tupList.groupBy { _._1 }
        .mapValues { v => v.map { _._2 }.sum }
        .filter { _._2 != 0 })
    }
  }

  property("MapAlgebra.dot works") {
    forAll { (m1: Map[Int, Int], m2: Map[Int, Int]) =>
      // .toList below is to make sure we don't remove duplicate values
      (MapAlgebra.dot(m1, m2) ==
        (m1.keySet ++ m2.keySet).toList.map { k => m1.getOrElse(k, 0) * m2.getOrElse(k, 0) }.sum)
    }
  }

  property("MapAlgebra.toGraph is correct") {
    forAll { (l: Set[(Int, Int)]) =>
      (MapAlgebra.toGraph(l).toIterable.flatMap { case (k, sv) => sv.map { v => (k, v) } }.toSet == l)
    }
  }

  property("MapAlgebra.sparseEquiv is correct") {
    forAll { (l: Map[Int, String], empties: Set[Int]) =>
      (!empties.isEmpty) ==> {
        val mapEq = MapAlgebra.sparseEquiv[Int, String]
        mapEq.equiv(l -- empties, l ++ empties.map(_ -> "").toMap) && !mapEq.equiv(
          l -- empties,
          l ++ empties.map(_ -> "not empty").toMap)
      }
    }
  }

  property("MapAlgebra.invert works") {
    forAll { (m: Map[Int, Int]) =>
      val m2 = MapAlgebra.invert(m)
      val m3 = HasAdditionOperatorAndZero.sum(for ((v, ks) <- m2.toIterable; k <- ks.toIterable) yield Map(k -> v))
      (m3 == m)
    }
  }

  property("MapAlgebra.invertExact works") {
    forAll { (m: Map[Option[Int], Set[Int]]) =>
      (MapAlgebra.invertExact(MapAlgebra.invertExact(m)) == m.filterKeys(_.isDefined))
    }
  }

  property("MapAlgebra.join works") {
    forAll { (m1: Map[Int, Int], m2: Map[Int, Int]) =>
      val m3 = MapAlgebra.join(m1, m2)
      val m1after = m3.mapValues { vw => vw._1 }.filter { _._2.isDefined }.mapValues { _.get }
      val m2after = m3.mapValues { vw => vw._2 }.filter { _._2.isDefined }.mapValues { _.get }
      val m1Orm2 = (m1.keySet | m2.keySet)
      ((m1after == m1) && (m2after == m2) && (m3.keySet == m1Orm2))
    }
  }

  def square(x: Int) = if (x % 2 == 0) Some(x * x) else None
  def mapEq[K] = MapAlgebra.sparseEquiv[K, Int]

  property("MapAlgebra.mergeLookup works") {
    forAll { (items: Set[Int]) =>
      (mapEq.equiv(
        MapAlgebra.mergeLookup[Int, Option[Int], Int](items)(square)(_ => None),
        Map(
          (None: Option[Int]) -> HasAdditionOperatorAndZero.sum(items.map(x => square(x).getOrElse(0))))) && mapEq.equiv(
          MapAlgebra.mergeLookup[Int, Int, Int](items)(square)(identity),
          MapAlgebra.sumByKey(
            items.map(x => x -> square(x).getOrElse(0)))))
    }
  }

  def arbAV[T: Arbitrary](sparse: T): Gen[AdaptiveVector[T]] =
    Gen.oneOf(
      for {
        l <- Arbitrary.arbitrary[List[T]]
      } yield AdaptiveVector.fromVector(Vector(l: _*), sparse),
      for {
        m <- Arbitrary.arbitrary[Map[Int, T]]
      } yield AdaptiveVector.fromMap(m.filter{ case (k, _) => (k < 1000) && (k >= 0) },
        sparse, 1000))

  property("AdaptiveVector[Int] has a semigroup") {
    implicit val arb = Arbitrary(arbAV(2))
    semigroupLawsEquiv[AdaptiveVector[Int]]
  }

  property("AdaptiveVector[Int] has a monoid") {
    implicit val arb = Arbitrary(arbAV(0))
    monoidLawsEq[AdaptiveVector[Int]](Equiv[AdaptiveVector[Int]].equiv)
  }

  property("AdaptiveVector[Int] has a group") {
    implicit val arb = Arbitrary(arbAV(1))
    groupLawsEq[AdaptiveVector[Int]](Equiv[AdaptiveVector[Int]].equiv)
  }

  property("AdaptiveVector[String] has a monoid") {
    implicit val arb = Arbitrary(arbAV(""))
    monoidLawsEq[AdaptiveVector[String]](Equiv[AdaptiveVector[String]].equiv)
  }
}
