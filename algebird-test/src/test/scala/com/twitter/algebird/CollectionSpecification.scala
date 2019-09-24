package com.twitter.algebird

import org.scalacheck.{Arbitrary, Gen}

import scala.collection.mutable.{Map => MMap}
import scala.collection.{Map => ScMap}
import org.scalacheck.Prop._

class CollectionSpecification extends CheckProperties {
  import com.twitter.algebird.BaseProperties._
  import com.twitter.algebird.scalacheck.arbitrary._

  property("OrValMonoid is a commutative monoid") {
    commutativeMonoidLaws[OrVal]
  }

  property("AndValMonoid is a commutative monoid") {
    commutativeMonoidLaws[AndVal]
  }

  property("Either is a Semigroup") {
    semigroupLaws[Either[String, Int]]
  }

  property("Either is a Semigroup, with a Right non-monoid semigroup") {
    semigroupLaws[Either[String, Max[Int]]]
  }

  property("Option Monoid laws") {
    monoidLaws[Option[Int]] && monoidLaws[Option[String]]
  }

  property("Option Group laws") {
    implicit val equiv: Equiv[Map[String, Option[Int]]] =
      Equiv.fromFunction { (a, b) =>
        val keys: Set[String] = a.keySet | b.keySet
        keys.forall { key: String =>
          val v1: Int = a.getOrElse(key, None).getOrElse(0)
          val v2: Int = b.getOrElse(key, None).getOrElse(0)
          v1 == v2
        }
      }
    groupLaws[Option[Int]] && groupLaws[Map[String, Option[Int]]]
  }

  property("List plus") {
    forAll { (a: List[Int], b: List[Int]) =>
      val mon = implicitly[Monoid[List[Int]]]
      ((a ++ b == mon.plus(a, b)) && (mon.zero == List[Int]()))
    }
  }

  property("List Monoid laws") {
    monoidLaws[List[Int]]
  }

  implicit def arbSeq[T: Arbitrary]: Arbitrary[Seq[T]] =
    Arbitrary { implicitly[Arbitrary[List[T]]].arbitrary.map { _.toSeq } }

  property("Seq plus") {
    forAll { (a: Seq[Int], b: Seq[Int]) =>
      val mon = implicitly[Monoid[Seq[Int]]]
      ((a ++ b == mon.plus(a, b)) && (mon.zero == Seq[Int]()))
    }
  }

  property("Seq Monoid laws") {
    monoidLaws[Seq[Int]]
  }

  property("Array Monoid laws") {
    implicit val equiv: Equiv[Array[Int]] = Equiv.by(_.deep)
    monoidLaws[Array[Int]]
  }

  property("Set plus") {
    forAll { (a: Set[Int], b: Set[Int]) =>
      val mon = implicitly[Monoid[Set[Int]]]
      ((a ++ b == mon.plus(a, b)) && (mon.zero == Set[Int]()))
    }
  }

  property("Set Monoid laws") {
    monoidLaws[Set[Int]]
  }

  implicit def mapArb[K: Arbitrary, V: Arbitrary: Monoid] = Arbitrary {
    val mv = implicitly[Monoid[V]]
    implicitly[Arbitrary[Map[K, V]]].arbitrary
      .map {
        _.filter { kv =>
          mv.isNonZero(kv._2)
        }
      }
  }

  implicit def scMapArb[K: Arbitrary, V: Arbitrary: Monoid] = Arbitrary {
    mapArb[K, V].arbitrary
      .map { map: Map[K, V] =>
        map: ScMap[K, V]
      }
  }

  implicit def mMapArb[K: Arbitrary, V: Arbitrary: Monoid] = Arbitrary {
    mapArb[K, V].arbitrary
      .map { map: Map[K, V] =>
        MMap(map.toSeq: _*): MMap[K, V]
      }
  }

  def mapPlusTimesKeys[M <: ScMap[Int, Int]](implicit rng: Ring[ScMap[Int, Int]], arbMap: Arbitrary[M]) =
    forAll { (a: M, b: M) =>
      // Subsets because zeros are removed from the times/plus values
      ((rng.times(a, b)).keys.toSet.subsetOf((a.keys.toSet & b.keys.toSet)) &&
      (rng.plus(a, b)).keys.toSet.subsetOf((a.keys.toSet | b.keys.toSet)) &&
      (rng.plus(a, a).keys == (a.filter { kv =>
        (kv._2 + kv._2) != 0
      }).keys))
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

  property("Map[Int,Int] Monoid laws") {
    isAssociative[Map[Int, Int]] && weakZero[Map[Int, Int]]
  }

  property("ScMap[Int,Int] Monoid laws") {
    isAssociative[ScMap[Int, Int]] && weakZero[ScMap[Int, Int]]
  }

  property("MMap[Int,Int] Monoid laws") {
    isAssociativeDifferentTypes[ScMap[Int, Int], MMap[Int, Int]] && weakZeroDifferentTypes[ScMap[Int, Int], MMap[
      Int,
      Int
    ]]
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

  property("Map[Int,String] Monoid laws") {
    isAssociative[Map[Int, String]] && weakZero[Map[Int, String]]
  }

  property("ScMap[Int,String] Monoid laws") {
    isAssociative[ScMap[Int, String]] && weakZero[ScMap[Int, String]]
  }

  property("MMap[Int,String] Monoid laws") {
    isAssociativeDifferentTypes[ScMap[Int, Int], MMap[Int, Int]] && weakZeroDifferentTypes[ScMap[Int, Int], MMap[
      Int,
      Int
    ]]
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
    Arbitrary {
      implicitly[Arbitrary[List[T]]].arbitrary.map { _.toIndexedSeq }
    }

  property("IndexedSeq (of a Semigroup) is a semigroup") {
    semigroupLaws[IndexedSeq[Max[Int]]]
  }

  // TODO: this test fails sometimes due to the equiv not doing the right thing.
  // Fix by defining an Equiv.
  property("IndexedSeq is a pseudoRing") {
    pseudoRingLaws[IndexedSeq[Int]]
  }

  property("Either is a Monoid") {
    monoidLaws[Either[String, Int]]
  }

  property("MapAlgebra.removeZeros works") {
    forAll { (m: Map[Int, Int]) =>
      (MapAlgebra.removeZeros(m).values.toSet.contains(0) == false)
    }
  }

  property("Monoid.sum performs w/ or w/o MapAlgebra.removeZeros") {
    forAll { (m: Map[Int, Int]) =>
      (Monoid.sum(m) == Monoid.sum(MapAlgebra.removeZeros(m)))
    }
  }

  property("MapAlgebra.sumByKey works") {
    forAll { (keys: List[Int], values: List[Int]) =>
      import com.twitter.algebird.Operators._
      val tupList = keys.zip(values)
      val expected = tupList
        .groupBy { _._1 }
        .mapValues { v =>
          v.map { _._2 }.sum
        }
        .filter { _._2 != 0 }
      MapAlgebra.sumByKey(tupList) == expected && tupList.sumByKey == expected
    }
  }

  property("MapAlgebra.group works") {
    forAll { (keys: List[Int], values: List[Int]) =>
      import com.twitter.algebird.Operators._
      val tupList = keys.zip(values)
      val expected = tupList.groupBy(_._1).mapValues(_.map(_._2).toList)
      MapAlgebra.group(tupList) == expected && tupList.group == expected
    }
  }

  property("MapAlgebra.dot works") {
    forAll { (m1: Map[Int, Int], m2: Map[Int, Int]) =>
      // .toList below is to make sure we don't remove duplicate values
      (MapAlgebra.dot(m1, m2) ==
        (m1.keySet ++ m2.keySet).toList.map { k =>
          m1.getOrElse(k, 0) * m2.getOrElse(k, 0)
        }.sum)
    }
  }

  property("MapAlgebra.toGraph is correct") {
    forAll { (l: Set[(Int, Int)]) =>
      (MapAlgebra
        .toGraph(l)
        .toIterable
        .flatMap {
          case (k, sv) =>
            sv.map { v =>
              (k, v)
            }
        }
        .toSet == l)
    }
  }

  property("MapAlgebra.sparseEquiv is correct") {
    forAll { (l: Map[Int, String], empties: Set[Int]) =>
      (!empties.isEmpty) ==> {
        val mapEq = MapAlgebra.sparseEquiv[Int, String]
        mapEq.equiv(l -- empties, l ++ empties.map(_ -> "").toMap) && !mapEq
          .equiv(l -- empties, l ++ empties.map(_ -> "not empty").toMap)
      }
    }
  }

  property("MapAlgebra.invert works") {
    forAll { (m: Map[Int, Int]) =>
      val m2 = MapAlgebra.invert(m)
      val m3 = Monoid.sum(for {
        (v, ks) <- m2.toIterable
        k <- ks.toIterable
      } yield Map(k -> v))
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
      val m1after = m3
        .mapValues { vw =>
          vw._1
        }
        .filter { _._2.isDefined }
        .mapValues { _.get }
      val m2after = m3
        .mapValues { vw =>
          vw._2
        }
        .filter { _._2.isDefined }
        .mapValues { _.get }
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
        Map((None: Option[Int]) -> Monoid.sum(items.map(x => square(x).getOrElse(0))))
      ) && mapEq.equiv(
        MapAlgebra.mergeLookup[Int, Int, Int](items)(square)(identity),
        MapAlgebra.sumByKey(items.map(x => x -> square(x).getOrElse(0)))
      ))
    }
  }

  def arbAV[T: Arbitrary](sparse: T): Gen[AdaptiveVector[T]] =
    Gen.oneOf(
      for {
        l <- Arbitrary.arbitrary[List[T]]
      } yield AdaptiveVector.fromVector(Vector(l: _*), sparse),
      for {
        m <- Arbitrary.arbitrary[Map[Int, T]]
      } yield AdaptiveVector.fromMap(m.filter {
        case (k, _) => (k < 1000) && (k >= 0)
      }, sparse, 1000),
      for {
        size <- Gen.posNum[Int]
      } yield AdaptiveVector.fromMap(Map.empty, sparse, size)
    )

  property("AdaptiveVector[Int] has a semigroup") {
    implicit val arb = Arbitrary(arbAV(2))
    semigroupLaws[AdaptiveVector[Int]]
  }

  property("AdaptiveVector[Int] has a monoid") {
    // TODO: remove this equiv instance once #583 is resolved.
    implicit val equiv = AdaptiveVector.denseEquiv[Int]
    implicit val arb = Arbitrary(arbAV(0))
    monoidLaws[AdaptiveVector[Int]]
  }

  property("AdaptiveVector[Int] has a group") {
    implicit val arb = Arbitrary(arbAV(1))
    groupLaws[AdaptiveVector[Int]]
  }

  property("AdaptiveVector[String] has a monoid") {
    // TODO: remove this equiv instance once #583 is resolved.
    implicit val equiv = AdaptiveVector.denseEquiv[String]
    implicit val arb = Arbitrary(arbAV(""))
    monoidLaws[AdaptiveVector[String]]
  }
}
