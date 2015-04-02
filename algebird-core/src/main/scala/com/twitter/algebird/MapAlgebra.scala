/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.algebird

import scala.collection.{ Map => ScMap }
import scala.collection.mutable.{ Map => MMap }

trait MapOperations[K, V, M <: ScMap[K, V]] {
  def add(oldMap: M, kv: (K, V)): M
  def remove(oldMap: M, k: K): M
  def fromMutable(mut: MMap[K, V]): M
}

abstract class GenericMapHasAdditionOperatorAndZero[K, V, M <: ScMap[K, V]](implicit val semigroup: HasAdditionOperator[V])
  extends HasAdditionOperatorAndZero[M] with MapOperations[K, V, M] {

  val nonZero: (V => Boolean) = semigroup match {
    case mon: HasAdditionOperatorAndZero[_] => mon.isNonZero(_)
    case _ => (_ => true)
  }

  override def isNonZero(x: M) =
    !x.isEmpty && (semigroup match {
      case mon: HasAdditionOperatorAndZero[_] => x.valuesIterator.exists { v =>
        mon.isNonZero(v)
      }
      case _ => true
    })

  override def plus(x: M, y: M) = {
    // Scala maps can reuse internal structure, so don't copy just add into the bigger one:
    // This really saves computation when adding lots of small maps into big ones (common)
    val (big, small, bigOnLeft) = if (x.size > y.size) { (x, y, true) } else { (y, x, false) }
    small match {
      // Mutable maps create new copies of the underlying data on add so don't use the
      // handleImmutable method.
      // Cannot have a None so 'get' is safe here.
      case mmap: MMap[_, _] => sumOption(Seq(big, small)).get
      case _ => handleImmutable(big, small, bigOnLeft)
    }
  }

  private def handleImmutable(big: M, small: M, bigOnLeft: Boolean) = {
    small.foldLeft(big) { (oldMap, kv) =>
      val newV = big
        .get(kv._1)
        .map { bigV =>
          if (bigOnLeft)
            semigroup.plus(bigV, kv._2)
          else
            semigroup.plus(kv._2, bigV)
        }
        .getOrElse(kv._2)
      if (nonZero(newV))
        add(oldMap, (kv._1 -> newV))
      else
        remove(oldMap, kv._1)
    }
  }
  override def sumOption(items: TraversableOnce[M]): Option[M] =
    if (items.isEmpty) None
    else {
      val mutable = MMap[K, V]()
      items.foreach { m =>
        m.foreach {
          case (k, v) =>
            val oldVOpt = mutable.get(k)
            // sorry for the micro optimization here: avoiding a closure
            val newV = if (oldVOpt.isEmpty) v else HasAdditionOperator.plus(oldVOpt.get, v)
            if (nonZero(newV))
              mutable.update(k, newV)
            else
              mutable.remove(k)
        }
      }
      Some(fromMutable(mutable))
    }
}

class MapHasAdditionOperatorAndZero[K, V](implicit semigroup: HasAdditionOperator[V]) extends GenericMapHasAdditionOperatorAndZero[K, V, Map[K, V]] {
  override lazy val zero = Map[K, V]()
  override def add(oldMap: Map[K, V], kv: (K, V)) = oldMap + kv
  override def remove(oldMap: Map[K, V], k: K) = oldMap - k
  override def fromMutable(mut: MMap[K, V]): Map[K, V] = new MutableBackedMap(mut)
}

class ScMapHasAdditionOperatorAndZero[K, V](implicit semigroup: HasAdditionOperator[V]) extends GenericMapHasAdditionOperatorAndZero[K, V, ScMap[K, V]] {
  override lazy val zero = ScMap[K, V]()
  override def add(oldMap: ScMap[K, V], kv: (K, V)) = oldMap + kv
  override def remove(oldMap: ScMap[K, V], k: K) = oldMap - k
  override def fromMutable(mut: MMap[K, V]): ScMap[K, V] = new MutableBackedMap(mut)
}

private[this] class MutableBackedMap[K, V](val backingMap: MMap[K, V]) extends Map[K, V] with java.io.Serializable {
  def get(key: K) = backingMap.get(key)

  def iterator = backingMap.iterator

  def +[B1 >: V](kv: (K, B1)) = backingMap.toMap + kv

  def -(key: K) = backingMap.toMap - key
}

/**
 * You can think of this as a Sparse vector group
 */
class MapGroup[K, V](implicit val group: Group[V]) extends MapHasAdditionOperatorAndZero[K, V]()(group)
  with Group[Map[K, V]] {
  override def negate(kv: Map[K, V]) = kv.mapValues { v => group.negate(v) }
}

class ScMapGroup[K, V](implicit val group: Group[V]) extends ScMapHasAdditionOperatorAndZero[K, V]()(group)
  with Group[ScMap[K, V]] {
  override def negate(kv: ScMap[K, V]) = kv.mapValues { v => group.negate(v) }
}

/**
 * You can think of this as a Sparse vector ring
 */
trait GenericMapRing[K, V, M <: ScMap[K, V]] extends Ring[M] with MapOperations[K, V, M] {

  implicit def ring: Ring[V]

  // It is possible to implement this, but we need a special "identity map" which we
  // deal with as if it were map with all possible keys (.get(x) == ring.one for all x).
  // Then we have to manage the delta from this map as we add elements.  That said, it
  // is not actually needed in matrix multiplication, so we are punting on it for now.
  override def one = sys.error("multiplicative identity for Map unimplemented")
  override def times(x: M, y: M): M = {
    val (big, small, bigOnLeft) = if (x.size > y.size) { (x, y, true) } else { (y, x, false) }
    small.foldLeft(zero) { (oldMap, kv) =>
      val bigV = big.getOrElse(kv._1, ring.zero)
      val newV = if (bigOnLeft) ring.times(bigV, kv._2) else ring.times(kv._2, bigV)
      if (ring.isNonZero(newV)) {
        add(oldMap, (kv._1 -> newV))
      } else {
        remove(oldMap, kv._1)
      }
    }
  }
}

class MapRing[K, V](implicit val ring: Ring[V]) extends MapGroup[K, V]()(ring)
  with GenericMapRing[K, V, Map[K, V]]

class ScMapRing[K, V](implicit val ring: Ring[V]) extends ScMapGroup[K, V]()(ring)
  with GenericMapRing[K, V, ScMap[K, V]]

object MapAlgebra {
  def rightContainsLeft[K, V: Equiv](l: Map[K, V], r: Map[K, V]): Boolean =
    l.forall {
      case (k, v) =>
        r.get(k).exists(Equiv[V].equiv(_, v))
    }

  implicit def sparseEquiv[K, V: HasAdditionOperatorAndZero: Equiv]: Equiv[Map[K, V]] = {
    Equiv.fromFunction { (m1, m2) =>
      val cleanM1 = removeZeros(m1)
      val cleanM2 = removeZeros(m2)
      rightContainsLeft(cleanM1, cleanM2) && rightContainsLeft(cleanM2, cleanM1)
    }
  }

  def mergeLookup[T, U, V: HasAdditionOperatorAndZero](keys: TraversableOnce[T])(lookup: T => Option[V])(present: T => U): Map[U, V] =
    sumByKey {
      keys.map { k =>
        present(k) -> lookup(k).getOrElse(HasAdditionOperatorAndZero.zero[V])
      }
    }

  // Returns a new map with zero-value entries removed
  def removeZeros[K, V: HasAdditionOperatorAndZero](m: Map[K, V]): Map[K, V] =
    m filter { case (_, v) => HasAdditionOperatorAndZero.isNonZero(v) }

  // groupBy ks, sum all the vs
  def sumByKey[K, V: HasAdditionOperator](pairs: TraversableOnce[(K, V)]): Map[K, V] =
    HasAdditionOperatorAndZero.sum(pairs map { Map(_) })

  // Consider this as edges from k -> v, produce a Map[K,Set[V]]
  def toGraph[K, V](pairs: TraversableOnce[(K, V)]): Map[K, Set[V]] =
    HasAdditionOperatorAndZero.sum(pairs.map { case (k, v) => Map(k -> Set(v)) })

  /** join the keys of two maps (similar to outer-join in a DB) */
  def join[K, V, W](map1: Map[K, V], map2: Map[K, W]): Map[K, (Option[V], Option[W])] =
    HasAdditionOperatorAndZero.plus(map1.mapValues { v => (List(v), List[W]()) },
      map2.mapValues { w => (List[V](), List(w)) })
      .mapValues { case (v, w) => (v.headOption, w.headOption) }

  /**
   * Reverses a graph losslessly
   * None key is for v's with no sources.
   */
  def invertExact[K, V](m: Map[Option[K], Set[V]]): Map[Option[V], Set[K]] = {
    def nonEmptyIter[T](i: Iterable[T]): Iterable[Option[T]] =
      if (i.isEmpty) Iterable(None) else { i.map { Some(_) } }

    HasAdditionOperatorAndZero.sum {
      for (
        (k, sv) <- m.view.toIterable;
        v <- nonEmptyIter(sv)
      ) yield Map(v -> k.toSet)
    }
  }
  /**
   * Invert the Common case of exactly one value for each key
   */
  def invert[K, V](m: Map[K, V]): Map[V, Set[K]] =
    HasAdditionOperatorAndZero.sum(m.view.toIterable.map { case (k, v) => Map(v -> Set(k)) })

  def dot[K, V](left: Map[K, V], right: Map[K, V])(implicit mring: Ring[Map[K, V]], mon: HasAdditionOperatorAndZero[V]): V =
    HasAdditionOperatorAndZero.sum(mring.times(left, right).values)
}
