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
import scala.collection.mutable.{ Builder, Map => MMap }

import com.twitter.algebird.macros.{ Cuber, Roller }

trait MapOperations[K, V, M <: ScMap[K, V]] {
  def add(oldMap: M, kv: (K, V)): M
  def remove(oldMap: M, k: K): M
  def fromMutable(mut: MMap[K, V]): M
}

abstract class GenericMapMonoid[K, V, M <: ScMap[K, V]](implicit val semigroup: Semigroup[V])
  extends Monoid[M] with MapOperations[K, V, M] {

  val nonZero: (V => Boolean) = semigroup match {
    case mon: Monoid[_] => mon.isNonZero(_)
    case _ => (_ => true)
  }

  override def isNonZero(x: M) =
    !x.isEmpty && (semigroup match {
      case mon: Monoid[_] => x.valuesIterator.exists { v =>
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
            val newV = if (oldVOpt.isEmpty) v else Semigroup.plus(oldVOpt.get, v)
            if (nonZero(newV))
              mutable.update(k, newV)
            else
              mutable.remove(k)
        }
      }
      Some(fromMutable(mutable))
    }
}

class MapMonoid[K, V](implicit semigroup: Semigroup[V]) extends GenericMapMonoid[K, V, Map[K, V]] {
  override lazy val zero = Map[K, V]()
  override def add(oldMap: Map[K, V], kv: (K, V)) = oldMap + kv
  override def remove(oldMap: Map[K, V], k: K) = oldMap - k
  override def fromMutable(mut: MMap[K, V]): Map[K, V] = new MutableBackedMap(mut)
}

class ScMapMonoid[K, V](implicit semigroup: Semigroup[V]) extends GenericMapMonoid[K, V, ScMap[K, V]] {
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
class MapGroup[K, V](implicit val group: Group[V]) extends MapMonoid[K, V]()(group)
  with Group[Map[K, V]] {
  override def negate(kv: Map[K, V]) = kv.mapValues { v => group.negate(v) }
}

class ScMapGroup[K, V](implicit val group: Group[V]) extends ScMapMonoid[K, V]()(group)
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

  implicit def sparseEquiv[K, V: Monoid: Equiv]: Equiv[Map[K, V]] = {
    Equiv.fromFunction { (m1, m2) =>
      val cleanM1 = removeZeros(m1)
      val cleanM2 = removeZeros(m2)
      rightContainsLeft(cleanM1, cleanM2) && rightContainsLeft(cleanM2, cleanM1)
    }
  }

  def mergeLookup[T, U, V: Monoid](keys: TraversableOnce[T])(lookup: T => Option[V])(present: T => U): Map[U, V] =
    sumByKey {
      keys.map { k =>
        present(k) -> lookup(k).getOrElse(Monoid.zero[V])
      }
    }

  // Returns a new map with zero-value entries removed
  def removeZeros[K, V: Monoid](m: Map[K, V]): Map[K, V] =
    m filter { case (_, v) => Monoid.isNonZero(v) }

  /**
   * For each key, sum all the values. Note that if V is a Monoid, the current
   * implementation will drop from the output any key where the values are all
   * Monoid.zero. If the Semigroup is a Monoid, This function is equivalent to:
   *
   *   pairs.filter(_._2 != Monoid.zero).groupBy(_._1).mapValues(_.map(_._2).sum)
   *
   * Otherwise, the function is equivalent to:
   *
   *   pairs.groupBy(_._1).mapValues(_.map(_._2).sum)
   */
  def sumByKey[K, V: Semigroup](pairs: TraversableOnce[(K, V)]): Map[K, V] =
    Monoid.sum(pairs map { Map(_) })

  /**
   * For each key, creates a list of all values. This function is equivalent to:
   *
   *   pairs.groupBy(_._1).mapValues(_.map(_._2))
   */
  def group[K, V](pairs: TraversableOnce[(K, V)]): Map[K, List[V]] =
    if (pairs.isEmpty) Map.empty
    else {
      val mutable = MMap[K, Builder[V, List[V]]]()
      pairs.foreach {
        case (k, v) =>
          val oldVOpt = mutable.get(k)
          // sorry for the micro optimization here: avoiding a closure
          val bldr = if (oldVOpt.isEmpty) {
            val b = List.newBuilder[V]
            mutable.update(k, b)
            b
          } else oldVOpt.get
          bldr += v
      }
      mutable.iterator.map { case (k, bldr) => (k, bldr.result) }.toMap
    }

  // Consider this as edges from k -> v, produce a Map[K,Set[V]]
  def toGraph[K, V](pairs: TraversableOnce[(K, V)]): Map[K, Set[V]] =
    Monoid.sum(pairs.map { case (k, v) => Map(k -> Set(v)) })

  /** join the keys of two maps (similar to outer-join in a DB) */
  def join[K, V, W](map1: Map[K, V], map2: Map[K, W]): Map[K, (Option[V], Option[W])] =
    Monoid.plus(map1.mapValues { v => (List(v), List[W]()) },
      map2.mapValues { w => (List[V](), List(w)) })
      .mapValues { case (v, w) => (v.headOption, w.headOption) }

  /**
   * Reverses a graph losslessly
   * None key is for v's with no sources.
   */
  def invertExact[K, V](m: Map[Option[K], Set[V]]): Map[Option[V], Set[K]] = {
    def nonEmptyIter[T](i: Iterable[T]): Iterable[Option[T]] =
      if (i.isEmpty) Iterable(None) else { i.map { Some(_) } }

    Monoid.sum {
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
    Monoid.sum(m.view.toIterable.map { case (k, v) => Map(v -> Set(k)) })

  def dot[K, V](left: Map[K, V], right: Map[K, V])(implicit mring: Ring[Map[K, V]], mon: Monoid[V]): V =
    Monoid.sum(mring.times(left, right).values)

  def cube[K, V](it: TraversableOnce[(K, V)])(implicit c: Cuber[K]): Map[c.K, List[V]] = {
    val map: collection.mutable.Map[c.K, List[V]] = collection.mutable.Map[c.K, List[V]]()
    it.toIterator.foreach {
      case (k, v) =>
        c(k).foreach { ik =>
          map.get(ik) match {
            case Some(vs) => map += ik -> (v :: vs)
            case None => map += ik -> List(v)
          }
        }
    }
    map.foreach { case (k, v) => map(k) = v.reverse }
    new MutableBackedMap(map)
  }

  def cubeSum[K, V](it: TraversableOnce[(K, V)])(implicit c: Cuber[K], sg: Semigroup[V]): Map[c.K, V] =
    sumByKey(it.toIterator.flatMap { case (k, v) => c(k).map((_, v)) })

  def cubeAggregate[T, K, U, V](it: TraversableOnce[T], agg: Aggregator[T, U, V])(fn: T => K)(implicit c: Cuber[K]): Map[c.K, V] =
    sumByKey(it.toIterator.flatMap { t => c(fn(t)).map((_, agg.prepare(t))) })(agg.semigroup)
      .map { case (k, v) => (k, agg.present(v)) }

  def rollup[K, V](it: TraversableOnce[(K, V)])(implicit r: Roller[K]): Map[r.K, List[V]] = {
    val map: collection.mutable.Map[r.K, List[V]] = collection.mutable.Map[r.K, List[V]]()
    it.toIterator.foreach {
      case (k, v) =>
        r(k).foreach { ik =>
          map.get(ik) match {
            case Some(vs) => map += ik -> (v :: vs)
            case None => map += ik -> List(v)
          }
        }
    }
    map.foreach { case (k, v) => map(k) = v.reverse }
    new MutableBackedMap(map)
  }

  def rollupSum[K, V](it: TraversableOnce[(K, V)])(implicit r: Roller[K], sg: Semigroup[V]): Map[r.K, V] =
    sumByKey(it.toIterator.flatMap { case (k, v) => r(k).map((_, v)) })

  def rollupAggregate[T, K, U, V](it: TraversableOnce[T], agg: Aggregator[T, U, V])(fn: T => K)(implicit r: Roller[K]): Map[r.K, V] =
    sumByKey(it.toIterator.flatMap { t => r(fn(t)).map((_, agg.prepare(t))) })(agg.semigroup)
      .map { case (k, v) => (k, agg.present(v)) }

}
