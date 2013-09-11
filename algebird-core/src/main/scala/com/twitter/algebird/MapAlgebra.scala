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

import scala.annotation.tailrec
import scala.collection.{Map => ScMap}

object GenericMapMonoid {
  def nonZeroFunctor[V](implicit semigroup: Semigroup[V]): (V => Boolean) = semigroup match {
    case mon: Monoid[_] => mon.isNonZero(_)
    case _ => (_ => true)
  }

  def isNonZero[K, V](x : ScMap[K,V])(implicit semigroup: Semigroup[V]) =
    !x.isEmpty && (semigroup match {
      case mon: Monoid[_] => x.valuesIterator.exists { v =>
        mon.isNonZero(v)
      }
      case _ => true
    })

  def plus[K, V](x : ScMap[K, V], y : ScMap[K, V], nonZeroFn: (V => Boolean))
      (implicit semigroup: Semigroup[V]): ScMap[K, V] = {
    // Scala maps can reuse internal structure, so don't copy just add into the bigger one:
    // This really saves computation when adding lots of small maps into big ones (common)
    val (big, small, bigOnLeft) = if(x.size > y.size) { (x,y,true) } else { (y,x,false) }
    small.foldLeft(big) { (oldMap, kv) =>
      val newV = big
        .get(kv._1)
        .map { bigV =>
        if(bigOnLeft)
            semigroup.plus(bigV, kv._2)
          else
            semigroup.plus(kv._2, bigV)
        }
        .getOrElse(kv._2)
      if (nonZeroFn(newV))
        oldMap + (kv._1 -> newV)
      else
        oldMap - kv._1
    }
  }
}

class MapMonoid[K,V](implicit val semigroup: Semigroup[V]) extends Monoid[Map[K,V]] {
  val nonZeroFn: (V => Boolean) = GenericMapMonoid.nonZeroFunctor
  override def isNonZero(x : Map[K,V]) = GenericMapMonoid.isNonZero(x)
  override lazy val zero = Map[K,V]()
  override def plus(x : Map[K,V], y : Map[K,V]) = GenericMapMonoid.plus(x, y, nonZeroFn).asInstanceOf[Map[K,V]]
}

class ScMapMonoid[K,V](implicit val semigroup: Semigroup[V]) extends Monoid[ScMap[K,V]] {
  val nonZeroFn: (V => Boolean) = GenericMapMonoid.nonZeroFunctor
  override def isNonZero(x : ScMap[K,V]) = GenericMapMonoid.isNonZero(x)
  override lazy val zero = ScMap[K,V]()
  override def plus(x : ScMap[K,V], y : ScMap[K,V]) = GenericMapMonoid.plus(x, y, nonZeroFn)
}

/** You can think of this as a Sparse vector group
 */
class MapGroup[K,V](implicit val group : Group[V]) extends MapMonoid[K,V]()(group)
  with Group[Map[K,V]] {
  override def negate(kv : Map[K,V]) = kv.mapValues { v => group.negate(v) }
}

/** You can think of this as a Sparse vector ring
 */
class MapRing[K,V](implicit val ring : Ring[V]) extends MapGroup[K,V]()(ring) with Ring[Map[K,V]] {
  // It is possible to implement this, but we need a special "identity map" which we
  // deal with as if it were map with all possible keys (.get(x) == ring.one for all x).
  // Then we have to manage the delta from this map as we add elements.  That said, it
  // is not actually needed in matrix multiplication, so we are punting on it for now.
  override def one = sys.error("multiplicative identity for Map unimplemented")
  override def times(x : Map[K,V], y : Map[K,V]) : Map[K,V] = {
    val (big, small, bigOnLeft) = if(x.size > y.size) { (x,y,true) } else { (y,x,false) }
    small.foldLeft(zero) { (oldMap, kv) =>
      val bigV = big.getOrElse(kv._1, ring.zero)
      val newV = if(bigOnLeft) ring.times(bigV, kv._2) else ring.times(kv._2, bigV)
      if (ring.isNonZero(newV)) {
        oldMap + (kv._1 -> newV)
      }
      else {
        oldMap - kv._1
      }
    }
  }
}

object MapAlgebra {
  def rightContainsLeft[K,V: Equiv](l: Map[K, V], r: Map[K, V]): Boolean =
    l.forall { case (k, v) =>
      r.get(k).exists(Equiv[V].equiv(_, v))
    }

  implicit def sparseEquiv[K,V: Monoid: Equiv]: Equiv[Map[K, V]] = {
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
  def removeZeros[K,V:Monoid](m: Map[K,V]): Map[K,V] =
    m filter { case (_,v) => Monoid.isNonZero(v) }

  // groupBy ks, sum all the vs
  def sumByKey[K,V:Semigroup](pairs: TraversableOnce[(K,V)]): Map[K,V] =
    Monoid.sum(pairs map { Map(_) })

  // Consider this as edges from k -> v, preduce a Map[K,Set[V]]
  def toGraph[K,V](pairs: TraversableOnce[(K,V)]): Map[K, Set[V]] =
    Monoid.sum(pairs.map { case (k,v) => Map(k -> Set(v)) })

  /** join the keys of two maps (similar to outer-join in a DB) */
  def join[K,V,W](map1: Map[K,V], map2: Map[K,W]): Map[K,(Option[V],Option[W])] =
    Monoid.plus(map1.mapValues { v => (List(v), List[W]()) },
      map2.mapValues { w => (List[V](), List(w)) })
      .mapValues { case (v,w) => (v.headOption, w.headOption) }

  /** Reverses a graph losslessly
   * None key is for v's with no sources.
   */
  def invertExact[K,V](m: Map[Option[K], Set[V]]): Map[Option[V], Set[K]] = {
    def nonEmptyIter[T](i: Iterable[T]): Iterable[Option[T]] =
      if(i.isEmpty) Iterable(None) else { i.map { Some(_) } }

    Monoid.sum {
      for((k, sv) <- m.view.toIterable;
          v <- nonEmptyIter(sv)) yield Map(v -> k.toSet)
    }
  }
  /** Invert the Common case of exactly one value for each key
   */
  def invert[K,V](m: Map[K,V]): Map[V, Set[K]] =
    Monoid.sum(m.view.toIterable.map { case (k,v) => Map(v -> Set(k)) })

  def dot[K,V](left : Map[K,V], right : Map[K,V])
    (implicit mring: Ring[Map[K,V]], mon: Monoid[V]): V =
    Monoid.sum(mring.times(left, right).values)
}
