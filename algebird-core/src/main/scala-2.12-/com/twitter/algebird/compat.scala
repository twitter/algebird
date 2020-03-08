package com.twitter.algebird

import scala.collection.mutable.{Builder, Map => MMap}

import scala.collection.generic.CanBuildFrom

private[algebird] class MutableBackedMap[K, V](val backingMap: MMap[K, V])
    extends Map[K, V]
    with java.io.Serializable {
  override def get(key: K): Option[V] = backingMap.get(key)

  override def iterator: Iterator[(K, V)] = backingMap.iterator

  override def +[B1 >: V](kv: (K, B1)): Map[K, B1] = backingMap.toMap + kv

  override def -(key: K): Map[K, V] = backingMap.toMap - key
}

private[algebird] trait CompatFold {

  /**
   * Simple Fold that collects elements into a container.
   */
  def container[I, C[_]](implicit cbf: CanBuildFrom[C[I], I, C[I]]): Fold[I, C[I]] =
    Fold.foldMutable[Builder[I, C[I]], I, C[I]]({ case (b, i) => b += i }, _ => cbf.apply(), _.result)
}

private[algebird] trait CompatDecayedVector {
  // This is the default monoid that never thresholds.
  // If you want to set a specific accuracy you need to implicitly override this
  implicit def monoid[F, C[_]](implicit vs: VectorSpace[F, C], metric: Metric[C[F]], ord: Ordering[F]) =
    DecayedVector.monoidWithEpsilon(-1.0)
}
