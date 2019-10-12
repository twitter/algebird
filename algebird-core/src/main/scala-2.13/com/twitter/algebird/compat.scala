package com.twitter.algebird

import scala.collection.Factory
import scala.collection.mutable.{Builder, Map => MMap}

private[algebird] class MutableBackedMap[K, V](val backingMap: MMap[K, V])
    extends Map[K, V]
    with java.io.Serializable {
  override def get(key: K): Option[V] = backingMap.get(key)

  override def iterator: Iterator[(K, V)] = backingMap.iterator

  override def updated[B1 >: V](k: K, v: B1): Map[K, B1] = backingMap.toMap + (k -> v)

  override def removed(key: K): Map[K, V] = backingMap.toMap - key
}

private[algebird] trait CompatFold {

  /**
   * Simple Fold that collects elements into a container.
   */
  def container[I, C[_]](implicit cbf: Factory[I, C[I]]): Fold[I, C[I]] =
    Fold.foldMutable[Builder[I, C[I]], I, C[I]]({ case (b, i) => b += i }, { _ =>
      cbf.newBuilder
    }, { _.result })
}

private[algebird] trait CompatDecayedVector {
  // This is the default monoid that never thresholds.
  // If you want to set a specific accuracy you need to implicitly override this
  implicit def monoid[C[_]](
      implicit vs: VectorSpace[Double, C],
      metric: Metric[C[Double]],
      ord: Ordering[Double]
  ) =
    DecayedVector.monoidWithEpsilon(-1.0)
}
