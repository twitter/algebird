package com.twitter.algebird.collections

import scala.collection.mutable
import scala.collection.immutable

private[algebird] class MutableBackedMap[K, V](val backingMap: mutable.Map[K, V])
    extends Map[K, V]
    with java.io.Serializable {
  override def get(key: K): Option[V] = backingMap.get(key)

  override def iterator: Iterator[(K, V)] = backingMap.iterator

  override def +[B1 >: V](kv: (K, B1)): Map[K, B1] = backingMap.toMap + kv

  override def -(key: K): Map[K, V] = backingMap.toMap - key
}

private[algebird] object compat {
  implicit class VectorExtensions(private val fact: immutable.Vector.type) extends AnyVal {
    def from[T](source: TraversableOnce[T]): immutable.Vector[T] = {
      val builder = immutable.Vector.newBuilder[T]
      builder ++= source
      builder.result()
    }
  }
}
