package com.twitter.algebird.collections

import scala.collection.mutable

private[algebird] class MutableBackedMap[K, V](val backingMap: mutable.Map[K, V])
    extends Map[K, V]
    with java.io.Serializable {
  override def get(key: K): Option[V] = backingMap.get(key)

  override def iterator: Iterator[(K, V)] = backingMap.iterator

  override def updated[B1 >: V](k: K, v: B1): Map[K, B1] = backingMap.toMap + (k -> v)

  override def removed(key: K): Map[K, V] = backingMap.toMap - key
}

private[algebird] object compat
