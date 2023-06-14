package com.twitter.algebird
import java.lang.{
  Boolean => JBool,
  Double => JDouble,
  Float => JFloat,
  Integer => JInt,
  Long => JLong,
  Short => JShort
}
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap}

import scala.collection.JavaConverters._

/**
 * Since maps are mutable, this always makes a full copy. Prefer scala immutable maps if you use scala
 * immutable maps, this operation is much faster TODO extend this to Group, Ring
 */
class JMapMonoid[K, V: Semigroup] extends Monoid[JMap[K, V]] {
  override lazy val zero: JHashMap[K, V] = new JHashMap[K, V](0)

  val nonZero: (V => Boolean) = implicitly[Semigroup[V]] match {
    case mon: Monoid[_] => mon.isNonZero(_)
    case _              => _ => true
  }

  override def isNonZero(x: JMap[K, V]): Boolean =
    !x.isEmpty && (implicitly[Semigroup[V]] match {
      case mon: Monoid[_] =>
        x.values.asScala.exists(v => mon.isNonZero(v))
      case _ => true
    })
  override def plus(x: JMap[K, V], y: JMap[K, V]): JHashMap[K, V] = {
    val (big, small, bigOnLeft) =
      if (x.size > y.size) {
        (x, y, true)
      } else {
        (y, x, false)
      }
    val vsemi = implicitly[Semigroup[V]]
    val result = new JHashMap[K, V](big.size + small.size)
    result.putAll(big)
    small.entrySet.asScala.foreach { kv =>
      val smallK = kv.getKey
      val smallV = kv.getValue
      if (big.containsKey(smallK)) {
        val bigV = big.get(smallK)
        val newV =
          if (bigOnLeft) vsemi.plus(bigV, smallV) else vsemi.plus(smallV, bigV)
        if (nonZero(newV))
          result.put(smallK, newV)
        else
          result.remove(smallK)
      } else {
        // No need to explicitly add with zero on V, just put in the small value
        result.put(smallK, smallV)
      }
    }
    result
  }
}
