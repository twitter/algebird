package com.twitter.algebird

import scala.collection.mutable.{Builder, Map => MMap}
import scala.collection.{Map => ScMap}
import scala.collection.compat._

abstract class GenericMapMonoid[K, V, M <: ScMap[K, V]](implicit val semigroup: Semigroup[V])
    extends Monoid[M]
    with MapOperations[K, V, M] {

  val nonZero: (V => Boolean) = semigroup match {
    case mon: Monoid[?] => mon.isNonZero(_)
    case _              => _ => true
  }

  override def isNonZero(x: M): Boolean =
    !x.isEmpty && (semigroup match {
      case mon: Monoid[?] =>
        x.valuesIterator.exists(v => mon.isNonZero(v))
      case _ => true
    })

  override def plus(x: M, y: M): M = {
    // Scala maps can reuse internal structure, so don't copy just add into the bigger one:
    // This really saves computation when adding lots of small maps into big ones (common)
    val (big, small, bigOnLeft) =
      if (x.size > y.size) {
        (x, y, true)
      } else {
        (y, x, false)
      }
    small match {
      // Mutable maps create new copies of the underlying data on add so don't use the
      // handleImmutable method.
      // Cannot have a None so 'get' is safe here.
      case _: MMap[?, ?] => sumOption(Seq(big, small)).get
      case _             => handleImmutable(big, small, bigOnLeft)
    }
  }

  private def handleImmutable(big: M, small: M, bigOnLeft: Boolean) =
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
        add(oldMap, kv._1 -> newV)
      else
        remove(oldMap, kv._1)
    }
  override def sumOption(items: TraversableOnce[M]): Option[M] =
    if (items.iterator.isEmpty) None
    else {
      val mutable = MMap[K, V]()
      items.iterator.foreach { m =>
        m.foreach { case (k, v) =>
          val oldVOpt = mutable.get(k)
          // sorry for the micro optimization here: avoiding a closure
          val newV =
            if (oldVOpt.isEmpty) v else Semigroup.plus(oldVOpt.get, v)
          if (nonZero(newV))
            mutable.update(k, newV)
          else
            mutable.remove(k)
        }
      }
      Some(fromMutable(mutable))
    }
}
