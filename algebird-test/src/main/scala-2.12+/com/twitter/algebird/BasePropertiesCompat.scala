package com.twitter.algebird

private[algebird] trait BasePropertiesCompat {
  def isNonZero[V: Semigroup](v: V): Boolean =
    implicitly[Semigroup[V]] match {
      case mon: Monoid[?] => mon.isNonZero(v)
      case _              => true
    }
}
