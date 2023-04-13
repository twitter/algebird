package com.twitter.algebird

class InvariantSemigroup[T, U](val forward: T => U, val reverse: U => T)(implicit val semigroup: Semigroup[T])
    extends Semigroup[U] {
  override def plus(l: U, r: U): U =
    forward(semigroup.plus(reverse(l), reverse(r)))
  override def sumOption(iter: TraversableOnce[U]): Option[U] =
    semigroup.sumOption(iter.map(reverse)).map(forward)

  /*
   * Note these work for the subclasses since in those cases semigroup
   * will be the appropriate algebra.
   */
  override val hashCode: Int = (forward, reverse, semigroup).hashCode
  override def equals(that: Any): Boolean =
    that match {
      case r: InvariantSemigroup[_, _] =>
        (hashCode == r.hashCode) &&
        (forward == r.forward) &&
        (reverse == r.reverse) &&
        (semigroup == r.semigroup)
      case _ => false
    }
}
