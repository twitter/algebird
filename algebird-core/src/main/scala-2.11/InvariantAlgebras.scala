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

class InvariantMonoid[T, U](forward: T => U, reverse: U => T)(implicit val monoid: Monoid[T])
    extends InvariantSemigroup[T, U](forward, reverse)
    with Monoid[U] {
  override val zero: U = forward(monoid.zero)
}

class InvariantGroup[T, U](forward: T => U, reverse: U => T)(implicit val group: Group[T])
    extends InvariantMonoid[T, U](forward, reverse)
    with Group[U] {
  override def negate(u: U): U = forward(group.negate(reverse(u)))
  override def minus(l: U, r: U): U =
    forward(group.minus(reverse(l), reverse(r)))
}

class InvariantRing[T, U](forward: T => U, reverse: U => T)(implicit val ring: Ring[T])
    extends InvariantGroup[T, U](forward, reverse)
    with Ring[U] {
  override val one: U = forward(ring.one)
  override def times(l: U, r: U): U =
    forward(ring.times(reverse(l), reverse(r)))
  override def product(iter: TraversableOnce[U]): U =
    forward(ring.product(iter.map(reverse)))
}
