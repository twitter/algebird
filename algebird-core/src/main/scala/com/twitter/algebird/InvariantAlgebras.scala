package com.twitter.algebird

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
