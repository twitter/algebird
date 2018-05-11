package com.twitter.algebird.generic

import shapeless._
import com.twitter.algebird._

object Shapeless extends Shapeless3 {

  implicit val hnilRing: Ring[HNil] =
    new ConstantRing(HNil)

}

abstract class Shapeless3 extends Shapeless2 {
  /**
   * Pairwise ring for arbitrary heterogeneous lists (HList).
   */
  implicit def hconsRing[A, B <: HList](implicit
    a: Ring[A],
    lb: Lazy[Ring[B]]): Ring[A :: B] = {
    // We use Lazy[Ring[A]] to avoid bogus ambiguous implicits at
    // the type-level. There is no value-level laziness needed, so we
    // immediately evaluate la.value.
    new HConsRing(a, lb.value)
  }

  def genericRing[A, Repr](implicit gen: Generic.Aux[A, Repr], r: Ring[Repr]): Ring[A] =
    new InvariantRing(gen.from _, gen.to _)
}

abstract class Shapeless2 extends Shapeless1 {
  /**
   * Pairwise group for arbitrary heterogeneous lists (HList).
   */
  implicit def hconsGroup[A, B <: HList](implicit
    a: Group[A],
    lb: Lazy[Group[B]]): Group[A :: B] = {
    // We use Lazy[Group[A]] to avoid bogus ambiguous implicits at
    // the type-level. There is no value-level laziness needed, so we
    // immediately evaluate la.value.
    new HConsGroup(a, lb.value)
  }

  def genericGroup[A, Repr](implicit gen: Generic.Aux[A, Repr], r: Group[Repr]): Group[A] =
    new InvariantGroup(gen.from _, gen.to _)
}

abstract class Shapeless1 extends Shapeless0 {
  /**
   * Pairwise monoid for arbitrary heterogeneous lists (HList).
   */
  implicit def hconsMonoid[A, B <: HList](implicit
    a: Monoid[A],
    lb: Lazy[Monoid[B]]): Monoid[A :: B] = {
    // We use Lazy[Monoid[A]] to avoid bogus ambiguous implicits at
    // the type-level. There is no value-level laziness needed, so we
    // immediately evaluate la.value.
    new HConsMonoid(a, lb.value)
  }

  def genericMonoid[A, Repr](implicit gen: Generic.Aux[A, Repr], m: Monoid[Repr]): Monoid[A] =
    new InvariantMonoid(gen.from _, gen.to _)
}

abstract class Shapeless0 {

  /**
   * Pairwise monoid for arbitrary heterogeneous lists (HList).
   */
  implicit def hconsSemigroup[A, B <: HList](implicit
    a: Semigroup[A],
    lb: Lazy[Semigroup[B]]): Semigroup[A :: B] =
    new HConsSemigroup[A, B](a, lb.value)

  def genericSemigroup[A, Repr](implicit gen: Generic.Aux[A, Repr], m: Semigroup[Repr]): Semigroup[A] =
    new InvariantSemigroup(gen.from _, gen.to _)
}

class HConsSemigroup[A, B <: HList](protected val a: Semigroup[A], protected val b: Semigroup[B]) extends Semigroup[A :: B] {
  def plus(x: A :: B, y: A :: B): A :: B =
    a.plus(x.head, y.head) :: b.plus(x.tail, y.tail)

  override def sumOption(xs: TraversableOnce[A :: B]): Option[A :: B] =
    if (xs.isEmpty) {
      None
    } else {
      val bufA = ArrayBufferedOperation.fromSumOption[A](1000)(a)
      val bufB = ArrayBufferedOperation.fromSumOption[B](1000)(b)
      xs.foreach {
        case a0 :: b0 =>
          bufA.put(a0)
          bufB.put(b0)
      }
      Some(bufA.flush.get :: bufB.flush.get)
    }

  override val hashCode = (a, b).hashCode

  override def equals(that: Any): Boolean =
    that match {
      case hcs: HConsSemigroup[_, _] =>
        (hashCode == hcs.hashCode) &&
          (a == hcs.a) &&
          (b == hcs.b)
      case _ => false
    }
}

class HConsMonoid[A, B <: HList](a: Monoid[A], b: Monoid[B]) extends HConsSemigroup(a, b) with Monoid[A :: B] {
  val zero: A :: B = a.zero :: b.zero
}

class HConsGroup[A, B <: HList](a: Group[A], b: Group[B]) extends HConsMonoid(a, b) with Group[A :: B] {
  override def minus(x: A :: B, y: A :: B): A :: B =
    a.minus(x.head, y.head) :: b.minus(x.tail, y.tail)

  def negate(x: A :: B): A :: B =
    a.negate(x.head) :: b.negate(x.tail)
}

class HConsRing[A, B <: HList](a: Ring[A], b: Ring[B]) extends HConsGroup(a, b) with Ring[A :: B] {
  val one: A :: B = a.one :: b.one

  def times(x: A :: B, y: A :: B): A :: B =
    a.times(x.head, y.head) :: b.times(x.tail, y.tail)
}
