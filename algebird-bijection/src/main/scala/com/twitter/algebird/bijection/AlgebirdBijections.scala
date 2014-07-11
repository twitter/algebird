/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.twitter.algebird.bijection

import com.twitter.algebird.{ Field, Group, Monoid, Ring, Semigroup }
import com.twitter.bijection.{ AbstractBijection, Bijection, ImplicitBijection, Conversion, Reverse }

import Conversion.asMethod // "as" syntax

/**
 * Bijections on Algebird's abstract algebra datatypes.
 *
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

class BijectedSemigroup[T, U](implicit val sg: Semigroup[T], bij: ImplicitBijection[T, U]) extends Semigroup[U] {
  def bijection: Bijection[U, T] = bij.bijection.inverse
  override def plus(l: U, r: U): U = sg.plus(l.as[T], r.as[T]).as[U]
  override def sumOption(iter: TraversableOnce[U]): Option[U] =
    sg.sumOption(iter.map { _.as[T] }).as[Option[U]]
}

class BijectedMonoid[T, U](implicit val monoid: Monoid[T], bij: ImplicitBijection[T, U]) extends BijectedSemigroup[T, U] with Monoid[U] {
  override def zero: U = monoid.zero.as[U]
}

class BijectedGroup[T, U](implicit val group: Group[T], bij: ImplicitBijection[T, U]) extends BijectedMonoid[T, U] with Group[U] {
  override def negate(u: U): U = group.negate(u.as[T]).as[U]
  override def minus(l: U, r: U): U = group.minus(l.as[T], r.as[T]).as[U]
}

class BijectedRing[T, U](implicit val ring: Ring[T], bij: ImplicitBijection[T, U]) extends BijectedGroup[T, U] with Ring[U] {
  override def one: U = ring.one.as[U]
  override def times(l: U, r: U): U = ring.times(l.as[T], r.as[T]).as[U]
  override def product(iter: TraversableOnce[U]): U =
    ring.product(iter map { _.as[T] }).as[U]
}

class BijectedField[T, U](implicit val field: Field[T], bij: ImplicitBijection[T, U]) extends BijectedRing[T, U] with Field[U] {
  override def div(l: U, r: U): U = field.div(l.as[T], r.as[T]).as[U]
  override def inverse(u: U): U = field.inverse(u.as[T]).as[U]
}

trait AlgebirdBijections {
  implicit def semigroupBijection[T, U](implicit bij: ImplicitBijection[T, U]): Bijection[Semigroup[T], Semigroup[U]] =
    new AbstractBijection[Semigroup[T], Semigroup[U]] {
      override def apply(sg: Semigroup[T]) = new BijectedSemigroup[T, U]()(sg, bij)
      override def invert(sg: Semigroup[U]) = new BijectedSemigroup[U, T]()(sg, Reverse(bij.bijection))
    }

  implicit def monoidBijection[T, U](implicit bij: ImplicitBijection[T, U]): Bijection[Monoid[T], Monoid[U]] =
    new AbstractBijection[Monoid[T], Monoid[U]] {
      override def apply(mon: Monoid[T]) = new BijectedMonoid[T, U]()(mon, bij)
      override def invert(mon: Monoid[U]) = new BijectedMonoid[U, T]()(mon, Reverse(bij.bijection))
    }

  implicit def groupBijection[T, U](implicit bij: ImplicitBijection[T, U]): Bijection[Group[T], Group[U]] =
    new AbstractBijection[Group[T], Group[U]] {
      override def apply(group: Group[T]) = new BijectedGroup[T, U]()(group, bij)
      override def invert(group: Group[U]) = new BijectedGroup[U, T]()(group, Reverse(bij.bijection))
    }

  implicit def ringBijection[T, U](implicit bij: ImplicitBijection[T, U]): Bijection[Ring[T], Ring[U]] =
    new AbstractBijection[Ring[T], Ring[U]] {
      override def apply(ring: Ring[T]) = new BijectedRing[T, U]()(ring, bij)
      override def invert(ring: Ring[U]) = new BijectedRing[U, T]()(ring, Reverse(bij.bijection))
    }

  implicit def fieldBijection[T, U](implicit bij: ImplicitBijection[T, U]): Bijection[Field[T], Field[U]] =
    new AbstractBijection[Field[T], Field[U]] {
      override def apply(field: Field[T]) = new BijectedField[T, U]()(field, bij)
      override def invert(field: Field[U]) = new BijectedField[U, T]()(field, Reverse(bij.bijection))
    }
}

object AlgebirdBijections extends AlgebirdBijections

trait ImplicitBijectedAlgebraics extends AlgebirdBijections {
  // The following is necessary in order to ensure that the automatic algebraic bijections don't diverge in the typechecker
  sealed class =!=[A, B]

  sealed trait LowerPriorityImplicits {
    implicit def equal[A]: =!=[A, A] = sys.error("should not be called")
  }

  object =!= extends LowerPriorityImplicits {
    implicit def nequal[A, B](implicit same: A =:= B = null): =!=[A, B] =
      if (same != null) sys.error("should not be called explicitly with same type")
      else new =!=[A, B]
  }

  implicit def bijSemi[T, U](implicit bij: ImplicitBijection[T, U], ev: T =!= U, t: Semigroup[T]): Semigroup[U] = t.as[Semigroup[U]]
  implicit def bijMonoid[T, U](implicit bij: ImplicitBijection[T, U], ev: T =!= U, t: Monoid[T]): Monoid[U] = t.as[Monoid[U]]
  implicit def bijGroup[T, U](implicit bij: ImplicitBijection[T, U], ev: T =!= U, t: Group[T]): Group[U] = t.as[Group[U]]
  implicit def bijRing[T, U](implicit bij: ImplicitBijection[T, U], ev: T =!= U, t: Ring[T]): Ring[U] = t.as[Ring[U]]
  implicit def bijField[T, U](implicit bij: ImplicitBijection[T, U], ev: T =!= U, t: Field[T]): Field[U] = t.as[Field[U]]
}

object ImplicitBijectedAlgebraics extends ImplicitBijectedAlgebraics
