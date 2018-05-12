package com.twitter.algebird

import org.scalatest.FunSuite

/**
 * This is just a compilation test that we can resolve
 * algebird types from implicit algebra instances.
 */
class AlgebraResolutionTest extends FunSuite {
  // A type with no built in algebird algebras
  trait Empty {}
  test("algebra.Semigroup") {
    implicit def fakeSemigroup[T]: algebra.Semigroup[T] = null
    implicitly[Semigroup[Empty]]
  }
  test("algebra.ring.AdditiveSemigroup") {
    implicit def fakeAdditiveSemigroup[T]: algebra.ring.AdditiveSemigroup[T] =
      Semigroup.from[T] { (a, b) =>
        a
      }

    implicitly[Semigroup[Empty]]
  }
  test("algebra.Monoid") {
    implicit def fakeMonoid[T]: algebra.Monoid[T] = null
    implicitly[Monoid[Empty]]
  }
  test("algebra.ring.AdditiveMonoid") {
    implicit def fakeAdditiveMonoid[T]: algebra.ring.AdditiveMonoid[T] =
      Monoid.from[T](null.asInstanceOf[T]) { (a, b) =>
        a
      }

    implicitly[Monoid[Empty]]
  }
  test("algebra.Group") {
    implicit def fakeGroup[T]: algebra.Group[T] = null
    implicitly[Group[Empty]]
  }
  test("algebra.ring.AdditiveGroup") {
    implicit def fakeAdditiveGroup[T]: algebra.ring.AdditiveGroup[T] =
      implicitly[Group[Null]].asInstanceOf[Group[T]]

    implicitly[Group[Empty]]
  }
  test("algebra.ring.Ring") {
    implicit def fakeRing[T]: algebra.ring.Ring[T] = null
    implicitly[Ring[Empty]]
  }
}
