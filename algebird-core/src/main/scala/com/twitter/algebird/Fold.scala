/*
Copyright 2014 Twitter, Inc.

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
package com.twitter.algebird

import java.io.Serializable
import scala.collection.compat._

/**
 * Folds are first-class representations of "Traversable.foldLeft." They have the nice property that
 * they can be fused to work in parallel over an input sequence.
 *
 * A Fold accumulates inputs (I) into some internal type (X), converting to a defined output type
 * (O) when done.  We use existential types to hide internal details and to allow for internal and
 * external (X and O) types to differ for "map" and "join."
 *
 * In discussing this type we draw parallels to Function1 and related types. You can think of a
 * fold as a function "Seq[I] => O" but in reality we do not have to materialize the input sequence
 * at once to "run" the fold.
 *
 * The traversal of the input data structure is NOT done by Fold itself. Instead we expose some
 * methods like "overTraversable" that know how to iterate through various sequence types and drive
 * the fold. We also expose some internal state so library authors can fold over their own types.
 *
 * See the companion object for constructors.
 */
sealed trait Fold[-I, +O] extends Serializable {

  /**
   * Users can ignore this type.
   *
   * The internal accumulator type. No one outside this Fold needs to know what this is, and that's
   * a good thing. It keeps type signatures sane and makes this easy to use for the amount of
   * flexibility it provides.
   */
  type X

  /**
   * Users can ignore this method.  It is exposed so library authors can run folds over their own
   * sequence types.
   *
   * "build" constructs a FoldState, which tells us how to run the fold.  It is expected that we can
   * run the same Fold many times over different data structures, but we must build a new FoldState
   * every time.
   *
   * See FoldState for information on how to use this for your own sequence types.
   */
  def build(): FoldState[X, I, O]

  /**
   * Transforms the output of the Fold after iteration is complete. This is analogous to
   * "Future.map" or "Function1.compose."
   */
  def map[P](f: O => P): Fold[I, P] = {
    val self = this
    new Fold[I, P] {
      type X = self.X
      override def build: FoldState[X, I, P] =
        self.build.map(f)
    }
  }

  /**
   * Joins two folds into one and combines the results. The fused fold accumulates with both at the
   * same time and combines at the end.
   */
  def joinWith[I2 <: I, P, Q](other: Fold[I2, P])(f: (O, P) => Q): Fold[I2, Q] = {
    val self = this
    new Fold[I2, Q] {
      type X = (self.X, other.X)
      override def build(): FoldState[X, I2, Q] = {
        val first = self.build()
        val second = other.build()
        new FoldState(
          {
            case ((x, y), i) => (first.add(x, i), second.add(y, i))
          },
          (first.start, second.start),
          {
            case (x, y) => f(first.end(x), second.end(y))
          }
        )
      }
    }
  }

  /**
   * Convenient shorthand for joining Folds without combining at the end.
   */
  def join[I2 <: I, P](other: Fold[I2, P]): Fold[I2, (O, P)] =
    joinWith(other) { case (o, p) => (o, p) }

  /**
   * Transforms the input of the fold before every accumulation. (The name comes from "contravariant
   * map.") This is analogous to "Function1.andThen."
   */
  def contramap[H](f: H => I): Fold[H, O] = {
    val self = this
    new Fold[H, O] {
      type X = self.X
      override def build(): FoldState[X, H, O] =
        self.build().contramap(f)
    }
  }

  /**
   * Trivially runs a Fold over an empty sequence.
   */
  def overEmpty: O = {
    // build is a "def" so we construct the state once and use the pieces to run the fold
    val state = build()
    state.end(state.start)
  }

  /**
   * Trivially runs a Fold over a single element sequence.
   */
  def overSingleton(i: I): O = {
    val state = build()
    state.end(state.add(state.start, i))
  }

  /**
   * Runs a Fold over a Traversable.
   */
  def overTraversable(is: TraversableOnce[I]): O = {
    val state = build()
    state.end(is.iterator.foldLeft(state.start)(state.add))
  }
}

/**
 * A FoldState defines a left fold with a "hidden" accumulator type. It is exposed so
 * library authors can run Folds over their own sequence types.
 *
 * The fold can be executed correctly according to the properties of "add" and your traversed
 * data structure. For example, the "add" function of a monoidal fold will be associative. A
 * FoldState is valid for only one iteration because the accumulator (seeded by "start"  may be
 * mutable.
 *
 * The three components of a fold are
 *   add: (X, I) => X - updates and returns internal state for every input I
 *   start: X - the initial state
 *   end: X => O - transforms internal state to a final result
 *
 * Folding over Seq(x, y) would produce the result
 *   end(add(add(start, x), y))
 */
final class FoldState[X, -I, +O] private[algebird] (val add: (X, I) => X, val start: X, val end: X => O)
    extends Serializable {

  /**
   * Transforms the output type of the FoldState (see Fold.map).
   */
  def map[P](f: O => P): FoldState[X, I, P] =
    new FoldState(add, start, end.andThen(f))

  /**
   * Transforms the input type of the FoldState (see Fold.contramap).
   */
  def contramap[H](f: H => I): FoldState[X, H, O] =
    new FoldState((x, h) => add(x, f(h)), start, end)
}

/**
 * Methods to create and run Folds.
 *
 * The Folds defined here are immutable and serializable, which we expect by default. It is
 * important that you as a user indicate mutability or non-serializability when defining new Folds.
 * Additionally, it is recommended that "end" functions not mutate the accumulator in order to
 * support scans (producing a stream of intermediate outputs by calling "end" at each step).
 */
object Fold extends CompatFold {

  /**
   * "import Fold.applicative" will bring the Applicative instance into scope. See FoldApplicative.
   */
  implicit def applicative[I]: Applicative[Fold[I, *]] =
    new FoldApplicative[I]

  /**
   * Turn a common Scala foldLeft into a Fold.
   * The accumulator MUST be immutable and serializable.
   */
  def foldLeft[I, O](o: O)(add: (O, I) => O): Fold[I, O] =
    fold[O, I, O](add, o, o => o)

  /**
   * A general way of defining Folds that supports a separate accumulator type.
   * The accumulator MUST be immutable and serializable.
   */
  def fold[M, I, O](add: (M, I) => M, start: M, end: M => O): Fold[I, O] =
    new Fold[I, O] {
      type X = M
      override def build(): FoldState[X, I, O] =
        new FoldState(add, start, end)
    }

  /**
   * A general way of defining Folds that supports constructing mutable or non-serializable
   * accumulators.
   */
  def foldMutable[M, I, O](add: (M, I) => M, start: Unit => M, end: M => O): Fold[I, O] =
    new Fold[I, O] {
      type X = M
      override def build(): FoldState[X, I, O] =
        new FoldState(add, start(()), end)
    }

  /**
   * Fuse a sequence of Folds into one that outputs the result of each.
   */
  def sequence[I, O](ms: Seq[Fold[I, O]]): Fold[I, Seq[O]] =
    new Fold[I, Seq[O]] {
      type X = Seq[Any]
      override def build(): FoldState[Seq[Any], I, Seq[O]] = {
        val bs: Seq[FoldState[Any, I, O]] =
          ms.map(_.build().asInstanceOf[FoldState[Any, I, O]])
        val adds =
          bs.map(_.add)
        val ends =
          bs.map(_.end)
        val starts: Seq[Any] =
          bs.map(_.start)
        val add: (Seq[Any], I) => Seq[Any] = { (xs, i) => adds.zip(xs).map { case (f, x) => f(x, i) } }
        val end: (Seq[Any] => Seq[O]) = { xs => ends.zip(xs).map { case (f, x) => f(x) } }
        new FoldState(add, starts, end)
      }
    }

  /**
   * An even simpler Fold that collects into a Seq.  Shorthand for "container[I, Seq];" fewer type
   * arguments, better type inferrence.
   */
  def seq[I]: Fold[I, Seq[I]] =
    container[I, Seq]

  /**
   * A Fold that does no work and returns a constant.  Analogous to Function1 const:
   *   def const[A, B](b: B): (A => B) = { _ => b }
   */
  def const[O](value: O): Fold[Any, O] =
    Fold.foldLeft(value) { case (u, _) => u }

  /**
   * A Fold that runs the given side effect for every element.
   */
  def foreach[I](e: I => Unit): Fold[I, Unit] =
    Fold.foldLeft(()) { case (_, i) => e(i) }

  /**
   * A Fold that returns the first value in a sequence.
   */
  def first[I]: Fold[I, Option[I]] =
    Fold.foldLeft[I, Option[I]](None) {
      case (None, i) => Some(i)
      case (x, _)    => x
    }

  /**
   * A Fold that returns the last value in a sequence.
   */
  def last[I]: Fold[I, Option[I]] =
    Fold.foldLeft[I, Option[I]](None) { case (_, i) => Some(i) }

  /**
   * A Fold that returns the max value in a sequence. (Biased to earlier equal values.)
   */
  def max[I](implicit ordering: Ordering[I]): Fold[I, Option[I]] =
    Fold.foldLeft[I, Option[I]](None) {
      case (None, i)                                  => Some(i)
      case (Some(y), i) if ordering.compare(y, i) < 0 => Some(i)
      case (x, _)                                     => x
    }

  /**
   * A Fold that returns a min value in a sequence. (Biased to earlier equal values.)
   */
  def min[I](implicit ordering: Ordering[I]): Fold[I, Option[I]] =
    Fold.foldLeft[I, Option[I]](None) {
      case (None, i)                                  => Some(i)
      case (Some(y), i) if ordering.compare(y, i) > 0 => Some(i)
      case (x, _)                                     => x
    }

  /**
   * A Fold that returns the sum of a numeric sequence. Does not protect against overflow.
   */
  def sum[I](implicit numeric: Monoid[I]): Fold[I, I] =
    Fold.foldLeft(numeric.zero) { case (x, i) => numeric.plus(x, i) }

  /**
   * For a semigroup, if we get more than 0 items, use plus
   */
  def sumOption[T](implicit sg: Semigroup[T]): Fold[T, Option[T]] =
    Fold.foldLeft(None: Option[T]) {
      case (None, i)    => Some(i)
      case (Some(l), r) => Some(sg.plus(l, r))
    }

  /**
   * A Fold that returns the product of a numeric sequence. Does not protect against overflow.
   */
  def product[I](implicit numeric: Ring[I]): Fold[I, I] =
    Fold.foldLeft(numeric.one) { case (x, i) => numeric.times(x, i) }

  /**
   * A Fold that returns the length of a sequence.
   */
  def size: Fold[Any, Long] =
    Fold.foldLeft(0L) { case (x, _) => x + 1 }

  /**
   * A Fold that returns "true" if all elements of the sequence statisfy the predicate.
   * Note this does not short-circuit enumeration of the sequence.
   */
  def forall[I](pred: I => Boolean): Fold[I, Boolean] =
    foldLeft(true)((b, i) => b && pred(i))

  /**
   * A Fold that returns "true" if any element of the sequence statisfies the predicate.
   * Note this does not short-circuit enumeration of the sequence.
   */
  def exists[I](pred: I => Boolean): Fold[I, Boolean] =
    foldLeft(false)((b, i) => b || pred(i))

  /**
   * A Fold that counts the number of elements satisfying the predicate.
   */
  def count[I](pred: I => Boolean): Fold[I, Long] =
    foldLeft(0L) {
      case (c, i) if pred(i) => c + 1L
      case (c, _)            => c
    }
}

/**
 * Folds are Applicatives!
 */
class FoldApplicative[I] extends Applicative[Fold[I, *]] {
  override def map[T, U](mt: Fold[I, T])(fn: T => U): Fold[I, U] =
    mt.map(fn)
  override def apply[T](v: T): Fold[I, T] =
    Fold.const(v)
  override def join[T, U](mt: Fold[I, T], mu: Fold[I, U]): Fold[I, (T, U)] =
    mt.join(mu)
  override def sequence[T](ms: Seq[Fold[I, T]]): Fold[I, Seq[T]] =
    Fold.sequence(ms)
  override def joinWith[T, U, V](mt: Fold[I, T], mu: Fold[I, U])(fn: (T, U) => V): Fold[I, V] =
    mt.joinWith(mu)(fn)
}
