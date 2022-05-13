/*
 Copyright 2013 Twitter, Inc.

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

package com.twitter.algebird.monad

import com.twitter.algebird.{Monad, Semigroup}

/**
 * Monad to handle mutating input state and possible failures. This is used to interact in the planning phase
 * with existing mutable APIs (like storm or cascading), but retain the ability to compose carefully.
 */
sealed trait StateWithError[S, +F, +T] {
  def join[F1 >: F, U](
      that: StateWithError[S, F1, U],
      mergeErr: (F1, F1) => F1,
      mergeState: (S, S) => S
  ): StateWithError[S, F1, (T, U)] =
    join(that)(Semigroup.from(mergeErr), Semigroup.from(mergeState))

  def join[F1 >: F, U](that: StateWithError[S, F1, U])(implicit
      sgf: Semigroup[F1],
      sgs: Semigroup[S]
  ): // TODO: deep joins could blow the stack, not yet using trampoline here
  StateWithError[S, F1, (T, U)] =
    StateFn { (requested: S) =>
      (run(requested), that.run(requested)) match {
        case (Right(s1, r1), Right(s2, r2)) =>
          Right((sgs.plus(s1, s2), (r1, r2)))
        case (Left(err1), Left(err2)) =>
          Left(sgf.plus(err1, err2)) // Our earlier is not ready
        case (Left(err), _) => Left(err)
        case (_, Left(err)) => Left(err)
      }
    }

  def apply(state: S): Either[F, (S, T)] = run(state)

  def run(state: S): Either[F, (S, T)]

  def flatMap[F1 >: F, U](next: T => StateWithError[S, F1, U]): StateWithError[S, F1, U] =
    FlatMappedState(this, next)

  def map[U](fn: (T) => U): StateWithError[S, F, U] =
    FlatMappedState(this, (t: T) => StateWithError.const(fn(t)))
}

/** Simple wrapper of a function in the Monad */
final case class StateFn[S, F, T](fn: S => Either[F, (S, T)]) extends StateWithError[S, F, T] {
  override def run(state: S): Either[F, (S, T)] = fn(state)
}

/**
 * A Trampolining instance that should prevent stack overflow at the expense of performance
 */
final case class FlatMappedState[S, F, T, U](start: StateWithError[S, F, T], fn: T => StateWithError[S, F, U])
    extends StateWithError[S, F, U] {
  override def run(state: S): Either[F, (S, U)] = {
    @annotation.tailrec
    def loop(inState: S, st: StateWithError[S, F, Any], stack: List[Any => StateWithError[S, F, Any]]): Any =
      st match {
        case StateFn(fn) =>
          fn(inState) match {
            case err @ Left(_) => err // bail at first error
            case noError @ Right(newState, out) =>
              stack match {
                case head :: tailStack => loop(newState, head(out), tailStack)
                case Nil               => noError // recursion ends
              }
          }
        case FlatMappedState(st, next) => loop(inState, st, next :: stack)
      }
    loop(state, this, Nil).asInstanceOf[Either[F, (S, U)]]
  }
}

object StateWithError {
  def getState[S]: StateWithError[S, Nothing, S] =
    StateFn((state: S) => Right((state, state)))
  def putState[S](newState: S): StateWithError[S, Nothing, Unit] =
    StateFn((_: S) => Right((newState, ())))
  def swapState[S](newState: S): StateWithError[S, Nothing, S] =
    StateFn((old: S) => Right((newState, old)))

  def const[S, T](t: T): StateWithError[S, Nothing, T] =
    StateFn((state: S) => Right((state, t)))
  def lazyVal[S, T](t: => T): StateWithError[S, Nothing, T] =
    StateFn((state: S) => Right((state, t)))
  def failure[S, F](f: F): StateWithError[S, F, Nothing] =
    StateFn(_ => Left(f))

  /**
   * Use like fromEither[Int](Right("good")) to get a constant Either in the monad
   */
  def fromEither[S]: ConstantStateMaker[S] = new ConstantStateMaker[S]
  class ConstantStateMaker[S] {
    def apply[F, T](either: Either[F, T]): StateWithError[S, F, T] = { (s: S) => either.right.map((s, _)) }
  }

  class FunctionLifter[S] {
    def apply[I, F, T](fn: I => Either[F, T]): (I => StateWithError[S, F, T]) = { (i: I) =>
      StateFn((s: S) => fn(i).right.map((s, _)))
    }
  }
  // TODO this should move to Monad and work for any Monad
  def toKleisli[S]: FunctionLifter[S] = new FunctionLifter[S]

  implicit def apply[S, F, T](fn: S => Either[F, (S, T)]): StateWithError[S, F, T] = StateFn(fn)
  implicit def monad[S, F]: Monad[StateWithError[S, F, *]] = new StateFMonad[F, S]

  class StateFMonad[F, S] extends Monad[StateWithError[S, F, *]] {
    override def apply[T](const: T): StateWithError[S, Nothing, T] = { (s: S) => Right((s, const)) }
    override def flatMap[T, U](
        earlier: StateWithError[S, F, T]
    )(next: T => StateWithError[S, F, U]): StateWithError[S, F, U] =
      earlier.flatMap(next)
  }
}
