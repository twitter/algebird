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

import com.twitter.algebird.Monad
import scala.util.{ Try, Success, Failure }

/**
 * Monad to handle mutating input state and possible failures.
 * This is used to interact in the planning phase with existing
 * mutable APIs (like storm or cascading), but retain the ability
 * to compose carefully.
 */
sealed trait StateWithError[S, +T] {
  def join[U](that: StateWithError[S, U], mergeErr: (Throwable, Throwable) => Throwable, mergeState: (S, S) => S): StateWithError[S, (T, U)] =
    StateFn { (requested: S) =>
      // TODO: deep joins could blow the stack, not yet using trampoline here
      (run(requested), that.run(requested)) match {
        case (Success((s1, r1)), Success((s2, r2))) => Success((mergeState(s1, s2), (r1, r2)))
        case (Failure(err1), Failure(err2)) => Failure(mergeErr(err1, err2)) // Our earlier is not ready
        case (Failure(err), _) => Failure(err)
        case (_, Failure(err)) => Failure(err)
      }
    }

  def apply(state: S): Try[(S, T)] = run(state)

  def run(state: S): Try[(S, T)]

  def flatMap[U](next: T => StateWithError[S, U]): StateWithError[S, U] =
    FlatMappedState(this, next)

  def map[U](fn: T => U): StateWithError[S, U] =
    FlatMappedState(this, { (t: T) => StateWithError.const(fn(t)) })
}
/** Simple wrapper of a function in the Monad */
final case class StateFn[S, T](fn: S => Try[(S, T)]) extends StateWithError[S, T] {
  def run(state: S) = fn(state)
}

/**
 * A Trampolining instance that should prevent stack overflow at the expense of performance
 */
final case class FlatMappedState[S, T, U](start: StateWithError[S, T], fn: T => StateWithError[S, U]) extends StateWithError[S, U] {
  def run(state: S): Try[(S, U)] = {
    @annotation.tailrec
    def loop(inState: S, st: StateWithError[S, Any], stack: List[Any => StateWithError[S, Any]]): Any = {
      st match {
        case StateFn(fn) =>
          fn(inState) match {
            case err @ Failure(_) => err // bail at first error
            case noError @ Success((newState, out)) =>
              stack match {
                case head :: tailStack => loop(newState, head(out), tailStack)
                case Nil => noError // recursion ends
              }
          }
        case FlatMappedState(st, next) => loop(inState, st, next :: stack)
      }
    }
    loop(state, this, Nil).asInstanceOf[Try[(S, U)]]
  }
}

object StateWithError {
  def getState[S]: StateWithError[S, S] = StateFn { (state: S) => Success(state, state) }
  def putState[S](newState: S): StateWithError[S, Unit] = StateFn { (_: S) => Success(newState, ()) }
  def swapState[S](newState: S): StateWithError[S, S] = StateFn { (old: S) => Success(newState, old) }

  def const[S, T](t: T): StateWithError[S, T] = StateFn { state: S => Success(state, t) }
  def lazyVal[S, T](t: => T): StateWithError[S, T] = StateFn { state: S => Success(state, t) }
  def failure[S](f: Throwable): StateWithError[S, Nothing] = StateFn(_ => Failure(f))

  /**
   * Use like fromTry[Int](Success("good"))
   * to get a constant Try in the monad
   */
  def fromTry[S] = new ConstantStateMaker[S]
  class ConstantStateMaker[S] {
    def apply[T](tryInstance: Try[T]): StateWithError[S, T] = { (s: S) =>
      tryInstance.map { (s, _) }
    }
  }

  class FunctionLifter[S] {
    def apply[I, T](fn: I => Try[T]): (I => StateWithError[S, T]) = { (i: I) =>
      StateFn { (s: S) => fn(i).map { (s, _) } }
    }
  }
  // TODO this should move to Monad and work for any Monad
  def toKleisli[S] = new FunctionLifter[S]

  implicit def apply[S, F, T](fn: S => Try[(S, T)]): StateWithError[S, T] = StateFn(fn)
  implicit def monad[S, F]: Monad[({ type Result[T] = StateWithError[S, T] })#Result] =
    new StateFMonad[F, S]

  class StateFMonad[F, S] extends Monad[({ type Result[T] = StateWithError[S, T] })#Result] {
    def apply[T](const: T) = { (s: S) => Success((s, const)) }
    def flatMap[T, U](earlier: StateWithError[S, T])(next: T => StateWithError[S, U]) = earlier.flatMap(next)
  }
}
