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

import com.twitter.algebird.ChainableCallbackCollectorBuilder

// A simple trampoline implementation which we copied for the State monad
sealed trait Trampoline[+A] {
  def map[B](fn: A => B): Trampoline[B]
  def flatMap[B](fn: A => Trampoline[B]): Trampoline[B] = FlatMapped(this, fn)
  /**
   * get triggers the computation which is run exactly once
   */
  def get: A
}

final case class Done[A](override val get: A) extends Trampoline[A] {
  def map[B](fn: A => B) = Done(fn(get))
}

final case class FlatMapped[C, A](start: Trampoline[C], fn: C => Trampoline[A]) extends Trampoline[A] {
  def map[B](fn: A => B) = FlatMapped(this, { (a: A) => Done(fn(a)) })
  lazy val get = Trampoline.run(this)
}

object Trampoline {
  val unit: Trampoline[Unit] = Done(())
  def apply[A](a: A): Trampoline[A] = Done(a)
  def lazyVal[A](a: => A): Trampoline[A] = FlatMapped(unit, { (u: Unit) => Done(a) })
  /**
   * Use this to call to another trampoline returning function
   * you break the effect of this if you directly recursively call a Trampoline
   * returning function
   */
  def call[A](layzee: => Trampoline[A]): Trampoline[A] = FlatMapped(unit, { (u: Unit) => layzee })
  implicit val ChainableCallbackCollectorBuilder: ChainableCallbackCollectorBuilder[Trampoline] = new ChainableCallbackCollectorBuilder[Trampoline] {
    def apply[A](a: A) = Done(a)
    def flatMap[A, B](start: Trampoline[A])(fn: A => Trampoline[B]) = start.flatMap(fn)
  }
  // This triggers evaluation. Will reevaluate every time. Prefer .get
  def run[A](tramp: Trampoline[A]): A = {
    @annotation.tailrec
    def loop(start: Trampoline[Any], stack: List[(Any) => Trampoline[Any]]): Any = {
      start match {
        case Done(a) => stack match {
          case next :: tail => loop(next(a), tail)
          case Nil => a
        }
        case FlatMapped(item, fn) => loop(item, fn :: stack)
      }
    }
    // Sorry for the cast, but it is tough to get the types right without a lot of wrapping
    loop(tramp, Nil).asInstanceOf[A]
  }
}
