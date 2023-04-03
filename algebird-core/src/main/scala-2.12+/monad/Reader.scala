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

// TODO this is general, move somewhere better

// Reader Monad, represents a series of operations that mutate some environment
// type (the input to the function)

sealed trait Reader[-Env, +T] {
  def apply(env: Env): T
  def flatMap[E1 <: Env, U](next: T => Reader[E1, U]): Reader[E1, U] =
    FlatMappedReader[E1, T, U](this, next)
  def map[U](thatFn: T => U): Reader[Env, U] =
    FlatMappedReader(this, (t: T) => ConstantReader(thatFn(t)))
}

final case class ConstantReader[+T](get: T) extends Reader[Any, T] {
  override def apply(env: Any): T = get
  override def map[U](fn: T => U): ConstantReader[U] = ConstantReader(fn(get))
  override def flatMap[E1 <: Any, U](next: T => Reader[E1, U]): Reader[E1, U] =
    next(get)
}
final case class ReaderFn[E, +T](fn: E => T) extends Reader[E, T] {
  override def apply(env: E): T = fn(env)
}
final case class FlatMappedReader[E, U, +T](first: Reader[E, U], fn: U => Reader[E, T]) extends Reader[E, T] {
  override def apply(env: E): T = {
    @annotation.tailrec
    def loop(r: Reader[E, Any], stack: List[(Any) => Reader[E, Any]]): Any =
      r match {
        case ConstantReader(get) =>
          stack match {
            case head :: tail => loop(head(get), tail)
            case Nil          => get
          }
        case ReaderFn(fn) =>
          stack match {
            case head :: tail => loop(head(fn(env)), tail)
            case Nil          => fn(env)
          }
        case FlatMappedReader(first, nextFn) => loop(first, nextFn.asInstanceOf[Any => Reader[E, Any]] :: stack)
      }
    loop(first, List(fn.asInstanceOf[(Any) => Reader[E, Any]])).asInstanceOf[T]
  }
}

object Reader {
  def const[T](t: T): Reader[Any, T] = ConstantReader(t)
  implicit def apply[E, T](fn: (E) => T): Reader[E, T] = ReaderFn(fn)

  class ReaderM[Env] extends Monad[Reader[Env, _]] {
    override def apply[T](t: T): ConstantReader[T] = ConstantReader(t)
    override def flatMap[T, U](self: Reader[Env, T])(next: T => Reader[Env, U]): Reader[Env, U] =
      self.flatMap(next)
    override def map[T, U](self: Reader[Env, T])(fn: T => U): Reader[Env, U] = self.map(fn)
  }

  implicit def monad[Env]: Monad[Reader[Env, _]] = new ReaderM[Env]
}
