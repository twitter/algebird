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
package com.twitter.algebird.util

import com.twitter.algebird._
import com.twitter.util.{Future, Promise, Return}

/**
 * This Monoid allows code to depends on the results of asynchronous
 * computation. It is relatively common to have code which takes a
 * Monoid and elements, but applies the computation in an opaque way
 * (a cache, for example). This allows the code handing over the
 * elements (in this case, Tunnel objects) to depend on the result
 * of the Monoid's computation. Note that this code does not depend
 * on any particular Monoid -- that dependency is strictly when the Tunnel
 * objects are created. This is the async analogue of Function1Monoid.
 */
class TunnelMonoid[V] extends Monoid[Tunnel[V]] {
  def zero = {
    val promise = new Promise[V]
    Tunnel(promise, promise)
  }

  override def isNonZero(v: Tunnel[V]) = !(v.promise eq v.future)

  def plus(older: Tunnel[V], newer: Tunnel[V]): Tunnel[V] = {
    val (Tunnel(f1, p1), Tunnel(f2, p2)) = (older, newer)
    f2.foreach { Tunnel.properPromiseUpdate(p1, _) }
    Tunnel(f1, p2)
  }
}

/**
 * The tunnel class represents a piece of computation that depends on the
 * fulfilment of a promise. IMPORTANT: see apply, but Tunnels are mutable,
 * and can only be fulfilled once. They are generally not reusable. Reusing
 * a Tunnel in computation by a TunnelMonoid will cause the promise to be
 * fulfilled more than once which will most likely lead to errors.
 */
case class Tunnel[V](future: Future[V], promise: Promise[V]) {
  def willEqual(other: Tunnel[V]): Future[Boolean] =
    for {
      b1 <- future
      b2 <- other.future
    } yield b1 == b2

  /**
   * This takes in a value and updates the promise, fulfilling the chain
   * of futures which depends on this final promise. IMPORTANT: this can
   * only be called once. In this way, it is dangerous to reuse Tunnel
   * objects in Monoid code that might reuse objects.
   */
  def apply(v: V): Future[V] = {
    Tunnel.properPromiseUpdate(promise, v)
    future
  }
}

object Tunnel {
  implicit def monoid[V]: TunnelMonoid[V] = new TunnelMonoid[V]

  /**
   * This lifts a value into a Tunnel. This is where the Monoidic
   * computation underlying a TunnelMonoid actually happens.
   */
  def toIncrement[V](v: V)(implicit monoid: Monoid[V]) = {
    val promise = new Promise[V]
    Tunnel(promise.map { monoid.plus(_, v) }, promise)
  }

  /**
   * This attempts to fulfil the promise. If it has already been fulfilled,
   * this will throw an error if the value is different from the previous
   * value that was used.
   */
  def properPromiseUpdate[V](promise: Promise[V], newV: V): Unit =
    if (!promise.updateIfEmpty(Return(newV))) {
      promise.foreach { oldV =>
        assert(
          oldV == newV,
          "Cannot set a promise multiple times with different values."
            + " Old value: %s  New value: %s".format(oldV, newV))
      }
    }

  def from[V](fn: V => V): Tunnel[V] = {
    val prom = new Promise[V]
    Tunnel(prom.map(fn), prom)
  }
}
