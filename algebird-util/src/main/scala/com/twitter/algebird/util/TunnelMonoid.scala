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
import com.twitter.util.{ Future, Promise, Return}

class TunnelMonoid[V] extends Monoid[Tunnel[V]] {
	def zero = {
    val promise = new Promise[V]
    Tunnel(promise, promise)
  }

	def plus(older:Tunnel[V], newer:Tunnel[V]):Tunnel[V] = {
    val (Tunnel(f1, p1), Tunnel(f2, p2)) = (older, newer)
    f2.foreach { Tunnel.properPromiseUpdate(p1, _) }
    Tunnel(f1, p2)
  }
}

object Tunnel {
  implicit def monoid[V]:TunnelMonoid[V] = new TunnelMonoid[V]

	def toIncrement[V](v:V)(implicit monoid:Monoid[V]) = {
    val promise = new Promise[V]
    Tunnel(promise.map { monoid.plus(_, v) }, promise)
  }

  def properPromiseUpdate[V](promise:Promise[V], newV:V):Unit = {
    if (!promise.updateIfEmpty(Return(newV))) {
      promise.foreach { oldV => 
        assert(oldV == newV, "Cannot set a promise multiple times with different values."
          + " Old value: %s  New value: %s".format(oldV, newV))
      }
    }
  }
}

case class Tunnel[V](future:Future[V], promise:Promise[V]) {
  def willEqual(other:Tunnel[V]):Future[Boolean] =
    for {
      b1 <- future
      b2 <- other.future
    } yield b1 == b2

  def apply(v:V):Future[V] = {
    Tunnel.properPromiseUpdate(promise, v)
    future
  }
}