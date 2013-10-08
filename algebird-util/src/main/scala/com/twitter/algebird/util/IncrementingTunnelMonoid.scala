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
import com.twitter.util.Promise

/**
 * This Monoid allows code to depend on the result of computation asynchronously.
 * This is a slightly less general version of the TunnelMonoid. See the documentation
 * for TunnelMonoid for general motivation. NOTE: the Promise will be fulfilled with
 * the value just before the PromiseLink is calculated.
 */
class IncrementingTunnelMonoid[V](monoid: Monoid[V]) extends Monoid[PromiseLink[V]] { //TODo(jcoveney) rename PromiseLink
	def zero = PromiseLink(new Promise, monoid.zero)

  def plus(older: PromiseLink[V], newer: PromiseLink[V]): PromiseLink[V] = {
    val (PromiseLink(p1, v1), PromiseLink(p2, v2)) = (older, newer)
    p2.foreach { futureV =>
      Tunnel.properPromiseUpdate(p1, monoid.plus(futureV, v2))
    }
    PromiseLink(p2, monoid.plus(v1, v2))
  }
}

/**
 * This class allows code to depends on the data that a value will be combined with,
 * fulfilling the Promise with the value just before the value is added in.
 */
case class PromiseLink[V](promise: Promise[V], value: V) {
  def completeWithStartingValue(startingV: V)(implicit monoid: Monoid[V]):V = {
    Tunnel.properPromiseUpdate(promise, value)
    monoid.plus(startingV, value)
  }
}

object PromiseLink {
  implicit def monoid[V](implicit innerMonoid: Monoid[V]): IncrementingTunnelMonoid[V] =
    new IncrementingTunnelMonoid[V](innerMonoid)

	def toPromiseLink[V](value:V) = PromiseLink(new Promise, value)
}