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
import com.twitter.util.{Await, Future}

import scala.util.Random

object TunnelMonoidProperties {
  def testTunnelMonoid[I: Monoid, V: Monoid](
      makeRandomInput: Int => I,
      makeTunnel: I => V,
      collapseFinalValues: (V, Seq[V], I) => Seq[Future[I]]
  ): Boolean = {
    val r = new Random
    val numbers = (1 to 40).map(_ => makeRandomInput(r.nextInt))
    def helper(seeds: Seq[I], toFeed: I) = {
      val tunnels = seeds.map(makeTunnel)
      @annotation.tailrec
      def process(tunnels: Seq[V]): V = {
        val size = tunnels.size
        if (size > 2) {
          val (tun1, tun2) = tunnels.splitAt(r.nextInt(size - 2))
          val (of2, rest) = tun2.splitAt(2)
          process(tun1 ++ (Monoid.plus(of2.head, of2.tail.head) +: rest))
        } else if (size == 2) {
          Monoid.plus(tunnels.head, tunnels.tail.head)
        } else {
          tunnels.head
        }
      }
      collapseFinalValues(process(tunnels), tunnels, toFeed)
    }
    numbers.forall { _ =>
      val toFeed = makeRandomInput(r.nextInt)
      val finalResults = helper(numbers, toFeed).zip(helper(numbers, toFeed)).map {
        case (f1, f2) =>
          for {
            b1 <- f1
            b2 <- f2
          } yield b1 == b2
      }
      Await.result(Future.collect(finalResults).map(_.forall(identity)))
    }
  }
}

class TunnelMonoidPropertiesextends extends CheckProperties {

  import TunnelMonoidProperties._
  implicit val monoid: Monoid[Int] = new Monoid[Int] {
    val zero = 0
    def plus(older: Int, newer: Int): Int = older + newer
  }

  property("associative") {
    def makeTunnel(seed: Int) = Tunnel.toIncrement(seed)
    def collapseFinalValues(finalTunnel: Tunnel[Int], tunnels: Seq[Tunnel[Int]], toFeed: Int) =
      finalTunnel(toFeed) +: tunnels.map(_.future)

    testTunnelMonoid[Int, Tunnel[Int]](identity, makeTunnel, collapseFinalValues)
  }
}
