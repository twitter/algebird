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
import com.twitter.util.{ Await, Future }
import scala.util.Random
import org.scalacheck.{ Arbitrary, Properties }
import scala.annotation.tailrec

object IncrementingTunnelMonoidProperties extends Properties("IncrementingTunnelMonoids") {
	property("associative") = {
    implicit val monoid = new Monoid[Int] {
      val zero = 0
      def plus(older:Int, newer:Int):Int = older + newer
    }
    val r = new Random
    val numbers = (1 to 40).map { _ => r.nextInt }
    def helper(seeds:Seq[Int], toFeed:Int) = {
      val tunnels = seeds.map { PromiseLink.toPromiseLink(_) }
      @tailrec
      def process(tunnels:Seq[PromiseLink[Int]]):PromiseLink[Int] = {
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
      val finalTunnel = process(tunnels)
      finalTunnel.completeWithStartingValue(toFeed)
      finalTunnel.promise +: tunnels.map { _.promise }
    }
    numbers.forall { num =>
      val finalResults = helper(numbers, num) zip helper(numbers, num) map {
        case (f1, f2) => for {
          b1 <- f1
          b2 <- f2
        } yield b1 == b2
      }
      Await.result(Future.collect(finalResults).map { _.forall(identity) })
    }
  }
}