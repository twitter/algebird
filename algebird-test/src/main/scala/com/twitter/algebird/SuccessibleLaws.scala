/*
Copyright 2016 Twitter, Inc.

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

import org.scalacheck.{Arbitrary, Prop}
import org.scalacheck.Prop.forAll

object SuccessibleLaws {
  private def ascending[T: Successible](t: T, next: Option[T]): Boolean =
    next match {
      case None => true // t is the max
      case Some(n) =>
        val ord = implicitly[Successible[T]].ordering
        ord.lt(t, n)
    }

  // Should always be true:
  def law[T: Successible](t: T): Boolean = {
    val next = Successible.next(t)
    val nextNext = Successible.next(next)
    ascending(t, next) && ascending(t, nextNext) && (next match {
      case None    => true
      case Some(n) => ascending(n, nextNext)
    })
  }

  def iterateNextIncreases[T: Successible](t: T, size: Short): Boolean =
    Successible.iterateNext(t).take(size.toInt).sliding(2).forall {
      case a :: b :: Nil => implicitly[Successible[T]].ordering.lt(a, b)
      case _ :: Nil      => true
      case s             => sys.error("should never happen: " + s)
    }

  /**
   * Use this to test your implementations:
   *
   * {{{
   * property("MyType is successible") { successibleLaws[MyType] }
   * }}}
   */
  def successibleLaws[T: Successible: Arbitrary]: Prop =
    forAll((t: T, size: Short) => law(t) && iterateNextIncreases(t, size))
}
