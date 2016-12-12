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

import org.scalacheck.{ Arbitrary, Prop }
import org.scalacheck.Prop.forAll

object PredecessibleLaws {
  private def descending[T: Predecessible](t: T, prev: Option[T]): Boolean =
    prev match {
      case None => true // t is the min
      case Some(p) =>
        val ord = implicitly[Predecessible[T]].ordering
        ord.lt(p, t)
    }

  // Should always be true:
  def law[T: Predecessible](t: T): Boolean = {
    val prev = Predecessible.prev(t)
    val prevPrev = Predecessible.prev(prev)
    descending(t, prev) && descending(t, prevPrev) && (prev match {
      case None => true
      case Some(p) => descending(p, prevPrev)
    })
  }

  def iteratePrevDecreases[T: Predecessible](t: T, size: Short): Boolean =
    Predecessible.iteratePrev(t).take(size.toInt).sliding(2).forall {
      case a :: b :: Nil => implicitly[Predecessible[T]].ordering.lt(b, a)
      case a :: Nil => true
      case s => sys.error("should never happen: " + s)
    }

  /**
   * Use this to test your implementations:
   *
   * {{{
   * property("MyType is predecessible") { predecessibleLaws[MyType] }
   * }}}
   */
  def predecessibleLaws[T: Predecessible: Arbitrary]: Prop =
    forAll { (t: T, size: Short) =>
      law(t) && iteratePrevDecreases(t, size)
    }

  @deprecated("Deprecated in favor of predecessibleLaws.", since = "0.12.3")
  def predessibleLaws[T: Predecessible: Arbitrary]: Prop = predecessibleLaws
}
