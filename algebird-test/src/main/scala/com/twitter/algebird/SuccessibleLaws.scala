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

object SuccessibleLaws {
  // Should always be true:
  def law[T: Successible](t: T): Boolean =
    Successible.next(t) match {
      case None => true // t is the max
      case Some(n) =>
        val pord = implicitly[Successible[T]].partialOrdering
        pord.lt(t, n)
    }

  def iterateNextIncreases[T: Successible](t: T, size: Short): Boolean =
    Successible.iterateNext(t).take(size.toInt).sliding(2).forall {
      case a :: b :: Nil => implicitly[Successible[T]].partialOrdering.lt(a, b)
      case a :: Nil => true
      case s => sys.error("should never happen: " + s)
    }

  /**
   * Use this to test your implementations:
   *
   * {{{
   * property("blah is successible") { successibleLaws[MyType] }
   * }}}
   */
  def successibleLaws[T: Successible: Arbitrary]: Prop =
    forAll { (t: T, size: Short) =>
      law(t) && iterateNextIncreases(t, size)
    }
}
