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
package scalacheck

import org.scalacheck.{ Arbitrary, Gen }
import org.scalacheck.Arbitrary.{ arbitrary => getArbitrary }
import Gen.oneOf

/**
 * Arbitrary instances for Algebird data structures.
 */
object arbitrary {
  import gen._

  implicit def firstArb[T: Arbitrary]: Arbitrary[First[T]] =
    Arbitrary(firstGen(getArbitrary[T]))

  implicit def lastArb[T: Arbitrary]: Arbitrary[Last[T]] =
    Arbitrary(lastGen(getArbitrary[T]))

  implicit def minArb[T: Arbitrary]: Arbitrary[Min[T]] =
    Arbitrary(getArbitrary[T].map(Min(_)))

  implicit def maxArb[T: Arbitrary]: Arbitrary[Max[T]] =
    Arbitrary(getArbitrary[T].map(Max(_)))

  implicit def orValArb: Arbitrary[OrVal] =
    Arbitrary(getArbitrary[Boolean].map(OrVal(_)))

  implicit def andValArb: Arbitrary[AndVal] =
    Arbitrary(getArbitrary[Boolean].map(AndVal(_)))

  implicit def intervalArb[T](implicit arb: Arbitrary[T], ord: Ordering[T]): Arbitrary[Interval[T]] =
    Arbitrary(
      oneOf(
        genUniverse[T], genEmpty[T],
        genInclusiveLower[T], genExclusiveLower[T],
        genInclusiveUpper[T], genExclusiveUpper[T],
        genIntersection[T]))

  implicit def lowerIntArb[T: Arbitrary: Ordering]: Arbitrary[Lower[T]] = Arbitrary(genLower)

  implicit def upperIntArb[T: Arbitrary: Ordering]: Arbitrary[Upper[T]] = Arbitrary(genUpper)

  implicit def intersectionArb[T: Arbitrary: Ordering]: Arbitrary[Interval.GenIntersection[T]] =
    Arbitrary(genIntersection)
}
