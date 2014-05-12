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

package com.twitter.algebird

import org.scalacheck.Arbitrary
import org.scalacheck.Gen._

/**
  * Generators useful in testing Interval
  */
object Generators {

  implicit def intervalArb[T:Arbitrary:Ordering]: Arbitrary[Interval[T]] =
    Arbitrary(oneOf(genUniverse, genEmpty, genInclusiveLower, genExclusiveLower, genInclusiveUpper, genExclusiveUpper, genIntersection))

  implicit def lowerIntArb[T:Arbitrary:Ordering]: Arbitrary[Lower[T]] =
    Arbitrary(oneOf(genInclusiveLower, genExclusiveLower))

  implicit def upperIntArb[T:Arbitrary:Ordering]: Arbitrary[Upper[T]] =
    Arbitrary(oneOf(genInclusiveUpper, genExclusiveUpper))

  implicit def intersectionArb[T:Arbitrary:Ordering]: Arbitrary[Interval.GenIntersection[T]] =
    Arbitrary(genIntersection)

  def genUniverse[T:Arbitrary:Ordering] =
      for {
        u <- Unit
      }
      yield Universe[T]()

  def genEmpty[T:Arbitrary:Ordering] =
      for {
        u <- Unit
      }
      yield Empty[T]()

  def genInclusiveLower[T:Arbitrary:Ordering] =
      for {
        l <- Arbitrary.arbitrary[T]
      } yield InclusiveLower(l)

  def genExclusiveLower[T:Arbitrary:Ordering] =
      for {
        l <- Arbitrary.arbitrary[T]
      } yield ExclusiveLower(l)

  def genInclusiveUpper[T:Arbitrary:Ordering] =
      for {
        u <- Arbitrary.arbitrary[T]
      } yield InclusiveUpper(u)

  def genExclusiveUpper[T:Arbitrary:Ordering] =
      for {
        u <- Arbitrary.arbitrary[T]
      } yield ExclusiveUpper(u)

  def genIntersection[T:Arbitrary:Ordering] =
      for {
        l <- Arbitrary.arbitrary[Lower[T]]
        u <- Arbitrary.arbitrary[Upper[T]] if l.intersects(u)
      } yield Intersection(l, u)
}
