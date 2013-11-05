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
package com.twitter.algebird

import java.util.Random

import org.scalacheck.{ Arbitrary, Properties }
import org.scalacheck.Prop.forAll

import scala.annotation.tailrec

object IncrementableProperties extends Properties("Incrementables") {
  import BaseProperties._
  import Incrementable._

  def alwaysIncreasing[T: Incrementable] = {
    val r = new Random
    val incrementable = implicitly[Incrementable[T]]
    val start = incrementable.zero
    val ord = incrementable.ordering
    1.to(100).foldLeft((start, true)) { case ((oldVal, hasBeenGreater), _) =>
      val newVal = incrementable.increment(oldVal)
      (newVal, hasBeenGreater && ord.compare(oldVal, newVal) < 0)
    }._2
  }

  def plusIsMultiplication[T](implicit incrementable: Incrementable[T], numeric: Numeric[T]) = {
    val r = new Random
    val num = numeric.fromInt(r.nextInt(1000) + 10)
    @tailrec
    def multipleIncrement(ret: T, i: T): T =
      if (numeric.compare(i, numeric.zero) == 0)
        ret
      else
        multipleIncrement(incrementable.increment(ret), numeric.minus(i, numeric.one))
    val start = incrementable.zero
    val unit = incrementable.one
    incrementable.plus(start, numeric.times(unit, num)) == multipleIncrement(start, num)
  }

  def incrementableLaws[T: Incrementable: Arbitrary] = monoidLaws[T] && alwaysIncreasing[T]
  def numericIncrementableLaws[T: Incrementable: Numeric: Arbitrary] = incrementableLaws[T] && plusIsMultiplication[T]

  property("Incrementable[Int] is an Incrementable") = numericIncrementableLaws[Int]
  property("Incrementable[Long] is an Incrementable") = numericIncrementableLaws[Long]
}
