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

object SuccessibleProperties extends Properties("Successibles") {
  import BaseProperties._
  import Successible._

  def alwaysIncreasing[T: Successible](start: T) = {
    val r = new Random
    val incrementable = implicitly[Successible[T]]
    val ord = incrementable.ordering
    1.to(100).foldLeft((start, true)) { case ((oldVal, hasBeenGreater), _) =>
      val newVal = incrementable.next(oldVal)
      (newVal, hasBeenGreater && ord.compare(oldVal, newVal) < 0)
    }._2
  }

  def successibleLaws[T: Successible: Arbitrary](start: T) = alwaysIncreasing[T](start)

  property("Successible[Int] is a Successible") = successibleLaws[Int](0)
  property("Successible[Long] is a Successible") = successibleLaws[Long](0L)
}
