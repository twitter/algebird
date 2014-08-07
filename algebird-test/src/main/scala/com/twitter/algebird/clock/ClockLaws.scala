/*
Copyright 2014 Twitter, Inc.

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

package com.twitter.algebird.clock

import com.twitter.algebird._

import org.scalacheck.{ Arbitrary, Prop }
import org.scalacheck.Prop.forAll

object ClockLaws {
  import BaseProperties.{ commutativeBandLaws, partialSemigroupLaws }
  import SuccessibleLaws.successibleLaws

  def plusDoesNotDecrease[T: Clock: Arbitrary] = {
    val clock = implicitly[Clock[T]]
    def sg: Semigroup[T] = clock.semilattice
    def ord: PartialOrdering[T] = clock.partialOrdering
    forAll { (t1: T, t2: T) =>
      val t3 = sg.plus(t1, t2)
      ord.lteq(t1, t3) && ord.lteq(t2, t3)
    }
  }

  def partialSemiIsOrdered[C: Clock: Arbitrary, T: Semigroup: Arbitrary] = {
    val psg: PartialSemigroup[(C, T)] = Clock.notLTEQSemigroup[C, T]
    def ord: PartialOrdering[C] = implicitly[Clock[C]].successible.partialOrdering
    forAll { (ctl: (C, T), ctr: (C, T)) =>
      (psg.tryPlus(ctl, ctr) == None) == (ord.tryCompare(ctl._1, ctr._1) match {
        case None => false
        case Some(x) if x < 0 => false
        case _ => true
      })
    }
  }

  def notLTEQIsPartialSemigroup[C: Clock: Arbitrary, T: Semigroup: Arbitrary] = {
    implicit val psg: PartialSemigroup[(C, T)] = Clock.notLTEQSemigroup[C, T]
    partialSemigroupLaws[(C, T)]
  }
  def isGTIsPartialSemigroup[C: Clock: Arbitrary, T: Semigroup: Arbitrary] = {
    implicit val psg: PartialSemigroup[(C, T)] = Clock.isGTSemigroup[C, T]
    partialSemigroupLaws[(C, T)]
  }

  def apply[T: Clock: Arbitrary]: Prop = {
    val clock = implicitly[Clock[T]]
    implicit def sg: Semigroup[T] = clock.semilattice
    implicit def succ: Successible[T] = clock.successible
    commutativeBandLaws[T] && successibleLaws[T] && plusDoesNotDecrease[T]
  }
}
