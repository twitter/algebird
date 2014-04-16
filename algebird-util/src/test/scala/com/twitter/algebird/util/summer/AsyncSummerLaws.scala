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

package com.twitter.algebird.util.summer

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._


import com.twitter.algebird.{MapAlgebra, Semigroup}
import com.twitter.util.{Future, Await, Duration, FuturePool}

import java.util.concurrent.Executors


object AsyncSummerLaws {
  val executor = Executors.newFixedThreadPool(4)
  val workPool = FuturePool(executor)

  implicit def arbFlushFreq = Arbitrary {
         Gen.choose(1, 4000)
            .map { x: Int => FlushFrequency(Duration.fromMilliseconds(x)) }
      }

  implicit def arbBufferSize = Arbitrary {
         Gen.choose(1, 10)
            .map { x => BufferSize(x) }
  }

  implicit def arbMemoryFlushPercent = Arbitrary {
         Gen.choose(80.0f, 90.0f)
            .map { x => MemoryFlushPercent(x) }
  }

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  def summingWithAndWithoutSummerShouldMatch[K, V: Semigroup](asyncSummer: AsyncSummer[(K, V), Map[K, V]], inputs: List[List[(K, V)]]) = {
    val reference = MapAlgebra.sumByKey(inputs.flatten)
    val resA = Await.result(Future.collect(inputs.map(asyncSummer.addAll(_)))).map(_.toList).flatten
    val resB = Await.result(asyncSummer.flush)
    require(asyncSummer.isFlushed, "The flush should have ensured we are flushed now")

    val other = MapAlgebra.sumByKey(resA.toList ++ resB.toList)
    val res = Equiv[Map[K, V]].equiv(
      reference,
      other
    )
    if(!res) {
      println("Different keys:")
      println("Only in reference:")
      (reference.keys.toSet -- other.keys.toSet).foreach(println(_))
      println("Only in calc..")
      (other.keys.toSet -- reference.keys.toSet).foreach(println(_))
      println("key\trefValue\tcalculated Value:")
      reference.foreach {case (k, refV) =>
        other.get(k) match {
          case Some(calcV) if(calcV != refV) => println("%s\t%d\t%s".format(k, refV, other.get(k)))
          case None => println("%s\t%d\t%s".format(k, refV, other.get(k)))
          case _ => ()
        }
      }
    }
    res
  }

}
