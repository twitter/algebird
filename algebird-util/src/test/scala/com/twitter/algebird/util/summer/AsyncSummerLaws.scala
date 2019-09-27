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

import org.scalacheck.{Arbitrary, Gen}
import com.twitter.algebird.{MapAlgebra, Semigroup}
import com.twitter.util.{Await, Duration, Future, FuturePool}

import java.util.concurrent.Executors

object AsyncSummerLaws {
  val executor = Executors.newFixedThreadPool(4)
  val workPool = FuturePool(executor)

  private[this] val schedulingExecutor = Executors.newFixedThreadPool(4)
  private[this] val schedulingWorkPool = FuturePool(schedulingExecutor)

  implicit def arbFlushFreq = Arbitrary {
    Gen
      .choose(1, 4000)
      .map { x: Int =>
        FlushFrequency(Duration.fromMilliseconds(x))
      }
  }

  implicit def arbBufferSize = Arbitrary {
    Gen
      .choose(1, 10)
      .map { x =>
        BufferSize(x)
      }
  }

  implicit def arbMemoryFlushPercent = Arbitrary {
    Gen
      .choose(80.0f, 90.0f)
      .map { x =>
        MemoryFlushPercent(x)
      }
  }

  implicit def arbCompactSize = Arbitrary {
    Gen
      .choose(1, 10)
      .map { x =>
        CompactionSize(x)
      }
  }

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  def summingWithAndWithoutSummerShouldMatch[K, V: Semigroup](
      asyncSummer: AsyncSummer[(K, V), Iterable[(K, V)]],
      inputs: List[List[(K, V)]]
  ) = {
    val reference = MapAlgebra.sumByKey(inputs.flatten)
    val resA = Await
      .result(Future.collect(inputs.map { i =>
        schedulingWorkPool {
          asyncSummer.addAll(i)
        }.flatMap(identity)
      }))
      .map(_.toList)
      .flatten
    val resB = Await.result(asyncSummer.flush)
    require(asyncSummer.isFlushed, "The flush should have ensured we are flushed now")

    val other = MapAlgebra.sumByKey(resA.toList ++ resB.toList)
    val res = Equiv[Map[K, V]].equiv(reference, other)
    res
  }

}
