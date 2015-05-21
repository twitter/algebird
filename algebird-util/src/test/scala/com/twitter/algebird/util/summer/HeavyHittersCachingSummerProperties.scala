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

import com.twitter.algebird.CheckProperties
import org.scalacheck.Prop._

class HeavyHittersCachingSummerProperties extends CheckProperties {
  import com.twitter.algebird.util.summer.AsyncSummerLaws._

  property("Summing with and without the summer should match") {
    forAll { (inputs: List[List[(Int, Long)]],
      flushFrequency: FlushFrequency,
      bufferSize: BufferSize,
      memoryFlushPercent: MemoryFlushPercent) =>
      val timeOutCounter = Counter("timeOut")
      val sizeCounter = Counter("size")
      val memoryCounter = Counter("memory")
      val insertOp = Counter("insertOp")
      val insertFails = Counter("insertFails")
      val tuplesIn = Counter("tuplesIn")
      val tuplesOut = Counter("tuplesOut")

      val summer = new AsyncListSum[Int, Long](bufferSize,
        flushFrequency,
        memoryFlushPercent,
        memoryCounter,
        timeOutCounter,
        insertOp,
        insertFails,
        sizeCounter,
        tuplesIn,
        tuplesOut,
        workPool,
        Compact(false),
        CompactionSize(0))
      val heavyHittersCachingSummer = HeavyHittersCachingSummer[Int, Long](flushFrequency,
        memoryFlushPercent,
        memoryCounter,
        timeOutCounter,
        tuplesOut,
        insertOp,
        sizeCounter,
        summer)
      (summingWithAndWithoutSummerShouldMatch(heavyHittersCachingSummer, inputs))
    }
  }

}
