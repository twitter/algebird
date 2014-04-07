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
package com.twitter.algebird.util.summer

import com.twitter.algebird._
import com.twitter.util.{Duration, Future}

/**
 * @author Ian O Connell
 */
trait AsyncSummer[Key, Value] { self =>
  def forceTick: Future[Map[Key, Value]]
  def tick: Future[Map[Key, Value]]
  def insert(vals: TraversableOnce[(Key, Value)]): Future[Map[Key, Value]]
  def cleanup: Future[Unit] = Future.Unit
  def withCleanup(cleanup: () => Future[Unit]) = {
    val oldSelf = self
    new AsyncSummerProxy[Key, Value] {
      override val self = oldSelf
      override def cleanup = {
        self.cleanup.flatMap { _ => cleanup }
      }
    }
  }
}

trait AsyncSummerProxy[Key, Value] extends AsyncSummer[Key, Value] {
  def self: AsyncSummer[Key, Value]
  def forceTick = self.forceTick
  def tick = self.tick
  def insert(vals: TraversableOnce[(Key, Value)]) = self.insert(vals)
  override def cleanup: Future[Unit] = self.cleanup
}


private[summer] trait WithFlushConditions[Key, Value] extends AsyncSummer[Key, Value] {
  protected var lastDump:Long = System.currentTimeMillis
  protected def softMemoryFlush: Float
  protected def flushFrequency: Duration

  protected def timedOut = (System.currentTimeMillis - lastDump) >= flushFrequency.inMilliseconds
  protected lazy val runtime  = Runtime.getRuntime

  protected def didFlush {lastDump = System.currentTimeMillis}

  protected def memoryWaterMark = {
    val used = ((runtime.totalMemory - runtime.freeMemory).toDouble * 100) / runtime.maxMemory
    used > softMemoryFlush
  }

  def tick: Future[Map[Key, Value]] = {
    if (timedOut || memoryWaterMark) {
          forceTick
      }
    else {
      Future.value(Map.empty)
    }
  }
}
