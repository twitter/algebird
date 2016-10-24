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
import com.twitter.util.{ Duration, Future }

/**
 * @author Ian O Connell
 */

trait AsyncSummer[T, +M <: Iterable[T]] { self =>
  def flush: Future[M]
  def tick: Future[M]
  def add(t: T) = addAll(Iterator(t))
  def addAll(vals: TraversableOnce[T]): Future[M]

  def isFlushed: Boolean
  def cleanup: Future[Unit] = Future.Unit
  def withCleanup(cleanup: () => Future[Unit]): AsyncSummer[T, M] = {
    val oldSelf = self
    new AsyncSummerProxy[T, M] {
      override val self = oldSelf
      override def cleanup = {
        oldSelf.cleanup.flatMap { _ => cleanup }
      }
    }
  }
}

trait AsyncSummerProxy[T, +M <: Iterable[T]] extends AsyncSummer[T, M] {
  def self: AsyncSummer[T, M]
  def flush = self.flush
  def tick = self.tick
  override def add(t: T) = self.add(t)
  def addAll(vals: TraversableOnce[T]) = self.addAll(vals)
  def isFlushed = self.isFlushed
  override def cleanup: Future[Unit] = self.cleanup
}

trait WithFlushConditions[T, M <: Iterable[T]] extends AsyncSummer[T, M] {
  private[this] val className = getClass.getName
  protected var lastDump: Long = System.currentTimeMillis
  protected def softMemoryFlush: MemoryFlushPercent
  protected def flushFrequency: FlushFrequency
  protected def emptyResult: M

  protected def memoryIncr: Incrementor
  protected def timeoutIncr: Incrementor

  protected def timedOut = (System.currentTimeMillis - lastDump) >= flushFrequency.v.inMilliseconds
  protected lazy val runtime = Runtime.getRuntime

  protected def memoryWaterMark = {
    val used = ((runtime.totalMemory - runtime.freeMemory).toDouble * 100) / runtime.maxMemory
    used > softMemoryFlush.v
  }

  def tick: Future[M] = {
    if (timedOut) {
      timeoutIncr.incr
      lastDump = System.currentTimeMillis // reset the timeout condition
      flush
    } else if (memoryWaterMark) {
      memoryIncr.incr
      lastDump = System.currentTimeMillis // reset the timeout condition
      flush
    } else {
      Future.value(emptyResult)
    }
  }
}
