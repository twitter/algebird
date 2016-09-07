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

package com.twitter.algebird.statistics

import java.util.concurrent.atomic.AtomicLong

/**
 *  Counter abstraction that can optionally be thread safe
 *  @author Julien Le Dem
 */
private object Counter {
  def apply(threadSafe: Boolean): Counter = if (threadSafe) AtomicCounter() else PlainCounter()
}

private sealed trait Counter {
  def increment(): Unit
  def add(v: Long): Unit
  def get: Long
  def toDouble = get.toDouble
  override def toString = get.toString
}

/** thread safe */
private case class AtomicCounter() extends Counter {
  private[this] final val counter = new AtomicLong(0)
  override def increment() = counter.incrementAndGet
  override def add(v: Long) = counter.addAndGet(v)
  override def get = counter.get
}

/** not thread safe */
private case class PlainCounter() extends Counter {
  private[this] final var counter: Long = 0
  override def increment() = counter += 1
  override def add(v: Long) = counter += v
  override def get = counter
}