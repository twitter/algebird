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
import com.twitter.util.Future

/**
 * @author Ian O Connell
 */
class NullSummer[Key, Value](tuplesIn: Incrementor, tuplesOut: Incrementor)(implicit
    semigroup: Semigroup[Value]
) extends AsyncSummer[(Key, Value), Map[Key, Value]] {
  def flush: Future[Map[Key, Value]] = Future.value(Map.empty)
  def tick: Future[Map[Key, Value]] = Future.value(Map.empty)
  def addAll(vals: TraversableOnce[(Key, Value)]): Future[Map[Key, Value]] = {

    val r = Semigroup
      .sumOption(vals.map { inV =>
        tuplesIn.incr
        Map(inV)
      })
      .getOrElse(Map.empty)
    tuplesOut.incrBy(r.size)
    Future.value(r)
  }
  override val isFlushed: Boolean = true
}
