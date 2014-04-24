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
import com.twitter.util.{Duration, Future, FuturePool}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import scala.collection.mutable.{Set => MSet, ArrayBuffer}
import scala.collection.breakOut

/**
 * @author Ian O Connell
 */

class DynamicSummer[Key, Value](override val flushFrequency: FlushFrequency,
                              override val softMemoryFlush: MemoryFlushPercent,
                              backingSummer: AsyncSummer[(Key, Value), Iterable[(Key, Value)]])
                             (implicit sg: Semigroup[Value])
                              extends AsyncSummer[(Key, Value), Iterable[(Key, Value)]]
                              with WithFlushConditions[(Key, Value), Iterable[(Key, Value)]] {
  type T = (Key, Value)
  override def flush: Future[Iterable[(Key, Value)]] = backingSummer.flush
  override def isFlushed = backingSummer.isFlushed
  override val emptyResult = Seq[(Key, Value)]()
  private[this] final val monoid = CMS.monoid(4, 100, 2, 0.05)
  private[this] var cmsData: CMS = monoid.zero


  private def updateCMSTestMatch(t: Long): Boolean = {
    cmsData = monoid.plus(cmsData, monoid.create(t))
    cmsData.heavyHitters.contains(t)
  }

  def wrapTraversableOnce(t: Iterator[T]): (ArrayBuffer[T], Iterator[T]) = {
    val store = new ArrayBuffer[(Key, Value)]
    val iter = new Iterator[(Key, Value)] {
      var head: T = null
      override def hasNext: Boolean = {
        if(head != null) return true
        while(t.hasNext) {
          val tmp = t.next
          if(updateCMSTestMatch(tmp._1.hashCode)) {
            head = tmp
            return true
          } else {
            store += tmp
          }
        }
        false
      }
      def next = {
        if(hasNext) {
          val t = head
          head = null
          t
        } else {
          throw new Exception("Calling next on empty iterator")
        }
      }
    }
    (store, iter)
  }

  def addAll(vals: TraversableOnce[(Key, Value)]): Future[Iterable[T]] = {
    val (s, iter) = wrapTraversableOnce(vals.toIterator)
    backingSummer.addAll(iter.toTraversable).map { fResp =>
      fResp ++ s
    }
  }
}