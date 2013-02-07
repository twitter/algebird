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

/**
 * @author Oscar Boykin
 */

import java.util.concurrent.ArrayBlockingQueue

import java.util.{ LinkedHashMap => JLinkedHashMap, Map => JMap }
import scala.collection.mutable.{Map => MMap}
import scala.collection.JavaConverters._
import scala.annotation.tailrec

object SummingIterator {
  def apply[V](summer: StatefulSummer[V], it: Iterator[V]): SummingIterator[V] =
    new SummingIterator(summer, it)
}

class SummingIterator[V](summer: StatefulSummer[V], it: Iterator[V])
  extends java.io.Serializable with Iterator[V] {

  def hasNext: Boolean = it.hasNext || (summer.isFlushed == false)
  def next = nextInternal

  @tailrec
  private def nextInternal: V = {
    if(it.hasNext) {
      summer.put(it.next) match {
        case None => nextInternal
        case Some(v) => v
      }
    }
    else {
      // if you call nextInternal and we have no more to put, and no more to flush
      // this is an error
      summer.flush.get
    }
  }
}
