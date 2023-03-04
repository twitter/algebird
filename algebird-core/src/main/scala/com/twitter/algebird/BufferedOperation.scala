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

import scala.collection.mutable.ArrayBuffer

/**
 * Represents something that consumes I and may emit O. Has some internal state that may be used to improve
 * performance. Generally used to model folds or reduces (see BufferedReduce)
 */
trait Buffered[I, O] extends java.io.Serializable {
  def put(i: I): Option[O]
  def flush: Option[O]
  def isFlushed: Boolean
}

abstract class ArrayBufferedOperation[I, O](size: Int) extends Buffered[I, O] {
  def operate(nonEmpty: Seq[I]): O

  require(size > 0, "buffer <= 0 not allowed")

  private val buffer = new ArrayBuffer[I](size)

  override def put(item: I): Option[O] = {
    buffer += item
    if (buffer.size >= size) flush
    else None
  }

  override def flush: Option[O] =
    if (buffer.isEmpty) None
    else {
      val res = operate(buffer.toSeq)
      buffer.clear()
      Some(res)
    }

  override def isFlushed: Boolean = buffer.isEmpty
}

object ArrayBufferedOperation {

  /**
   * Returns an ArrayBufferedOperation instance that internally uses the `sumOption` implementation of the
   * supplied Semigroup[T]
   */
  def fromSumOption[T](size: Int)(implicit sg: Semigroup[T]): BufferedReduce[T] =
    new ArrayBufferedOperation[T, T](size) with BufferedReduce[T] {
      // calling `.get is okay because the interface guarantees a
      // non-empty sequence.
      override def operate(items: Seq[T]): T = sg.sumOption(items.iterator).get
    }
}

/**
 * This never emits on put, you must call flush designed to be use in the stackable pattern with
 * ArrayBufferedOperation
 */
trait BufferedReduce[V] extends Buffered[V, V] {
  abstract override def put(item: V): Option[V] = {
    val res = super.put(item)
    // avoiding closures for performance critical code:
    if (res.isDefined) put(res.get)
    else None
  }
}
