/*
Copyright 2018 Stripe

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

import scala.collection.immutable.Queue
import Operators._

/**
 *
 * Convenience case class defined with a monoid for aggregating elements over
 * a finite window.
 *
 * @param total Known running total of `T`
 * @param items queue of known trailing elements.
 *
 *  Example usage:
 *
 *  case class W28[T](window: Window[T]) {
 *    def total = this.window.total
 *    def items = this.window.items
 *    def size = this.window.size
 *  }
 *
 *  object W28 {
 *    val windowSize = 28
 *    def apply[T](v: T): W28[T] = W28[T](Window(v))
 *
 *    implicit def w28Monoid[T](implicit p: Priority[Group[T], Monoid[T]]): Monoid[W28[T]] =
 *      new Monoid[W28[T]] {
 *        private val WT: Monoid[Window[T]] = WindowMonoid[T](windowSize)
 *        def zero = W28[T](WT.zero)
 *        def plus(a: W28[T], b: W28[T]): W28[T] =
 *          W28[T](WT.plus(a.window, b.window))
 *      }
 *  }
 *  val elements = getElements()
 *
 *  val trailing90Totals =
 *    elements
 *      .map{ W90 }
 *      .scanLeft(W90(0)) { (a, b) => a + b }
 *      .map{ _.total }
 */

case class Window[T](total: T, items: Queue[T]) {
  def size = this.items.size
}

object Window {
  def apply[T](v: T): Window[T] = Window[T](v, Queue[T](v))
  def from[T](ts: Iterable[T])(implicit m: WindowMonoid[T]) = m.fromIterable(ts)
}

/**
 * Provides a natural monoid for combining windows truncated to some window size.
 *
 * @param windowSize Upper limit of the number of items in a window.
 */


case class WindowMonoid[T](
    windowSize: Int
)(implicit p: Priority[Group[T], Monoid[T]])
    extends Monoid[Window[T]] {

  require(windowSize >= 1, "Windows must have positive sizes")

  def zero = p.fold(g => Window(g.zero))(m => Window(m.zero))

  def plus(a: Window[T], b: Window[T]): Window[T] =
    p.fold(g => plusG(a, b)(g))(m => plusM(a, b)(m))

  def plusG(a: Window[T], b: Window[T])(implicit g: Group[T]): Window[T] =
    if (b.items.size >= windowSize) {
      var total: T = b.total
      var q = b.items
      while (q.size > windowSize) {
        total = total - q.head
        q = q.tail
      }
      Window(total, q)
    } else {
      // we need windowSize - b.items.size from `a`
      var truncA = a.items
      var totalA = a.total
      val truncTo = windowSize - b.size
      while (truncA.size > truncTo) {
        totalA = totalA - truncA.head
        truncA = truncA.tail
      }
      val items = truncA ++ b.items
      val total = totalA + b.total
      Window(total, items)
    }

  def plusM(a: Window[T], b: Window[T])(implicit m: Monoid[T]): Window[T] =
    if (b.items.size >= windowSize) {
      val items = b.items.takeRight(windowSize)
      val total = m.sum(items)
      Window(total, items)
    } else {
      // we need windowSize - b.items.size from `a`
      val fromA = a.items.takeRight(windowSize - b.items.size)
      val items = fromA ++ b.items
      val total = m.sum(fromA) + b.total
      Window(total, items)
    }

  override def sumOption(ws: TraversableOnce[Window[T]]): Option[Window[T]] =
    if (ws.isEmpty) None
    else {
      val it = ws.toIterator
      var queue = Queue.empty[T]
      while (it.hasNext) {
        queue = (queue ++ it.next.items).takeRight(windowSize)
      }
      val monT: Monoid[T] = p.join
      Some(Window(monT.sum(queue), queue))
    }

  def fromIterable(ts: Iterable[T]): Window[T] = {
    val monT: Monoid[T] = p.join
    val right = ts.toList.takeRight(windowSize)
    val total = monT.sum(right)
    Window(total, Queue(right: _*))
  }
}
