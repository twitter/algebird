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

/**
 *
 * Convenience case class defined with a monoid for aggregating elements over
 * a finite window.
 *
 * @param total Known running total of `T`
 * @param items queue of known trailing elements.
 */

case class Window[T](total: T, items: Queue[T]) {
  def size = this.items.size
}

object Window {
  def apply[T](v: T): Window[T] = Window[T](v, Queue[T](v))
  def from[T](ts: Traversable[T])(implicit m: WindowMonoid[T]) = m.fromTraversable(ts)
}

/**
 * Provides a natural monoid for combining windows truncated to some window size.
 *
 * @param windowSize
 */


case class WindowMonoid[T](
    windowSize: Int
)(implicit p: Priority[Group[T], Monoid[T]])
    extends Monoid[Window[T]] {
  val zero =
    p match {
      case Priority.Preferred(g) => Window(g.zero)
      case Priority.Fallback(m)  => Window(m.zero)
    }

  def plus(a: Window[T], b: Window[T]): Window[T] =
    p match {
      case Priority.Preferred(g) => plusG(a, b)(g)
      case Priority.Fallback(m)  => plusM(a, b)(m)
    }

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
      val fromA = a.items.takeRight(windowSize - b.items.size)
      val res = fromA ++ b.items
      val total = g.sum(fromA) + b.total
      Window(total, res)
    }

  def plusM(a: Window[T], b: Window[T])(implicit m: Monoid[T]): Window[T] =
    if (b.items.size >= windowSize) {
      var q = b.items
      while (q.size > windowSize) {
        q = q.tail
      }
      val total = m.sum(q)
      Window(total, q)
    } else {
      // we need windowSize - b.items.size from `a`
      val fromA = a.items.takeRight(windowSize - b.items.size)
      val res = fromA ++ b.items
      val total = m.sum(fromA) + b.total
      Window(total, res)
    }

    def fromTraversable(ts: Traversable[T]): Window[T] = {
      val monT: Monoid[T] = p.join
      val right = ts.toList.takeRight(windowSize)
      val total = monT.sum(right)
      Window(total, Queue(right: _*))
    }

    override def sumOption(ws: TraversableOnce[Window[T]]): Option[Window[T]] = {
      if(ws.isEmpty) None
      else {
        val it = ws.toIterator
        var queue = Queue[T]()
        while (it.hasNext) {
          queue = (queue ++ it.next.items).takeRight(windowSize)
        }
        Some(fromTraversable(queue))
      }
    }
}

/*
  Example usage:

  case class W90[T](window: Window[T]) {
    def total = this.window.total
  }

  object W90 {
    def apply[T](v: T): W90[T] = W90[T](new Window(v))

    implicit def w90Monoid[T: Monoid]: Monoid[W90[T]] = new Monoid[W90[T]] {
      private val WT: Monoid[Window[T]] = WindowMonoid[T](90)
      def zero = W90[T](WT.zero)
      def plus(a: W90[T], b: W90[T]): W90[T] =
        W90[T](WT.plus(a.window, b.window))
    }
  }

  val elements = getElements()

  val trailing90Totals =
    elements
      .map{ W90 ( _ ) }
      .foldLeft(W90(0)) { (a, b) => a + b }
      .map{ _.total }
 */
