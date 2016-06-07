package com.twitter.algebird

sealed abstract class Batched[T] {
  def sum(implicit sg: Semigroup[T]): T
  private[algebird] def size: Int
}

object Batched {
  /**
   * this is a *reversed non-empty list*
   */
  private[algebird] case class Item[T](get: T) extends Batched[T] {
    def size = 1
    def sum(implicit sg: Semigroup[T]): T = get
  }
  private[algebird] case class Items[T](left: Batched[T], right: Batched[T]) extends Batched[T] {
    val size = left.size + right.size
    def sum(implicit sg: Semigroup[T]): T = sg.sumOption(iterator).get

    private[this] def iterator: Iterator[T] =
      new ItemsIterator(this)

    private class ItemsIterator[A](root: Batched[A]) extends Iterator[A] {
      var it: Iterator[A] = Iterator.empty
      var stack: List[Batched[A]] = Nil

      descend(root)

      def ascend(): Unit =
        if (it.hasNext) () else {
          stack match {
            case Nil =>
              ()
            case h :: t =>
              stack = t
              descend(h)
          }
        }

      @annotation.tailrec private def descend(v: Batched[A]): Unit =
        v match {
          case Items(lhs, rhs) =>
            stack = rhs :: stack
            descend(lhs)
          case Item(value) =>
            it = Iterator(value)
        }

      def hasNext(): Boolean =
        if (it.hasNext) true else {
          ascend()
          it.hasNext
        }

      def next(): A =
        if (it.hasNext) it.next else {
          ascend()
          it.next
        }
    }
  }
}

class BatchedCommutativeSemigroup[T](batchSize: Int,
  sg: Semigroup[T]) extends Semigroup[Batched[T]] {
  require(batchSize > 0, s"Batch size must be > 0, found: $batchSize")

  def plus(a: Batched[T], b: Batched[T]): Batched[T] = {
    val next = Batched.Items(a, b)
    if (next.size < batchSize) next
    else Batched.Item(next.sum(sg))
  }
}

