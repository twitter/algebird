package com.twitter.algebird

import com.twitter.bijection._
import java.nio._

class QTreeBufferable[A](implicit bufferable : Bufferable[A], monoid : Monoid[A]) extends AbstractBufferable[QTree[A]] {
  implicit val recursive = this

  def put(into: ByteBuffer, tree : QTree[A]) = {
    Bufferable.reallocatingPut(into){Bufferable.put(_, QTree.unapply(tree).get)}
  }

  def get(from: ByteBuffer) = {
    Bufferable.get[(Long, Int, Long, A, Option[QTree[A]], Option[QTree[A]])](from).map {
      case (bb, (offset, level, count, sum, lowerChild, upperChild)) =>
        (bb, new QTree(offset, level, count, sum, lowerChild, upperChild))
    }
  }
}
