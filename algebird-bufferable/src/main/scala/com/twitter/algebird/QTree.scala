package com.twitter.algebird

import com.twitter.bijection._
import java.nio._

class QTreeBufferable[A](implicit bufferable : Bufferable[A], monoid : Monoid[A])
  extends AbstractBufferable[QTree[A]] with BufferableImplicits {
  implicit val recursive = this

  def put(into: ByteBuffer, tree : QTree[A]) = {
    into.reallocatingPut(QTree.unapply(tree).get)
  }

  def get(from: ByteBuffer) = read(from) {reader =>
    for((offset, level, count, sum, lowerChild, upperChild) <-
          reader.get[(Long, Int, Long, A, Option[QTree[A]], Option[QTree[A]])])
      yield new QTree(offset, level, count, sum, lowerChild, upperChild)
  }
}
