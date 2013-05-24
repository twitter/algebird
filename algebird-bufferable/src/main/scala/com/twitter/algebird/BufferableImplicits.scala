package com.twitter.algebird

import com.twitter.bijection._

object BufferableImplicits {
  implicit val hllBijection = HyperLogLogBijection
  implicit val hllBufferable = Bufferable.viaBijection[HLL, Array[Byte]]
  implicit def qtreeBufferable[A](implicit bufferable : Bufferable[A], implicit monoid : Monoid[A]) = new QTreeBufferable
}