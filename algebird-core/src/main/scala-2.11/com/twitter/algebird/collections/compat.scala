package com.twitter.algebird.collections

import scala.collection.immutable

private[algebird] object compat {
  implicit class VectorExtensions(private val fact: immutable.Vector.type) extends AnyVal {
    def from[T](source: TraversableOnce[T]): immutable.Vector[T] = {
      val builder = immutable.Vector.newBuilder[T]
      builder ++= source
      builder.result()
    }
  }
}
