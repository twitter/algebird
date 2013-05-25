package com.twitter.algebird

import com.twitter.bijection._
import java.nio._

class RichByteBuffer(into : ByteBuffer) {
  def reallocatingPut[T:Bufferable](value : T) : ByteBuffer = Bufferable.reallocatingPut(into){Bufferable.put(_,value)}
}

class ByteBufferReader(var from : ByteBuffer) {
  def get[T:Bufferable] = Bufferable.get(from).map {
    case (bb, t) => {
      from = bb
      t
    }
  }
}

trait BufferableImplicits {
  implicit val hllBijection = HyperLogLogBijection
  implicit val hllBufferable = Bufferable.viaBijection[HLL, Array[Byte]]
  implicit def qtreeBufferable[A](implicit bufferable : Bufferable[A], monoid : Monoid[A]) = new QTreeBufferable
  implicit def adaptiveVectorBufferable[A:Bufferable] = new AdaptiveVectorBufferable

  implicit def bb2RichBB(into : ByteBuffer) = new RichByteBuffer(into)
  def read[T](from : ByteBuffer)(fn : ByteBufferReader => Option[T]) = {
    val reader = new ByteBufferReader(from)
    fn(reader).map{(reader.from, _)}
  }
}

object BufferableImplicits extends BufferableImplicits