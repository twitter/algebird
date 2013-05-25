package com.twitter.algebird

import com.twitter.bijection._
import java.nio._

class AdaptiveVectorBufferable[A](implicit bufferable : Bufferable[A])
  extends AbstractBufferable[AdaptiveVector[A]] with BufferableImplicits {

  val DENSE : Byte = 0
  val SPARSE : Byte = 1

  def put(into: ByteBuffer, vector : AdaptiveVector[A]) = {
    vector match {
      case DenseVector(seq, sparseValue, denseCount) => {
        into
          .reallocatingPut(DENSE)
          .reallocatingPut((seq, sparseValue, denseCount))
      }
      case SparseVector(map, sparseValue, size) => {
        into
          .reallocatingPut(SPARSE)
          .reallocatingPut((map, sparseValue, size))
      }
    }
  }

  def get(from: ByteBuffer) = read(from) { reader =>
    reader.get[Byte].flatMap {
      case DENSE => reader.get[(Vector[A], A, Int)].map {
        case (seq, sparseValue, denseCount) => DenseVector(seq, sparseValue, denseCount)
      }
      case SPARSE => reader.get[(Map[Int,A], A, Int)].map {
        case (map, sparseValue, size) => SparseVector(map, sparseValue, size)
      }
    }
  }
}