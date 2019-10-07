package com.twitter.algebird

import java.nio.ByteBuffer

/**
 * A wrapper for `Array[Byte]` that provides sane implementations of `hashCode`, `equals`, and `toString`.
 * The wrapped array of bytes is assumed to be never modified.
 *
 * Note: Unfortunately we cannot make [[Bytes]] a value class because a value class may not override the `hashCode`
 * and `equals` methods (cf. SIP-15, criterion 4).
 *
 * =Alternatives=
 *
 * Instead of wrapping an `Array[Byte]` with this class you can also convert an `Array[Byte]` to a `Seq[Byte]` via
 * Scala's `toSeq` method:
 *
 * {{{
 * val arrayByte: Array[Byte] = Array(1.toByte)
 * val seqByte: Seq[Byte] = arrayByte.toSeq
 * }}}
 *
 * Like [[Bytes]], a `Seq[Byte]` has sane `hashCode`, `equals`, and `toString` implementations.
 *
 * Performance-wise we found that a `Seq[Byte]` is comparable to [[Bytes]].  For example, a `CMS[Seq[Byte]]` was
 * measured to be only slightly slower than `CMS[Bytes]` (think: single-digit percentages).
 *
 * @param array the wrapped array of bytes
 *
 * @see [[MinHasher]]
 */
final case class Bytes(array: Array[Byte]) extends java.io.Serializable {

  require(array != null, "array must not be null")

  override def hashCode: Int =
    scala.util.hashing.MurmurHash3.arrayHash(array, Bytes.HashSeed)

  /**
   * Implementation detail: This `equals` method is defined in terms of the wrapped array, which is a mutable field.
   * In general such a definition of `equals` is considered bad practice, but in our case we justify the use of a
   * mutable field because the contract of [[Bytes]] requires that the wrapped array must never be modified (and we
   * intentionally do not create a defensive, immutable copy because of performance considerations).
   */
  override def equals(that: Any): Boolean = that match {
    case Bytes(thatArray) => java.util.Arrays.equals(array, thatArray)
    case _                => false
  }

  override def toString: String =
    array.map(_.toString).mkString("Bytes(", ",", ")")

  def apply(idx: Int): Byte = array.apply(idx)

  def size: Int = array.size
}

object Bytes {

  private val HashSeed = 0

  implicit val ordering: Ordering[Bytes] = new Ordering[Bytes] {
    override def compare(a: Bytes, b: Bytes): Int =
      ByteBuffer.wrap(a.array).compareTo(ByteBuffer.wrap(b.array))
  }

}
