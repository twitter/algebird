package com.twitter.algebird

import java.nio._

/**
 * Base class minhash signatures; LazyMinHashSignature and MinHashSignature.
 * The lazy minhash signature is used to store the values until the actual minhash signature is needed at
 * aggregation time.  This is to avoid GC overload the early by avoiding early allocation of many large
 * byte arrays.  Instead, only the values are stored using LazyMinHashWithValue, and we allocate the
 * buffer and pass it to LazyMinHashWithBytes.
 */
abstract class LazyMinHash extends java.io.Serializable
case class LazyMinHashWithValue(value: String) extends LazyMinHash
case class LazyMinHashWithBytes(bytes: Array[Byte]) extends LazyMinHash

/**
 * This is used to make minhash algorithm more scalable by deferring per-element minhash signature generation
 * to aggregation time.  The minhash byte array is created only when LazyMinHasher.plus()
 * is called and immediately thrown away after aggregation.  This reduces the burden on GC as well as
 * reducing the memory footprint significantly.
 * Since lazy minhashes are wrappers, we also made the lazy minhaser a wrapper of any minhasher type.
 */
class LazyMinHasher[H](minHasher: MinHasher[H])(implicit n: Numeric[H])
  extends Monoid[LazyMinHash] {
  /** Create a minhash signature for a single String value */
  def init(value: String): LazyMinHash = LazyMinHashWithValue(value)
  /** Signature for empty set, needed to be a proper Monoid */
  val zero: LazyMinHash = LazyMinHashWithBytes(minHasher.zero.bytes)

  /** Set union */
  override def plus(l: LazyMinHash, r: LazyMinHash): LazyMinHash = {
    (l, r) match {
      case (LazyMinHashWithBytes(l), LazyMinHashWithValue(r)) => {
        LazyMinHashWithBytes(minHasher.plus(MinHashSignature(l), minHasher.init(r)).bytes)
      }
      case (LazyMinHashWithValue(l), LazyMinHashWithBytes(r)) => {
        LazyMinHashWithBytes(minHasher.plus(minHasher.init(l), MinHashSignature(r)).bytes)
      }
      case (LazyMinHashWithBytes(l), LazyMinHashWithBytes(r)) => {
        LazyMinHashWithBytes(minHasher.plus(MinHashSignature(l), MinHashSignature(r)).bytes)
      }
      case (LazyMinHashWithValue(l), LazyMinHashWithValue(r)) => {
        LazyMinHashWithBytes(minHasher.plus(minHasher.init(l), minHasher.init(r)).bytes)
      }
      case _ => throw new Exception(s"Unhandled term types in plus operation for LazyMinHasher")
    }
  }

  def toMinHash(lazySig: LazyMinHash): MinHashSignature = {
    lazySig match {
      case LazyMinHashWithValue(k) => minHasher.init(k)
      case LazyMinHashWithBytes(b) => MinHashSignature(b)
      case _ => throw new Exception(s"Unhandled LazyMinHash type ${lazySig.getClass.getName}")
    }
  }

  /** Esimate Jaccard similarity (size of union / size of intersection) */
  def similarity(left: LazyMinHash, right: LazyMinHash): Double =
    minHasher.similarity(toMinHash(left), toMinHash(right))
}
