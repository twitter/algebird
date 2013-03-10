package com.twitter.algebird

import java.nio.ByteBuffer

import com.twitter.algebird.hash.{Hashable, Murmur}

/**
  * Instances of MinHasher can create, combine, and compare fixed-sized signatures of
  * arbitrarily sized sets.
  *
  * A signature is represented by a byte array of approx maxBytes size.
  * You can initialize a signature with a single element, usually a Long or String.
  * You can combine any two set's signatures to produce the signature of their union.
  * You can compare any two set's signatures to estimate their jaccard similarity.
  * You can use a set's signature to estimate the number of distinct values in the set.
  * You can also use a combination of the above to estimate the size of the intersection of
  * two sets from their signatures.
  * The more bytes in the signature, the more accurate all of the above will be.
  *
  * You can also use these signatures to quickly find similar sets without doing
  * n^2 comparisons. Each signature is assigned to several buckets; sets whose signatures
  * end up in the same bucket are likely to be similar. The targetThreshold controls
  * the desired level of similarity - the higher the threshold, the more efficiently
  * you can find all the similar sets.
  *
  * This abstract superclass is generic with regards to the size of the hash used.
  * Depending on the number of unique values in the domain of the sets, you may want
  * a MinHasher16, a MinHasher32, or a new custom subclass.
  *
  * This implementation is modeled after Chapter 3 of Ullman and Rajaraman's Mining of Massive Datasets:
  * http://infolab.stanford.edu/~ullman/mmds/ch3a.pdf
  *
  * TODO: make a wrapper class, MinHashSignature, so this Monoid is not on a raw type (Array[Byte]).
**/
abstract class MinHasher[H](targetThreshold : Double, maxBytes : Int)(implicit n : Numeric[H]) extends Monoid[Array[Byte]] {
  /** the number of bytes used for each hash in the signature */
  def hashSize : Int

  /** For explanation of the "bands" and "rows" see Ullman and Rajaraman */
  val numBands = pickBands(targetThreshold, maxBytes / hashSize)
  val numRows = maxBytes / numBands / hashSize
  val numHashes = numRows * numBands
  val numBytes = numHashes * hashSize

  /** This seed could be anything */
  val seed = 123456789

  /** We always use a 128 bit hash function, so the number of hash functions is different
    * (and usually smaller) than the number of hashes in the signature.
  **/
  val hashFunctions: Seq[Hashable[ByteBuffer, (Long,Long)]] = {
    val r = new scala.util.Random(seed)
    val numHashFunctions = math.ceil(numBytes / 16.0).toInt
    (1 to numHashFunctions).map{i => Murmur.hash3(r.nextLong)}
  }

  /** Signature for empty set, needed to be a proper Monoid */
  val zero = buildArray{maxHash}

  /** Set union */
  def plus(left : Array[Byte], right : Array[Byte]) = {
    buildArray(left, right){(l,r) => n.min(l, r)}
  }

  /** Esimate jaccard similarity (size of union / size of intersection) */
  def similarity(left : Array[Byte], right : Array[Byte]) = {
    val matching = buildArray(left,right){(l,r) => if(l == r) n.one else n.zero}
    matching.map{_.toDouble}.sum / numHashes
  }

  /** Bucket keys to use for quickly finding other similar items via locality sensitive hashing
   *
   * Check other signatures who collide in at least one bucket for high similarity
   * Put another way: sigs: Iterable[MinHashSignature] =>
   *   sigs.flatMap(s => buckets(s).map { (_, s)} )
   *     .groupBy { _._1 }
   *     .mapValues { candidates: Iterable[(Long, MinHashSignature)] =>
   *       val sigs = candidates.view.map { _._2 }
   *       for(c1 <- sigs;
   *           c2 <- sigs;
   *           if Ordering[MinHashSignature].lt(c1, c2)
   *           if jaccardSimilarity(c1, c2) >= query)
   *        yield (c1, c2)
   *     }
   *  But you probably want to do the above calculation in map-reduce
   */
  def buckets(sig : Array[Byte]): Seq[Long] = {
    sig.grouped(numRows*hashSize).toList.map{band =>
      val (long1, long2) = hashFunctions.head(ByteBuffer.wrap(band))
      long1
    }
  }

  /** Create a signature for a single Long value */
  def init(value : Long) : Array[Byte] = init {
    val buf = ByteBuffer.allocate(8)
    buf.putLong(value)
    buf.rewind
    buf
  }

  /** Create a signature for a single String value */
  def init(value : String) : Array[Byte]= init(ByteBuffer.wrap(value.getBytes))

  /** Create a signature for an arbitrary value */
  def init(serialized: ByteBuffer) : Array[Byte] = {
    val bytes = new Array[Byte](numBytes)
    val buffer = ByteBuffer.allocate(hashFunctions.size * 16)
    val longBuffer = buffer.asLongBuffer
    hashFunctions.foreach{h =>
      val (long1, long2) = h(serialized)
      longBuffer.put(long1)
      longBuffer.put(long2)
    }
    buffer.rewind
    buffer.get(bytes)
    bytes
  }

  /** useful for understanding the effects of numBands and numRows
   *
   * For the LSH, the probability of becoming a candidate a sigmoid with max
   * slope at this value of jaccard similarity,
   * put another way, this is where d(probabilityOfInclusion(s))/ds is at its max
   */
  val estimatedThreshold = math.pow(1.0/numBands, 1.0/numRows)

  /** useful for understanding the effects of numBands and numRows
   *
   * This is the probability that two MinHashSignature objects will end up in at least
   * one of the same buckets for consideration GIVEN that their true similarity
   * is sim
   */
  def probabilityOfInclusion(sim : Double) = 1.0 - math.pow(1.0 - math.pow(sim, numRows), numBands)

  /** numerically solve the inverse of estimatedThreshold, given numBands*numRows */
  def pickBands(threshold : Double, hashes : Int) = {
    val target = hashes * -1 * math.log(threshold)
    var bands = 1
    while(bands * math.log(bands) < target)
      bands += 1
    bands
  }

  /** Maximum value the hash can take on (not 2*hashSize because of signed types) */
  def maxHash : H

  /** Initialize a byte array by generating hash values */
  def buildArray(fn: => H) : Array[Byte]

  /** Decode two signatures into hash values, combine them somehow, and produce a new array */
  def buildArray(left : Array[Byte], right : Array[Byte])(fn: (H,H) => H) : Array[Byte]
}

class MinHasher32(t : Double, n : Int) extends MinHasher[Int](t,n) {
  def hashSize = 4
  def maxHash = Int.MaxValue
  def buildArray(fn: => Int) : Array[Byte] = {
    val byteBuffer = ByteBuffer.allocate(numBytes)
    val writeBuffer = byteBuffer.asIntBuffer
    1.to(numHashes).foreach{i => writeBuffer.put(fn)}
    byteBuffer.array
  }

  def buildArray(left : Array[Byte], right : Array[Byte])(fn: (Int,Int) => Int) : Array[Byte] = {
    val leftBuffer = ByteBuffer.wrap(left).asIntBuffer
    val rightBuffer = ByteBuffer.wrap(right).asIntBuffer
    buildArray{fn(leftBuffer.get, rightBuffer.get)}
  }

  /** seems to work, but experimental and not generic yet */
  def approxCount(sig : Array[Byte]) = {
    val buffer = ByteBuffer.wrap(sig).asIntBuffer
    val mean = 1.to(numHashes).map{i => buffer.get.toLong}.sum / numHashes
    (2L << 31) / (mean.toLong + (2L << 30))
  }
}

class MinHasher16(t : Double, n : Int) extends MinHasher[Char](t,n) {
  def hashSize = 2
  def maxHash = Char.MaxValue
  def buildArray(fn: => Char) : Array[Byte] = {
    val byteBuffer = ByteBuffer.allocate(numBytes)
    val writeBuffer = byteBuffer.asCharBuffer
    1.to(numHashes).foreach{i => writeBuffer.put(fn)}
    byteBuffer.array
  }

  def buildArray(left : Array[Byte], right : Array[Byte])(fn: (Char,Char) => Char) : Array[Byte] = {
    val leftBuffer = ByteBuffer.wrap(left).asCharBuffer
    val rightBuffer = ByteBuffer.wrap(right).asCharBuffer
    buildArray{fn(leftBuffer.get, rightBuffer.get)}
  }
}
