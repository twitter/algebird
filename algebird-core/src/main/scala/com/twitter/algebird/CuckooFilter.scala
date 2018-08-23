package com.twitter.algebird

import algebra.BoundedSemilattice

import scala.collection.BitSet

/**
  * Cuckoo filter for Algebird in a Monoid way of life.
  * referring :
  *  - https://github.com/twitter/algebird/issues/560#issue-185161455
  *  - http://www.eecs.harvard.edu/%7Emichaelm/postscripts/cuckoo-conext2014.pdf
  *  - https://github.com/newhoggy/pico-cuckoo-filter/blob/master/pico-cuckoo-filter/src/main/scala/org/pico/cuckoo/filter/CuckooFilter.scala
  * By nature, this filter isn't commutative
  *
  **/


object CuckooFilter {
  // TODO : optimal parameter have to be compute here : see Asymptotic behavior of the original paper.
  def apply[A](bucketSize: Int, fingerPrintSize: LongBitSet, kicks: Int, buckets: Int)(
    implicit hash1: Hash128[A],
    hash2: Hash128[A]): CuckooFilterMonoid[A] = {
    null
  }
}

/**
  * The cuckoo filter monoid
  **/
// TODO : Add the fingerprint size.
case class CuckooFilterMonoid[A](fingerprintBucket: Int, maxKicks: Int = 3, totalBuckets: Int = 256)(
  implicit h1: Hash128[A])
  extends Monoid[CF[A]]
    with BoundedSemilattice[CF[A]] {
  override def zero: CF[A] = ???

  override def plus(x: CF[A], y: CF[A]): CF[A] = ???

  /**
    * Create a cuckoo filter with one item.
    */
  def create(item: A): BF[A] = ???

  /**
    * Create a cuckoo filter with multiple items.
    */
  def create(data: A*): BF[A] = create(data.iterator)

  /**
    * Create a cuckoo filter with multiple items from an iterator
    */
  def create(data: Iterator[A]): BF[A] = ???

}

/**
  * Cuckoo data structure abstract
  */
sealed abstract class CF[A] extends java.io.Serializable {

  def ++(other: CF[A]): CF[A]

  def +(other: A): CF[A]

  def bucketBits : Int

  def bucketIndex(hash : Long) : Int

  def checkAndAdd(item: A): (CF[A], ApproximateBoolean)

  def contains(item: A): ApproximateBoolean = {
    if (maybeContains(item)) {
      // Bloom filter stuff here
    }
    // That's the deal
    ApproximateBoolean.exactFalse
  }

  def maybeContains(item: A): Boolean

  def numBits: Int

  def width: Int

  def density = numBits.toDouble / width

  

  def size: Approximate[Long]
}

/**
  * Empty cuckoo filter
  **/
case class CFZero[A](fingerPrintBit :Int, fingerPrintBucket: Int, maxKicks: Int = 3, totalBuckets: Int = 256) extends CF[A] {

  override def checkAndAdd(item: A): (CF[A], ApproximateBoolean) = ???

  override def maybeContains(item: A): Boolean = ???

  override def numBits: Int = ???

  override def width: Int = ???

  override def size: Approximate[Long] = Approximate.zero

  override def ++(other: CF[A]): CF[A] = other

  override def +(other: A): CF[A] = new CFItem[A](other, new CFHash[A](), fingerPrintBit, fingerPrintBucket, maxKicks = maxKicks, totalBuckets = totalBuckets)

  override def bucketBits: Int = 0

  override def bucketIndex(hash: Long): Int = 0
}

/**
  * One item cuckoo
  **/

case class CFItem[A](item: A, cFHash: CFHash[A], fingerprintBucket : Int, fingerprintBit: Int, maxKicks: Int = 3, totalBuckets: Int = 256) extends CF[A] {

  override def checkAndAdd(item: A): (CF[A], ApproximateBoolean) = ???

  override def maybeContains(item: A): Boolean = ???

  override def numBits: Int = cFHash.generateFingerPrint(cFHash(item)(1))

  override def width: Int = ???

  override def size: Approximate[Long] = ???

  override def ++(other: CF[A]): CF[A] = ??? //CFInstance()
  override def +(other: A): CF[A] = this ++ CFItem[A](other,cFHash, fingerprintBucket, fingerprintBit, maxKicks, totalBuckets)

  override def bucketBits: Int = ???

  override def bucketIndex(hash: Long): Int = ???
}

/**
  * Multiple items cuckoo
  **/

case class CFInstance[A](hash: CFHash[A],
                         cuckooBitSet: Array[BitSet],
                         fingerprintBit : Int,
                         fingerprintBucket: Int,
                         totalBuckets: Int) extends CF[A] {

  override def checkAndAdd(item: A): (CF[A], ApproximateBoolean) = ???

  override def maybeContains(item: A): Boolean = ???

  override def numBits: Int = {
    cuckooBitSet.foldLeft(0)((z, b) => z + b.size)
  }

  override def width: Int = ???

  override def size: Approximate[Long] = ???

  override def ++(other: CF[A]): CF[A] = ???

  override def +(other: A): CFInstance[A] = {
    val firstHash = hash(1, other)
    val cuckooBS = Array(fingerprintBucket, LongBitSet.empty(fingerprintBucket * fingerprintBit))




    null
  }

  override def bucketBits: Int = fingerprintBit * fingerprintBucket

  override def bucketIndex(hash: Long): Int = ???
}


/**
  * The hash class for cuckoo
  **/
private[algebird] case class CFHash[A](fingerprintSize : Int)(implicit hash: Hash128[A]) {

  def apply(seed: Long, valueToHash: A): Array[Long] = {
    println(hash.hashWithSeed(seed, valueToHash))
    Array(hash.hashWithSeed(seed, valueToHash)._1, hash.hashWithSeed(seed, valueToHash)._2)
  }

  // TODO: size fo fingerprint goes here
  // took from https://github.com/efficient/cuckoofilter/blob/aac6569cf30f0dfcf39edec1799fc3f8d6f594da/src/cuckoofilter.h#L66
  def generateFingerPrint(f: Long ): Byte = {
    var fingerprint: Byte = (f & 1 << fingerprintSize).toByte
    //fingerprint +=  (fingerprint == 0).asInstanceOf[Byte]

    fingerprint
  }

}
