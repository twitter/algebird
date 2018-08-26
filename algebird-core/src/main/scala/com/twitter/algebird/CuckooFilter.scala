package com.twitter.algebird

import algebra.BoundedSemilattice
import com.googlecode.javaewah.datastructure.BitSet
import scala.util.Random

/**
  * Cuckoo filter for Algebird in a Monoid way of life.
  * referring :
  *  - https://github.com/twitter/algebird/issues/560#issue-185161455
  *  - http://www.eecs.harvard.edu/%7Emichaelm/postscripts/cuckoo-conext2014.pdf
  *  - https://github.com/irfansharif/cfilter
  * By nature, this filter isn't commutative
  *
  **/


object CuckooFilter {
  // TODO : optimal parameter have to be compute here : see Asymptotic behavior of the original paper.
  def apply[A](bucketSize: Int, fingerPrintSize: LongBitSet, kicks: Int, buckets: Int)(
    implicit hash1: Hash128[A]): CuckooFilterMonoid[A] = {
    null
  }
}

/**
  * The cuckoo filter monoid
  **/
// TODO : Add the fingerprint size.
case class CuckooFilterMonoid[A](fingerPrintBit: Int, fingerprintBucket: Int, maxKicks: Int = 3, totalBuckets: Int = 256)(
  implicit h1: Hash128[A])
  extends Monoid[CF[A]]
    with BoundedSemilattice[CF[A]] {

  override def zero: CF[A] = CFZero(fingerPrintBit, fingerprintBucket)

  override def plus(left: CF[A], right: CF[A]): CF[A] = left ++ right

  /**
    * Create a cuckoo filter with one item.
    */
  def create(item: A): CF[A] = CFItem[A](item, new CFHash[A](), fingerprintBucket, fingerPrintBit, totalBuckets)

  /**
    * Create a cuckoo filter with multiple items.
    */
  def create(data: A*): CF[A] = create(data.iterator)

  /**
    * Create a cuckoo filter with multiple items from an iterator
    */
  def create(data: Iterator[A]): CF[A] = ???

}

/**
  * Cuckoo data structure abstract
  */
sealed abstract class CF[A] extends java.io.Serializable {

  def ++(other: CF[A]): CF[A]

  def +(other: A): CF[A]

  def bucketBits: Int

  def bucketIndex(hash: Long): Int

  def checkAndAdd(item: A): (CF[A], ApproximateBoolean)

  def contains(item: A): ApproximateBoolean = {
    if (maybeContains(item)) {
      // cuckoo checking here
    }
    // That's the deal
    ApproximateBoolean.exactFalse
  }

  def maybeContains(item: A): Boolean

  def numBits: Int

  val maxKicks = 256

  def size: Approximate[Long]
}

/**
  * Empty cuckoo filter
  **/
case class CFZero[A](fingerPrintBit: Int, fingerPrintBucket: Int, totalBuckets: Int = 256)(implicit hash: Hash128[A]) extends CF[A] {

  override def checkAndAdd(item: A): (CF[A], ApproximateBoolean) = ???

  override def maybeContains(item: A): Boolean = ???

  override def numBits: Int = 0

  override def size: Approximate[Long] = Approximate.zero

  override def ++(other: CF[A]): CF[A] = other

  override def +(other: A): CF[A] = new CFItem[A](other, new CFHash[A](), fingerPrintBit, fingerPrintBucket, totalBuckets = totalBuckets)

  override def bucketBits: Int = 0

  override def bucketIndex(hash: Long): Int = 0
}

/**
  * One item cuckoo
  **/

case class CFItem[A](item: A, cFHash: CFHash[A], fingerprintBucket: Int, fingerprintBit: Int, totalBuckets: Int = 256)(implicit hashFingerprint: Hash128[A], hashFingerprintRaw: Hash128[Long]) extends CF[A] {

  override def checkAndAdd(item: A): (CF[A], ApproximateBoolean) = ???

  override def maybeContains(item: A): Boolean = ???

  override def numBits: Int = ??? // cFHash.generateFingerPrint(cFHash(item)(1))


  override def size: Approximate[Long] = ???

  override def ++(other: CF[A]): CF[A] = {
    CFInstance(cFHash, Array.fill[BitSet](totalBuckets)(new BitSet(fingerprintBucket * fingerprintBit)), fingerprintBucket, fingerprintBit, totalBuckets)
  }

  override def +(other: A): CF[A] = this ++ CFItem[A](other, cFHash, fingerprintBucket, fingerprintBit, totalBuckets)

  override def bucketBits: Int = ???

  override def bucketIndex(hash: Long): Int = ???
}

/**
  * Multiple items cuckoo
  **/
case class CFInstance[A](hash: CFHash[A],
                         cuckooBitSet: Array[BitSet],
                         fingerprintBucket: Int,
                         fingerprintBit: Int,
                         totalBuckets: Int)(implicit hashFingerprint: Hash128[A], hashFingerprintRaw: Hash128[Long]) extends CF[A] {

  override def checkAndAdd(item: A): (CF[A], ApproximateBoolean) = ???

  override def maybeContains(item: A): Boolean = ???


  override def numBits: Int = {
    cuckooBitSet.foldLeft(0)((z, b) => z + b.size)
  }

  override def size: Approximate[Long] = ???

  override def ++(other: CF[A]): CF[A] = {
    other match {

      case CFZero(_, _, _) => this
    }
  }

  def swapFingerPrint(bucketIndex: Int, fingerprint: Long): Long = {
    println("swap fingerprint !")
    val bucket = cuckooBitSet(bucketIndex)
    if (bucket.empty()) {
      val randomIndex = Random.nextInt(bucket.size)
      val fingerRandom = List(bucket)(randomIndex)

      return 1L //fingerRandom
    }
    fingerprint
  }

  private def insert(elem: A): Boolean = {
    // TODO: type issue, too much .toInt in my taste
    println(s"elem : $elem")
    val fp = new Fingerprint[A]().apply(elem)
    val h = hash(1, elem) % totalBuckets
    val k = (fp ^ h) % totalBuckets
    insertFingerprint(h.toInt, fp) || insertFingerprint(k.toInt, fp) || {
      // choose random index to start kick
      var index = if (Random.nextBoolean()) k else h
      for (n <- 0 until maxKicks) {
        val fingerprintKicked = swapFingerPrint(index.toInt, fp)
        // partial cuckoo hash key
        index = index ^ hashFingerprintRaw.hash(fingerprintKicked)._1 % totalBuckets
        if (cuckooBitSet(index.toInt).size != fingerprintBucket) {
          // cuckooBitSet.update(index.toInt, cuckooBitSet(index.toInt) + fingerprintKicked.toInt)
          true
        }
      }
      false
    }
  }

  override def +(other: A): CFInstance[A] = {
    if (insert(other)) {
      return new CFInstance[A](hash, cuckooBitSet, fingerprintBucket, fingerprintBit, totalBuckets)
    }
    throw new RuntimeException("can't add element to a full cuckoo filter.")
  }


  private def deleteFingerprint(index: Int, fp: Byte): Unit = {
    cuckooBitSet(index).flip(fp.toInt)
  }

  private def insertFingerprint(index: Int, fp: Byte): Boolean = {
    // TODO : you can make it better
    if (cuckooBitSet(index).cardinality() < fingerprintBucket) {
      cuckooBitSet(index).set(fp)
      return true
    }

    false
  }

  override def bucketIndex(hash: Long): Int = ???

  override def bucketBits: Int = fingerprintBit * fingerprintBucket
}

// Let's be generic because the fingerprint have to be hashable
private[algebird] case class Fingerprint[A: Hash128]() {

  def seedFingerprint = 128

  def apply(elem: A)(implicit hash: Hash128[A]): Byte = {
    val hashed = hash.hashWithSeed(seedFingerprint, elem)._1
    val absolut = math.abs(hashed.toByte).toByte
    absolut
  }
}


/**
  * The hash class for cuckoo
  **/
private[algebird] case class CFHash[A]()(implicit hash: Hash128[A]) {

  def apply(seed: Long, valueToHash: A): Long = {

    val hashed = hash.hashWithSeed(seed, valueToHash)._1
    math.abs(hashed.toByte).toByte

  }

}
