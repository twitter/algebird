/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package com.twitter.algebird

import algebra.BoundedSemilattice
import com.googlecode.javaewah.{IntIterator, EWAHCompressedBitmap => CBitSet}

import scala.util.Random


/**
  * Cuckoo filter for Algebird in a Monoid way of life.
  * referring :
  *  - https://github.com/twitter/algebird/issues/560#issue-185161455
  *  - http://www.eecs.harvard.edu/%7Emichaelm/postscripts/cuckoo-conext2014.pdf
  *  - https://github.com/irfansharif/cfilter
  * By nature, this filter isn't commutative
  *
  * From the inital paper, there is no problem to consider || fingerprint|| = ln(N) where N = fingerprintPerBucket * totalBucket
  * "" as long as we use reasonably sized buckets, the fingerprint size can remain small. "", we'll use a a 32 bits fingerprint.
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
case class CuckooFilterMonoid[A](fingerprintBucket: Int, totalBuckets: Int = 256)(
  implicit h1: Hash128[A])
  extends Monoid[CF[A]]
    with BoundedSemilattice[CF[A]] {

  val cFHash: CFHash[A] = CFHash[A](totalBuckets)

  def plus(left: CF[A], right: CF[A]): CF[A] = left ++ right

  val zero: CF[A] = CFZero(fingerprintBucket)

  /**
    * it's come from the BloomFilter. But I don't think it's a good idea because insertion from scratch
    * describe by the "set" and "add" functions are not safe. */
  override def sumOption(iter: TraversableOnce[CF[A]]): Option[CF[A]] =
    if (iter.isEmpty) None
    else {
      val buckets = Array.fill[CBitSet](totalBuckets)(new CBitSet(fingerprintBucket))
      var sets = 0

      @inline def setFingerprint(index: Int, fp: Int): Unit = {
        buckets(index).set(fp)
        sets += 1
      }

      var oneItem: CFItem[A] = null

      @inline def add(cfItem: CFItem[A]): Unit = {
        oneItem = cfItem
        val (h, k, fp) = cFHash(cfItem.item)
        setFingerprint(h, fp)
      }

      iter.foreach({
        case CFZero(_, _) => ()
        case cItem@CFItem(_, _, _, _) => add(cItem)
        case CFInstance(_, bitsets, _, _) => bitsets.zipWithIndex.foreach(e => {
          val iter = e._1.intIterator()
          while (iter.hasNext) {
            setFingerprint(e._2, iter.next())
          }
        })
      })

      if (sets == 0) Some(zero)
      else if (sets == 1) Some(oneItem)
      else {
        Some(CFInstance[A](cFHash, buckets, fingerprintBucket, totalBuckets))
      }
    }


  /**
    * Create a cuckoo filter with one item.
    */
  def create(item: A): CF[A] = CFItem[A](item, new CFHash[A](totalBuckets), fingerprintBucket, totalBuckets)

  /**
    * Create a cuckoo filter with multiple items.
    */
  def create(data: A*): CF[A] = create(data.iterator)

  /**
    * Create a cuckoo filter with multiple items from an iterator
    */
  def create(data: Iterator[A]): CF[A] = sum(data.map(CFItem(_, cFHash, fingerprintBucket, totalBuckets)))

}

object CF {
  implicit def equiv[A]: Equiv[CF[A]] =
    new Equiv[CF[A]] {
      override def equiv(x: CF[A], y: CF[A]): Boolean = {

        def toIntIt(c: CF[A]): IntIterator = c match {
          case CFItem(item, hash, _, _) =>
            new IntIterator {
              val (_, _, fp) = hash(item)
              var pos = 0

              override def hasNext: Boolean = pos < 1

              override def next(): Int = {
                pos += 1
                fp
              }
            }
          case CFInstance(_, bs, fpBck, _) =>
            new IntIterator {

              // IntIterator for a Array[CbitSet]
              var intIterators: Array[IntIterator] = bs.map(_.intIterator())

              override def hasNext: Boolean = {
                if (intIterators.isEmpty) return false

                intIterators.exists(_.hasNext)
              }

              override def next(): Int = {
                if (intIterators.isEmpty) throw new IllegalAccessException()

                intIterators = intIterators.filter(_.hasNext)
                val (iteratorHasNext, idx) = intIterators.zipWithIndex.find(e => e._1.hasNext)
                  .getOrElse(throw new IllegalAccessException())

                val nextVal = iteratorHasNext.next()
                intIterators.update(idx, iteratorHasNext)
                nextVal
              }
            }
          case CFZero(_, _) =>
            new IntIterator {
              def hasNext = false

              def next = sys.error("BFZero has no hashes set")
            }
        }

        def equiIntIter(a: IntIterator, b: IntIterator): Boolean = {
          while (a.hasNext && b.hasNext) {
            if (!(a.next() == b.next)) {
              return false
            }
          }
          a.hasNext == b.hasNext
        }

        val xit = toIntIt(x)
        val yit = toIntIt(y)

        (x eq y) || (x.bucketNumber == y.bucketNumber) &&
          (x.fingerprintBucket == y.fingerprintBucket) && equiIntIter(toIntIt(x), toIntIt(y))
      }
    }

}


/**
  * Cuckoo data structure abstract
  */
sealed abstract class CF[A] extends java.io.Serializable {

  val maxKicks = 256

  val bucketNumber: Int

  val fingerprintBucket: Int

  def ++(other: CF[A]): CF[A]

  def +(other: A): CF[A]

  def -(other: A): CF[A]

  def checkAndAdd(item: A): (CF[A], ApproximateBoolean)

  def contains(item: A): ApproximateBoolean = {
    if (maybeContains(item)) {
      // cuckoo checking here
    }
    // That's the deal
    ApproximateBoolean.exactFalse
  }

  def delete(item: A): Boolean

  def lookup(item: A): Boolean

  def maybeContains(item: A): Boolean

  def size: Approximate[Long]
}

/**
  * Empty cuckoo filter
  **/
case class CFZero[A](fingerPrintBucket: Int, totalBuckets: Int = 256)(implicit hash: Hash128[A]) extends CF[A] {

  override def checkAndAdd(item: A): (CF[A], ApproximateBoolean) = ???

  override def maybeContains(item: A): Boolean = ???

  override def size: Approximate[Long] = Approximate.zero

  override def ++(other: CF[A]): CF[A] = other

  override def +(other: A): CF[A] = new CFItem[A](other, new CFHash[A](totalBuckets), fingerPrintBucket, totalBuckets)

  override def lookup(elem: A): Boolean = false

  override def delete(item: A): Boolean = false

  override def -(other: A): CF[A] = this

  override val bucketNumber: Int = totalBuckets
  override val fingerprintBucket: Int = fingerPrintBucket
}

/**
  * One item cuckoo
  **/

case class CFItem[A](item: A, cFHash: CFHash[A], fingerPrintBucket: Int, totalBuckets: Int = 256)(implicit hashFingerprint: Hash128[A], hashFingerprintRaw: Hash128[Int]) extends CF[A] {

  override def checkAndAdd(item: A): (CF[A], ApproximateBoolean) = ???

  override def maybeContains(item: A): Boolean = ???

  override def size: Approximate[Long] = ???

  override def +(other: A): CF[A] = this ++ CFItem[A](other, cFHash, fingerPrintBucket, totalBuckets)

  def toInstance(): CFInstance[A] = {
    new CFInstance[A](cFHash, Array.fill[CBitSet](totalBuckets)(new CBitSet(fingerPrintBucket)), fingerPrintBucket, totalBuckets) + item
  }

  override def ++(other: CF[A]): CF[A] = other match {

    case CFZero(_, _) => this

    case cfInstance@CFInstance(_, _, _, _) => cfInstance + item

    case cfItem@CFItem(_, _, _, _) =>
      toInstance() + cfItem.item
  }

  override def lookup(elem: A): Boolean = elem == item

  override def delete(item: A): Boolean = false

  override def -(other: A): CF[A] = if (item == other) CFZero(fingerPrintBucket, totalBuckets) else this

  override val bucketNumber: Int = totalBuckets
  override val fingerprintBucket: Int = fingerPrintBucket
}

/**
  * Multiple items cuckoo
  **/
case class CFInstance[A](hash: CFHash[A],
                         cuckooBitSet: Array[CBitSet],
                         fingerPrintBucket: Int,
                         totalBuckets: Int)(implicit hashFingerprint: Hash128[A], hashFingerprintRaw: Hash128[Int]) extends CF[A] {


  override def checkAndAdd(item: A): (CF[A], ApproximateBoolean) = ???

  override def maybeContains(item: A): Boolean = ???

  override def size: Approximate[Long] = ???

  override def ++(other: CF[A]): CF[A] = {
    other match {
      case CFZero(_, _) => this
      case cfItem@CFItem(_, _, _, _) => this + cfItem.item
      case CFInstance(_, cuckooBit, fpBucket, totalBck) =>
        require(fpBucket == fingerprintBucket)
        require(totalBck == totalBuckets)
        CFInstance(hash, cuckooBitSet.zip(cuckooBit).map(e => e._1.or(e._2)), fingerprintBucket, totalBuckets)
    }
  }

  override def +(other: A): CFInstance[A] = {
    if (insert(other)) {
      return new CFInstance[A](hash, cuckooBitSet, fingerprintBucket, totalBuckets)
    }
    throw new RuntimeException("can't add element to a full cuckoo filter.")
  }

  private def insert(elem: A): Boolean = {
    val (h, k, fp) = hashes(elem)
    insertFingerprint(h, fp) || insertFingerprint(k, fp) || {
      // choose random index to start kick
      var index = if (Random.nextBoolean()) k else h
      for (n <- 0 until maxKicks) {
        val fingerprintKicked = swapRandomFingerprint(index, fp)
        // partial cuckoo hash key
        index = (index ^ hashFingerprint(fingerprintKicked)) % totalBuckets
        if (insertFingerprint(index, fingerprintKicked)) {
          return true
        }
      }
      return false
    }
  }

  def swapRandomFingerprint(bucketIndex: Int, fingerprint: Int): Int = {
    val bucket = cuckooBitSet(bucketIndex)
    if (bucket.cardinality() > 0) {
      var fingerprints: List[Int] = List()
      val it = bucket.intIterator()
      while (it.hasNext)
        fingerprints = fingerprints.::(it.next())
      val randomIndex = Random.nextInt(fingerprints.size)

      deleteFingerprint(bucketIndex, fingerprints(randomIndex))
      insertFingerprint(bucketIndex, fingerprint)

      return fingerprints(randomIndex)
    }
    fingerprint
  }

  private def deleteFingerprint(indexBucket: Int, fp: Int): Boolean = {
    val bucket = cuckooBitSet(indexBucket)
    if (bucket.isEmpty || isFingerprintInBuck(bucket, fp))
      return false
    val it = bucket.intIterator()
    val bitSet = new CBitSet(fingerprintBucket * 64 + 8)
    while (it.hasNext) {
      if (it.next() != fp)
        bitSet.set(it.next())
    }
    cuckooBitSet.update(indexBucket, bitSet)
    true
  }

  private def isFingerprintInBuck(bucket: CBitSet, fp: Int): Boolean = {
    val it = bucket.intIterator()
    while (it.hasNext)
      if (it.next() == fp) {
        return true
      }
    false
  }

  private def hashFingerprint(fp: Int): Int = {
    hashFingerprintRaw.hash(fp)._1.toInt & 0x7fffffff
  }

  private def insertFingerprint(index: Int, fp: Int): Boolean = {
    if (cuckooBitSet(index).cardinality() < fingerprintBucket) {
      cuckooBitSet(index).set(fp)
      return true
    }
    false
  }

  def hashes(elem: A): (Int, Int, Int) = {
    hash(elem)
  }


  override def lookup(elem: A): Boolean = {
    val (h, k, fp) = hashes(elem)
    isFingerprintInBuck(cuckooBitSet(h), fp) || isFingerprintInBuck(cuckooBitSet(k), fp)
  }

  override def -(other: A): CF[A] = {
    delete(other)
    new CFInstance[A](hash, cuckooBitSet, fingerprintBucket, totalBuckets)
  }

  override def delete(elem: A): Boolean = {
    val (h, k, fp) = hashes(elem)
    if (deleteFingerprint(h, fp) || deleteFingerprint(k, fp))
      return true
    false
  }

  override val bucketNumber: Int = totalBuckets
  override val fingerprintBucket: Int = fingerPrintBucket
}

/**
  * The hash class for cuckoo
  **/
private[algebird] case class CFHash[A](totalBuckets: Int)(implicit hash: Hash128[A]) {
  def apply(valueToHash: A): (Int, Int, Int) = {
    val fp = fingerprint(valueToHash)
    val h = hash.hash(valueToHash)._1.toInt & 0x7fffffff % totalBuckets
    val k = (fp ^ h) % totalBuckets
    (h, k, fp)
  }

  def fingerprint(value: A): Int = hash.hashWithSeed(fingerprintSeed, value)._1.toInt & 0x7fffffff

  def fingerprintSeed = 128

}
