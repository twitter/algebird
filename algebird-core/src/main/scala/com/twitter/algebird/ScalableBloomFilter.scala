package com.twitter.algebird

import com.googlecode.javaewah.{ EWAHCompressedBitmap => CBitSet }
import scala.collection.mutable.{ BitSet => MBitSet }

object ScalableBloomFilter {

  /**
   * The default parameter values for this implementation are based on the findings in "Scalable Bloom Filters",
   * Almeida, Baquero, et al.: http://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf
   */
  def apply(fpProb: Double,
    initialCapacity: Int,
    growthRate: Int = 2,
    tighteningRatio: Double = 0.9) = {
    new ScalableBloomFilter(fpProb, initialCapacity, growthRate, tighteningRatio,
      List(BloomFilter(initialCapacity, fpProb).create()))
  }
}

/**
 * Scalable Bloom Filter - a scalable collection of [[BloomFilterMonoid]] that approximately
 * maintains its false probability rate as it grows.
 *
 * @param fpProb - desired false probability rate
 * @param headCapacity - expected number of insertions in the initial filter after which a new filter is added
 * @param growthRate - factor by which each additional bloom filter is larger than the previous
 * @param tighteningRatio - how much "tighter" the fpProb must be for each successive filter added
 * @param filters - the underlying [[BF]] collection
 */
case class ScalableBloomFilter(fpProb: Double,
  headCapacity: Int,
  growthRate: Int,
  tighteningRatio: Double,
  filters: List[BF]) extends java.io.Serializable {

  /**
   * Add a single item to the SBF.  If this increases the estimated number of items
   * beyond the current filter capacity, a new filter will be created.
   */
  def +(item: String) = {
    val head = filters.head + item

    if (head.size.estimate >= headCapacity) {
      val newFpProb = fpProb * tighteningRatio
      val newCapacity = headCapacity * growthRate
      val newFilter = BloomFilter(newCapacity, newFpProb).create()
      new ScalableBloomFilter(newFpProb, newCapacity, growthRate, tighteningRatio, newFilter :: head :: filters.tail)
    } else {
      new ScalableBloomFilter(fpProb, headCapacity, growthRate, tighteningRatio, head :: filters.tail)
    }
  }

  private def filterForItems(items: Iterator[String], hashes: BFHash, width: Int) = {
    val setBits = MBitSet()
    // set the bits in order for the underlying EWAHCompressedBitmap
    items.foreach {
      hashes(_).foreach {
        setBits += _
      }
    }
    val compressedBitmap = new CBitSet()
    setBits.foreach(compressedBitmap.set)
    new BFSparse(hashes, compressedBitmap, width)
  }

  /**
   * Batch populate the SBF.  This performs better than iteratively adding single items with +.
   */
  def ++(itemsIterator: Iterator[String]) = {
    var currentFilters = filters
    var head = filters.head
    var currentFpProb = fpProb
    var currentCapacity = headCapacity
    while (itemsIterator.nonEmpty) {
      val chunk = itemsIterator.take(currentCapacity - head.size.estimate.toInt)
      val newFilter = filterForItems(chunk, head.hashes, head.width)
      currentFilters = (head ++ newFilter) :: currentFilters.tail
      if (itemsIterator.nonEmpty) {
        currentFpProb *= tighteningRatio
        currentCapacity *= growthRate
        head = BloomFilter(currentCapacity, currentFpProb).create()
        currentFilters ::= head
      }
    }
    new ScalableBloomFilter(currentFpProb, currentCapacity, growthRate, tighteningRatio, currentFilters)
  }

  def contains(item: String) = filters.exists(_.contains(item).isTrue)

  def count = filters.size

  def size = filters.map(_.size).reduce((b, a) => a + b)
}
