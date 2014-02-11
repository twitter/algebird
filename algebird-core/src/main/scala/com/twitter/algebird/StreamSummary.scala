package com.twitter.algebird

import scala.collection.immutable.SortedMap

object StreamSummary {
  /**
    * Construct StreamSummary with maximum size of m containing a single item.
    */
  def apply[T](m: Int, item: T): StreamSummary[T] = new SSOne(m, item)

  private[algebird] val ordering = Ordering.by[(_, (Long, Long)), (Long, Long)]{ case (item, (count, err)) => (-count, err) }
}

/**
  * Data structure used in the Space-Saving Algorithm to find the approximate most frequent and top-k elements.
  * The algorithm is described in "Efficient Computation of Frequent and Top-k Elements in Data Streams".
  * See here: www.cs.ucsb.edu/research/tech_reports/reports/2005-23.pdf
  * Note that the adaptation to hadoop and parallelization were not described in the article and have not been proven to be mathematically correct
  * or preserve the guarantees or benefits of the algorithm.
  */
sealed abstract class StreamSummary[T] private[algebird] () {
  import StreamSummary.ordering

  /**
    * Maximum number of counters to keep (parameter "m" in the research paper).
    */
  def capacity: Int

  /**
    * Current lowest value for count
    */
  def min: Long

  /**
    * Map of item to counter, where each counter consist of a observed count and possible over-estimation (error)
    */
  def counters: Map[T, (Long, Long)]

  def ++(other: StreamSummary[T]): StreamSummary[T]

  /**
    * returns the frequency estimate for the item
    */
  def frequency(item: T): Approximate[Long] = {
    val (count, err) = counters.getOrElse(item, (min, min))
    Approximate(count - err, count, count, 1.0)
  }

  /**
    * Get the elements that show up more than thres times.
    * Returns sorted in descending order: (item, Approximate[Long], guaranteed)
    */
  def mostFrequent(thres: Int): Seq[(T, Approximate[Long], Boolean)] =
    counters
      .iterator
      .filter { case (item, (count, err)) => count >= thres }
      .toList
      .sorted(ordering)
      .map { case (item, (count, err)) => (item, Approximate(count - err, count, count, 1.0), thres <= count - err) }

  /**
    * Get the top-k elements.
    * Returns sorted in descending order: (item, Approximate[Long], guaranteed)
    */
  def topK(k: Int): Seq[(T, Approximate[Long], Boolean)] = {
    require(k < capacity)
    val si = counters
      .toList
      .sorted(ordering)
      .iterator
    val siK = si.take(k).toList
    val countKPlus1 = if (si.hasNext) si.next()._2._1 else 0
    siK.map { case (item, (count, err)) => (item, Approximate(count - err, count, count, 1.0), countKPlus1 < count - err) }
  }

  /**
    * Check consistency with other StreamSummary, useful for testing.
    * Returns boolean indicating if they are consistent
    */
  def consistentWith(that: StreamSummary[T]): Boolean = 
    (counters.keys ++ that.counters.keys).forall{ item => (frequency(item) - that.frequency(item)) ~ 0 }
}

class SSZero[T] private[algebird] () extends StreamSummary[T] with Serializable {

  def capacity = -1

  def min = 0L

  def counters = Map[T, (Long, Long)]()

  def ++(other: StreamSummary[T]): StreamSummary[T] = other

}

class SSOne[T] private[algebird] (val capacity: Int, val item: T) extends StreamSummary[T] {
  require(capacity > 1)

  def min = 0L

  def counters = Map(item -> (1L, 1L))

  def ++(other: StreamSummary[T]): StreamSummary[T] = other match {
    case other: SSZero[_] => this
    case other: SSOne[_] => SSMany(this).add(other)
    case other: SSMany[_] => other.add(this)
  }

}

object SSMany {
  private[algebird] def apply[T](one: SSOne[T]): SSMany[T] = new SSMany(one.capacity, Map(one.item -> (1L, 0L)), SortedMap(1L -> Set(one.item)))
}

class SSMany[T] private (val capacity: Int, val counters: Map[T, (Long, Long)], private val bucketsOption: Option[SortedMap[Long, Set[T]]]) extends StreamSummary[T] {
  //assert(bucketsOption.forall(_.values.map(_.size).sum == counters.size))

  private[algebird] def this(capacity: Int, counters: Map[T, (Long, Long)]) = this(capacity, counters, None)

  private def this(capacity: Int, counters: Map[T, (Long, Long)], buckets: SortedMap[Long, Set[T]]) = this(capacity, counters, Some(buckets))

  lazy val buckets: SortedMap[Long, Set[T]] = bucketsOption match {
    case Some(buckets) => buckets
    case None => SortedMap[Long, Set[T]]() ++ counters.groupBy(_._2._1).mapValues(_.keySet)
  }

  private val exact: Boolean = counters.size < capacity

  lazy val min = if (counters.size < capacity) 0L else buckets.firstKey

  // item is already present and just needs to be bumped up one
  private def bump(item: T) = {
    val (count, err) = counters(item)
    val counters1 = counters + (item -> (count + 1L, err)) // increment by one
    val currBucket = buckets(count) // current bucket
    val buckets1 = {
      if (currBucket.size == 1) // delete current bucket since it will be empty
        buckets - count
      else // remove item from current bucket
        buckets + (count -> (currBucket - item))
    } + (count + 1L -> (buckets.getOrElse(count + 1L, Set()) + item))
    new SSMany(capacity, counters1, buckets1)
  }

  // lose one item to meet capacity constraint
  private def loseOne = {
    val firstBucket = buckets(buckets.firstKey)
    val itemToLose = firstBucket.head
    val counters1 = counters - itemToLose
    val buckets1 = if (firstBucket.size == 1)
      buckets - min
    else
      buckets + (min -> (firstBucket - itemToLose))
    new SSMany(capacity, counters1, buckets1)
  }

  // introduce new item
  private def introduce(item: T, count: Long, err: Long) = {
    val counters1 = counters + (item -> (count, err))
    val buckets1 = buckets + (count -> (buckets.getOrElse(count, Set()) + item))
    new SSMany(capacity, counters1, buckets1)
  }

  // add a single element
  private[algebird] def add(x: SSOne[T]): SSMany[T] = {
    require(x.capacity == capacity)
    if (counters.contains(x.item))
      bump(x.item)
    else
      (if (exact) this else this.loseOne).introduce(x.item, min + 1L, min)
  }

  // merge two stream summaries
  // defer the creation of buckets since more pairwise merges might be necessary and buckets are not used for those
  private def merge(x: SSMany[T]): SSMany[T] = {
    require(x.capacity == capacity)
    val counters1 = Map() ++
      (counters.keySet ++ x.counters.keySet)
      .toList
      .map { key =>
        val (count1, err1) = counters.getOrElse(key, (min, min))
        val (count2, err2) = x.counters.getOrElse(key, (x.min, x.min))
        (key -> (count1 + count2, err1 + err2))
      }
      .sorted(StreamSummary.ordering)
      .take(capacity)
    new SSMany(capacity, counters1)
  }

  def ++(other: StreamSummary[T]): StreamSummary[T] = other match {
    case other: SSZero[_] => this
    case other: SSOne[_] => add(other)
    case other: SSMany[_] => merge(other)
  }

}

class StreamSummaryMonoid[T] extends Monoid[StreamSummary[T]] {

  override val zero = new SSZero[T]()

  override def plus(x: StreamSummary[T], y: StreamSummary[T]): StreamSummary[T] = x ++ y

}
