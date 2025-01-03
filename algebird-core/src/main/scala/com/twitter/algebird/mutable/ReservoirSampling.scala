package com.twitter.algebird.mutable

import com.twitter.algebird.{Monoid, MonoidAggregator}

import scala.collection.mutable
import scala.util.Random

/**
 * A reservoir of the currently sampled items.
 *
 * @param capacity
 *   the reservoir capacity
 * @tparam T
 *   the element type
 */
sealed abstract class Reservoir[T](val capacity: Int) {
  require(capacity > 0, "reservoir size must be positive")

  def size: Int
  def isEmpty: Boolean = size == 0
  def isFull: Boolean = size == capacity

  // When the reservoir is full, w is the threshold for accepting an element into the reservoir, and
  // the following invariant holds: The maximum score of the elements in the reservoir is w,
  // and the remaining elements are distributed as U[0, w].
  // Scores are not kept explicitly, only their distribution is tracked and sampled from.
  // (w = 1 when the reservoir is not full.)
  def w: Double = 1

  private[algebird] def toSeq: mutable.IndexedSeq[T]

  /**
   * Add an element to the reservoir. If the reservoir is full then the element will replace a random element
   * in the reservoir, and the threshold <pre>w</pre> is updated.
   *
   * When adding multiple elements, [[append]] should be used to take advantage of exponential jumps.
   *
   * @param x
   *   the element to add
   * @param rng
   *   the random source
   */
  private[algebird] def accept(x: T, rng: Random): Reservoir[T]

  // The number of items to skip before accepting the next item is geometrically distributed
  // with probability of success w / prior. The prior will be 1 when adding to a single reservoir,
  // but when merging reservoirs it will be the threshold of the reservoir being pulled from,
  // and in this case we require that w < prior.
  private def nextAcceptTime(rng: Random, prior: Double): Int =
    (-rng.self.nextExponential / Math.log1p(-w / prior)).toInt

  /**
   * Add multiple elements to the reservoir.
   *
   * @param xs
   *   the elements to add
   * @param rng
   *   the random source
   * @param prior
   *   the threshold of the elements being added, such that the added element's value is distributed as
   *   <pre>U[0, prior]</pre>
   * @return
   *   this reservoir
   */
  def append(xs: TraversableOnce[T], rng: Random, prior: Double = 1.0): Reservoir[T] =
    xs match {
      case seq: IndexedSeq[T] => Reservoir.append(this, seq, rng, prior)
      case _                  => Reservoir.append(this, xs, rng, prior)
    }
}

private[algebird] case class Empty[T](k: Int) extends Reservoir[T](k) {
  override val size: Int = 0
  override val isEmpty: Boolean = true
  override val isFull: Boolean = false

  override def toSeq: mutable.IndexedSeq[T] = mutable.IndexedSeq()

  override def accept(x: T, rng: Random): Reservoir[T] =
    Singleton(k, x, if (capacity == 1) rng.nextDouble else 1)

  override def toString: String = s"Empty($capacity)"
}

// A reservoir with one element (but possibly higher capacity), optimizing the common case of sampling a
// single element.
private[algebird] case class Singleton[T](k: Int, x: T, w1: Double) extends Reservoir[T](k) {
  override val size: Int = 1
  override val isEmpty: Boolean = false
  override val isFull: Boolean = capacity == 1
  override val w: Double = w1

  override def toSeq: mutable.IndexedSeq[T] = mutable.IndexedSeq(x)

  override def accept(y: T, rng: Random): Reservoir[T] =
    if (isFull)
      Singleton(k, y, w * rng.nextDouble)
    else
      new ArrayReservoir(k).accept(x, rng).accept(y, rng)

  override def toString: String = s"Singleton($capacity, $w, $x)"
}

// A reservoir backed by a mutable buffer - will mutate by aggregation!
private[algebird] class ArrayReservoir[T](val k: Int) extends Reservoir[T](k) {
  private val reservoir: mutable.ArrayBuffer[T] = new mutable.ArrayBuffer(k)
  private var w1: Double = 1
  private val kInv: Double = 1d / capacity

  override def size: Int = reservoir.size
  override def w: Double = w1
  override def toSeq: mutable.IndexedSeq[T] = reservoir

  override def accept(x: T, rng: Random): Reservoir[T] = {
    if (isFull) {
      reservoir(rng.nextInt(capacity)) = x
    } else {
      reservoir.append(x)
    }
    if (isFull) {
      w1 *= Math.pow(rng.nextDouble, kInv)
    }
    this
  }

  override def toString: String = s"ArrayReservoir($capacity, $w, ${reservoir.toList})"
}

object Reservoir {
  def apply[T](capacity: Int): Reservoir[T] = Empty(capacity)

  private[algebird] def append[T](
      self: Reservoir[T],
      xs: TraversableOnce[T],
      rng: Random,
      prior: Double
  ): Reservoir[T] = {
    var res = self
    var skip = if (res.isFull) res.nextAcceptTime(rng, prior) else 0
    for (x <- xs) {
      if (!res.isFull) {
        // keep adding until reservoir is full
        res = res.accept(x, rng)
        if (res.isFull) {
          skip = res.nextAcceptTime(rng, prior)
        }
      } else if (skip > 0) {
        skip -= 1
      } else {
        res = res.accept(x, rng)
        skip = res.nextAcceptTime(rng, prior)
      }
    }
    res
  }

  /**
   * Add multiple elements to the reservoir. This overload is optimized for indexed sequences, where we can
   * skip over multiple indexes without accessing the elements.
   *
   * @param xs
   *   the elements to add
   * @param rng
   *   the random source
   * @param prior
   *   the threshold of the elements being added, such that the added element's value is distributed as
   *   <pre>U[0, prior]</pre>
   * @return
   *   this reservoir
   */
  private[algebird] def append[T](
      self: Reservoir[T],
      xs: IndexedSeq[T],
      rng: Random,
      prior: Double
  ): Reservoir[T] = {
    var res = self
    var i = xs.size.min(res.capacity - res.size)
    for (j <- 0 until i) {
      res = res.accept(xs(j), rng)
    }

    val end = xs.size
    assert(res.isFull || i == end)
    while (i >= 0 && i < end) {
      i += res.nextAcceptTime(rng, prior)
      // the addition can overflow, in which case i < 0
      if (i >= 0 && i < end) {
        // element enters the reservoir
        res = res.accept(xs(i), rng)
        i += 1
      }
    }
    res
  }
}

/**
 * This is the "Algorithm L" reservoir sampling algorithm [1], with modifications to act as a monoid by
 * merging reservoirs.
 *
 * [1] Kim-Hung Li, "Reservoir-Sampling Algorithms of Time Complexity O(n(1+log(N/n)))", 1994
 *
 * @tparam T
 *   the item type
 */
class ReservoirMonoid[T](val capacity: Int)(implicit val randomSupplier: () => Random)
    extends Monoid[Reservoir[T]] {

  override def zero: Reservoir[T] = Reservoir(capacity)
  override def isNonZero(r: Reservoir[T]): Boolean = !r.isEmpty

  /**
   * Merge two reservoirs. NOTE: This mutates one or both of the reservoirs. They should not be used after
   * this operation, except when using the return value for further aggregation.
   */
  override def plus(left: Reservoir[T], right: Reservoir[T]): Reservoir[T] =
    if (left.isEmpty) right
    else if (left.size + right.size <= left.capacity) {
      // the sum of the sizes is less than the reservoir size, so we can just merge
      left.append(right.toSeq, randomSupplier())
    } else {
      val (s1, s2) = if (left.w < right.w) (left, right) else (right, left)
      val xs = s2.toSeq
      val rng = randomSupplier()
      if (s2.isFull) {
        assert(s1.isFull)
        // The highest score in s2 is w, and the other scores are distributed as U[0, w].
        // Since s1.w < s2.w, we have to drop the single (sampled) element with the highest score
        // unconditionally. The other elements enter the reservoir with probability s1.w / s2.w.
        val i = rng.nextInt(xs.size)
        xs(i) = xs.head
        s1.append(xs.drop(1), rng, s2.w)
      } else {
        s1.append(xs, rng)
      }
    }
}

/**
 * An aggregator that uses reservoir sampling to sample k elements from a stream of items. Because the
 * reservoir is mutable, it is a good idea to copy the result to an immutable view before using it, as is done
 * by [[ReservoirSamplingToListAggregator]].
 *
 * @param capacity
 *   the number of elements to sample
 * @param randomSupplier
 *   the random generator
 * @tparam T
 *   the item type
 * @tparam C
 *   the result type
 */
abstract class ReservoirSamplingAggregator[T, +C](capacity: Int)(implicit val randomSupplier: () => Random)
    extends MonoidAggregator[T, Reservoir[T], C] {
  override val monoid: ReservoirMonoid[T] = new ReservoirMonoid(capacity)
  override def prepare(x: T): Reservoir[T] = Reservoir(capacity).accept(x, randomSupplier())

  override def apply(xs: TraversableOnce[T]): C = present(agg(xs))

  override def applyOption(inputs: TraversableOnce[T]): Option[C] =
    if (inputs.isEmpty) None else Some(apply(inputs))

  override def append(r: Reservoir[T], t: T): Reservoir[T] = r.append(Seq(t), randomSupplier())

  override def appendAll(r: Reservoir[T], xs: TraversableOnce[T]): Reservoir[T] =
    r.append(xs, randomSupplier())

  override def appendAll(xs: TraversableOnce[T]): Reservoir[T] = agg(xs)

  private def agg(xs: TraversableOnce[T]): Reservoir[T] =
    appendAll(monoid.zero, xs)
}

class ReservoirSamplingToListAggregator[T](capacity: Int)(implicit randomSupplier: () => Random)
    extends ReservoirSamplingAggregator[T, List[T]](capacity)(randomSupplier) {
  override def present(r: Reservoir[T]): List[T] =
    randomSupplier().shuffle(r.toSeq).toList

  override def andThenPresent[D](f: List[T] => D): MonoidAggregator[T, Reservoir[T], D] =
    new AndThenPresent(this, f)
}

/**
 * Monoid that implements [[andThenPresent]] without ruining the optimized behavior of the aggregator.
 */
private[algebird] class AndThenPresent[-A, B, C, +D](val agg: MonoidAggregator[A, B, C], f: C => D)
    extends MonoidAggregator[A, B, D] {
  override val monoid: Monoid[B] = agg.monoid
  override def prepare(a: A): B = agg.prepare(a)
  override def present(b: B): D = f(agg.present(b))

  override def apply(xs: TraversableOnce[A]): D = f(agg(xs))
  override def applyOption(xs: TraversableOnce[A]): Option[D] = agg.applyOption(xs).map(f)
  override def append(b: B, a: A): B = agg.append(b, a)
  override def appendAll(b: B, as: TraversableOnce[A]): B = agg.appendAll(b, as)
  override def appendAll(as: TraversableOnce[A]): B = agg.appendAll(as)
}
