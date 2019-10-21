/*
Copyright 2014 Twitter, Inc.

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

/**
 * HLLSeries can produce a HyperLogLog counter for any window into the past,
 * using a constant factor more space than HyperLogLog.
 *
 * For each hash bucket, rather than keeping a single max RhoW value, it keeps
 * every RhoW value it has seen, and the max timestamp where it saw that value.
 * This allows it to reconstruct an HLL as it would be had it started at zero at
 * any given point in the past, and seen the same updates this structure has seen.
 *
 * @param bits The number of bits to use
 * @param rows Vector of maps of RhoW -> max timestamp where it was seen
 *
 * @return  New HLLSeries
 */
case class HLLSeries(bits: Int, rows: Vector[Map[Int, Long]]) {

  def insert(data: Array[Byte], timestamp: Long): HLLSeries = {
    import HyperLogLog._
    val hashed = hash(data)
    val j0 = j(hashed, bits)
    val rhow = rhoW(hashed, bits) - 1

    val newRows = if (rhow < rows.size) {
      val row = rows(rhow)
      val t = row.get(j0) match {
        case Some(t0) => Math.max(t0, timestamp)
        case None     => timestamp
      }
      rows.updated(rhow, row.updated(j0, t))
    } else {
      val padded =
        (rows.size until rhow).foldLeft(rows)((rs, _) => rs :+ Map.empty)
      padded :+ Map(j0 -> timestamp)
    }
    HLLSeries(bits, newRows)
  }

  def maxRhowStats(threshold: Long): (Int, Double) = {
    val seen = scala.collection.mutable.Set.empty[Int]
    var sum: Double = 0.0
    var i = rows.size - 1
    while (i >= 0) {
      val it = rows(i).iterator
      while (it.hasNext) {
        val (k, t) = it.next
        if (t >= threshold && seen.add(k)) {
          sum += HyperLogLog.negativePowersOfTwo(i + 1)
        }
      }
      i -= 1
    }
    (seen.size, sum)
  }

  def approximateSizeSince(threshold: Long): Approximate[Long] = {
    val (maxRhowSize, maxRhowSum) = maxRhowStats(threshold)
    val size = 1 << bits
    val zeroCnt = size - maxRhowSize
    val z = 1.0 / (zeroCnt + maxRhowSum)
    HyperLogLog.approximateSize(bits, size, zeroCnt, z)
  }

  /**
   * @param since Timestamp from which to reconstruct the HLL
   *
   * @return New HLLSeries only including RhoWs for values seen at or after the given timestamp
   */
  def since(threshold: Long): HLLSeries =
    HLLSeries(bits, rows.map { _.filter { case (_, ts) => ts >= threshold } })

  def toHLL: HLL = {
    val monoid = new HyperLogLogMonoid(bits)
    if (rows.isEmpty) monoid.zero
    else {
      monoid.sum(rows.iterator.zipWithIndex.map {
        case (map, i) =>
          SparseHLL(bits, map.transform {
            case _ =>
              Max((i + 1).toByte)
          })
      })
    }
  }
}

/**
 * =Example Usage=
 *
 * val hllSeriesMonoid = new HyperLogLogSeriesMonoid(bits)
 *
 * val examples: Seq[Array[Byte], Long]
 * val series = examples
 *                .map { case (bytes, timestamp) =>
 *                  hllSeriesMonoid.create(bytes, timestamp)
 *                }
 *                .reduce { hllSeriesMonoid.plus(_,_) }
 *
 * val estimate1 = series.since(timestamp1.toLong).toHLL.estimatedSize
 * val estimate2 = series.since(timestamp2.toLong).toHLL.estimatedSize
 */
class HyperLogLogSeriesMonoid(val bits: Int) extends Monoid[HLLSeries] {
  import HyperLogLog._

  override val zero: HLLSeries = HLLSeries(bits, Vector.empty)

  def create(example: Array[Byte], timestamp: Long): HLLSeries = {
    val hashed = hash(example)
    val rhow = rhoW(hashed, bits)
    val e = Map.empty[Int, Long]
    val bldr = Vector.newBuilder[Map[Int, Long]]
    var i = 1
    while (i < rhow) { bldr += e; i += 1 }
    bldr += Map(j(hashed, bits) -> timestamp)
    HLLSeries(bits, bldr.result())
  }

  override def plus(left: HLLSeries, right: HLLSeries): HLLSeries = {
    val ln = left.rows.size
    val rn = right.rows.size
    if (ln > rn) {
      plus(right, left)
    } else {
      val bldr = Vector.newBuilder[Map[Int, Long]]
      val lit = left.rows.iterator
      val rit = right.rows.iterator
      while (lit.hasNext && rit.hasNext) bldr += combine(lit.next, rit.next)
      val zipped = bldr.result()
      HLLSeries(bits, zipped ++ right.rows.slice(ln, rn))
    }
  }

  private def combine(left: Map[Int, Long], right: Map[Int, Long]): Map[Int, Long] =
    if (left.size > right.size) {
      combine(right, left)
    } else {
      left.foldLeft(right) {
        case (m, (k, lv)) =>
          m.updated(k, m.get(k) match {
            case None     => lv
            case Some(rv) => Math.max(lv, rv)
          })
      }
    }
}
