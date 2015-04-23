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
  /**
   * @param since Timestamp from which to reconstruct the HLL
   *
   * @return New HLLSeries only including RhoWs for values seen at or after the given timestamp
   */
  def since(threshold: Long) =
    HLLSeries(
      bits,
      rows.map{ _.filter{ case (j, ts) => ts >= threshold } })

  def toHLL: HLL =
    if (rows.isEmpty)
      SparseHLL(bits, Map())
    else
      rows.zipWithIndex.map{
        case (map, i) =>
          SparseHLL(bits, map.mapValues{ ts => Max((i + 1).toByte) }): HLL
      }.reduce{ _ + _ }
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

  val zero = HLLSeries(bits, Vector())

  def create(example: Array[Byte], timestamp: Long): HLLSeries = {
    val hashed = hash(example)
    val (j, rhow) = jRhoW(hashed, bits)

    val vector = Vector.fill(rhow - 1){ Map[Int, Long]() } ++ Vector(Map(j -> timestamp))
    HLLSeries(bits, vector)
  }

  def plus(left: HLLSeries, right: HLLSeries): HLLSeries = {
    if (left.rows.size > right.rows.size)
      plus(right, left)
    else {
      val zipped = left.rows.zip(right.rows).map{
        case (l, r) =>
          combine(l, r)
      }
      HLLSeries(
        bits,
        zipped ++ right.rows.slice(left.rows.size, right.rows.size))
    }
  }

  private def combine(left: Map[Int, Long], right: Map[Int, Long]): Map[Int, Long] = {
    if (left.size > right.size)
      combine(right, left)
    else {
      right ++
        left.map{ case (k, v) => k -> (right.getOrElse(k, 0L).max(v)) }
    }
  }
}
