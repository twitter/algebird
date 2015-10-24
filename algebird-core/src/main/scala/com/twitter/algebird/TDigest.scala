/*
Copyright 2015 Twitter, Inc.

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

import tdmap.TDigestMap

/**
 * A t-digest sketch of sampled numeric data, as described in:
 * Computing Extremely Accurate Quantiles Using t-Digests,
 * Ted Dunning and Otmar Ertl,
 * https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf
 */
case class TDigest(
  delta: Double,
  nclusters: Int,
  clusters: TDigestMap) extends Serializable {

  // re-cluster when number of clusters exceeds this threshold
  private val R = (TDigest.K / delta).toInt

  private case class Cluster(centroid: Double, mass: Double, massUB: Double)

  /**
   * Returns a new t-digest with value x included in its sketch; td + x is equivalent to
   * td + (x, 1).
   * @param x The numeric data value to include in the sketch
   * @return the updated sketch
   */
  def +[N](x: N)(implicit num: Numeric[N]): TDigest = this.+((x, 1))

  /**
   * Returns a new t-digest with new pair (x, w) included in its sketch.
   * @param xw A pair (x, w) where x is the numeric value and w is its weight
   * @return the updated sketch
   * @note This implements 'algorithm 1' from:
   * Computing Extremely Accurate Quantiles Using t-Digests,
   * Ted Dunning and Otmar Ertl,
   * https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf
   */
  def +[N1, N2](xw: (N1, N2))(implicit num1: Numeric[N1], num2: Numeric[N2]): TDigest = {
    val s = this.update(xw)
    if (s.nclusters <= R) s
    else {
      // too many clusters: attempt to compress it by re-clustering
      val ds = scala.util.Random.shuffle(s.clusters.toVector)
      ds.foldLeft(TDigest.empty(delta))((d, e) => d.update(e))
    }
  }

  /**
   * Add this digest to another
   * @param that The right-hand t-digest operand
   * @return the sum of left and right digests, as defined by TDigestSemigroup
   * @see TDigestSemigroup
   */
  def ++(that: TDigest): TDigest = TDigestSemigroup.plus(this, that, this.delta)

  // This is most of 'algorithm 1', except for re-clustering which is factored out to avoid
  // recursive calls during a reclustering phase
  private def update[N1, N2](xw: (N1, N2))(implicit num1: Numeric[N1], num2: Numeric[N2]) = {
    val xn = num1.toDouble(xw._1)
    var wn = num2.toDouble(xw._2)
    require(wn > 0.0, "data weight must be > 0")

    // Get the current cluster nearest to incoming (xn)
    // Note: 'near' will have length 0,1, or 2:
    // length 0 => current cluster map was empty (no data yet)
    // length 1 => exactly one cluster was closest to (xn)
    // length 2 => (xn) was mid-point between two clusters (both are returned, in key order)
    val near = clusters.nearest(xn)

    if (near.isEmpty) {
      // our map is empty, so insert this pair as the first cluster
      TDigest(delta, nclusters + 1, clusters + ((xn, wn)))
    } else {
      // compute upper bounds for cluster masses, from their quantile estimates
      var massPS = clusters.prefixSum(near.head._1, open = true)
      val massTotal = clusters.sum
      val s = near.map {
        case (c, m) =>
          val q = (massPS + m / 2.0) / massTotal
          val ub = massTotal * delta * q * (1.0 - q)
          massPS += m
          Cluster(c, m, ub)
      }

      // assign new mass (wn) among the clusters
      var cmNew = Vector.empty[(Double, Double)]
      scala.util.Random.shuffle(s).foreach { clust =>
        if (wn <= 0.0) {
          // if we have already distributed all the mass, remaining clusters unchanged
          cmNew = cmNew :+ ((clust.centroid, clust.mass))
        } else if (xn == clust.centroid) {
          // if xn lies exactly on the centroid, add all mass in regardless of bound
          cmNew = cmNew :+ ((clust.centroid, clust.mass + wn))
          wn = 0.0
        } else if (clust.mass < clust.massUB) {
          // cluster can accept more mass, respecting its upper bound
          val dm = math.min(wn, clust.massUB - clust.mass)
          val mass = clust.mass + dm
          val dc = dm * (xn - clust.centroid) / mass
          wn -= dm
          cmNew = cmNew :+ ((clust.centroid + dc, mass))
        } else {
          // cluster is at its upper bound for mass, it remains unchanged
          cmNew = cmNew :+ ((clust.centroid, clust.mass))
        }
      }

      // any remaining mass becomes a new cluster
      if (wn > 0.0) cmNew = cmNew :+ ((xn, wn))

      // remove original clusters and replace with the new ones
      val clustDel = near.iterator.map(_._1).foldLeft(clusters)((c, e) => c - e)
      val clustNew = cmNew.foldLeft(clustDel)((c, p) => c.increment(p._1, p._2))
      val nc = nclusters - s.length + cmNew.length

      // return the updated t-digest
      TDigest(delta, nc, clustNew)
    }
  }

  /**
   * Compute a cumulative probability (CDF) for a numeric value, from the estimated probability
   * distribution represented by this t-digest sketch.
   * @param x a numeric value
   * @return the cumulative probability that a random sample from the distribution is <= x
   */
  def cdf[N](x: N)(implicit num: Numeric[N]): Double = clusters.cdf(x)

  /**
   * Compute the inverse cumulative probability (inverse-CDF) for a quantile value, from the
   * estimated probability distribution represented by this t-digest sketch.
   * @param q a quantile value.  The value of q is expected to be on interval [0, 1]
   * @return the value x such that cdf(x) = q
   */
  def cdfInverse[N](q: N)(implicit num: Numeric[N]): Double = clusters.cdfInverse(q)
}

/** Factory functions for TDigest */
object TDigest {
  /**
   * Default value for a t-digest delta parameter.  The number of clusters varies, roughly, as
   * about (50/delta), when data are presented in random order
   * (it may grow larger if data are not presented randomly).  The default corresponds to
   * an expected number of clusters of about 100.
   */
  val deltaDefault: Double = (50.0 / 100.0) // delta * E[clusters] ~ 50

  /**
   * The t-digest algorithm will re-cluster itself whenever its number of clusters exceeds
   * (K/delta).  This value is set such that the threshold is about 10x the heuristically
   * expected number of clusters for the user-specified delta value.  Generally the number of
   * clusters will only trigger the corresponding re-clustering threshold when data are being
   * presented in a non-random order.
   */
  val K: Double = 10.0 * 50.0

  /**
   * Obtain an empty t-digest
   * @param delta a sketch resolution parameter.
   * @note Smaller values of delta yield sketches with more clusters, and higher resolution
   * @note The expected number of clusters will vary (roughly) as (50/delta)
   */
  def empty(delta: Double = deltaDefault): TDigest = {
    require(delta > 0.0, s"delta was not > 0")
    TDigest(delta, 0, TDigestMap.empty)
  }

  /**
   * Sketch some data with a t-digest
   * @param data The data elements to sketch
   * @param delta The sketch resolution parameter.
   * @return A t-digest sketch of the input data
   * @note Smaller values of delta yield sketches with more clusters, and higher resolution
   * @note The expected number of clusters will vary (roughly) as (50/delta)
   */
  def sketch[N](
    data: TraversableOnce[N],
    delta: Double = deltaDefault)(implicit num: Numeric[N]): TDigest = {
    require(delta > 0.0, s"delta was not > 0")
    val td = data.foldLeft(empty(delta))((c, e) => c + ((e, 1)))
    val ds = scala.util.Random.shuffle(td.clusters.toVector)
    ds.foldLeft(empty(delta))((c, e) => c + e)
  }

  /**
   * Obtain a TDigest semigroup instance
   * @param delta The TDigest sketch resolution parameter
   * @return a TDigest semigroup
   */
  def semigroup: TDigestSemigroup = new TDigestSemigroup

  /**
   * Obtain a TDigest aggregator instance
   * @tparam N The type of numeric data expected
   * @param delta The TDigest sketch resolution parameter
   * @return a TDigest aggregator
   */
  def aggregator[N](delta: Double = deltaDefault)(implicit num: Numeric[N]): TDigestAggregator[N] =
    new TDigestAggregator[N](delta)

  /** Try to add a bit of efficiency to wasteful construction of t-digest objects for each input */
  private[algebird] def prepare[N](x: N, delta: Double = deltaDefault)(implicit num: Numeric[N]) =
    TDigest(delta, 1, TDigestMap.empty + ((num.toDouble(x), 1.0)))
}

/** The Semigroup type class for TDigest objects */
class TDigestSemigroup extends Semigroup[TDigest] {
  /**
   * Add two t-digests to yield a new aggregated digest
   * @param ltd the left-hand t-digest operand
   * @param rtd the right hand t-digest
   * @return the sum of left and right digests, defined as their aggregation
   * @note This operation satisfies a Semigroup law, with the caveat
   * that it is only "statistically" associative: d1++(d2++d3) will be statistically
   * similar to (d1++d2)++d3, but rarely identical.
   */
  def plus(ltd: TDigest, rtd: TDigest): TDigest = TDigestSemigroup.plus(ltd, rtd, ltd.delta)
}

object TDigestSemigroup {
  def plus(ltd: TDigest, rtd: TDigest, delta: Double = TDigest.deltaDefault): TDigest = {
    if (ltd.nclusters <= 1 && rtd.nclusters > 1) plus(rtd, ltd, delta)
    else if (rtd.nclusters == 0) ltd
    else if (rtd.nclusters == 1) {
      // handle the singleton RHS case specially to prevent quadratic catastrophe when
      // it is being used in the Aggregator use case
      val d = rtd.clusters.asInstanceOf[tdmap.tree.INodeTD].data
      ltd + ((d.key, d.value))
    } else {
      val ds = scala.util.Random.shuffle(ltd.clusters.toVector ++ rtd.clusters.toVector)
      ds.foldLeft(TDigest.empty(delta))((d, e) => d + e)
    }
  }
}

/**
 * Aggregator subclass for sketching data using TDigest
 * @tparam N the expected type of numeric input data
 * @param delta the sketch resolution parameter
 */
class TDigestAggregator[N](delta: Double = TDigest.deltaDefault)(implicit num: Numeric[N])
  extends Aggregator[N, TDigest, TDigest] {
  require(delta > 0.0, s"delta was not > 0")
  def semigroup = TDigest.semigroup
  def prepare(x: N) = TDigest.prepare(x, delta)
  def present(td: TDigest) = td
}
