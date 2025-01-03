package com.twitter.algebird

import com.twitter.algebird.scalacheck.Distribution._
import org.scalacheck.{Gen, Prop}

object RandomSamplingLaws {

  def sampleOneUniformly[T](newSampler: Int => Aggregator[Int, T, Seq[Int]]): Prop = {
    val n = 100

    "sampleOne" |: forAllSampled(10000, Gen.choose(1, 20))(_ => uniform(n)) { k =>
      newSampler(k).andThenPresent(_.head).apply(0 until n)
    }
  }

  def reservoirSizeOne[T](newSampler: Int => Aggregator[Int, T, Seq[Int]]): Prop = {
    val n = 100

    "reservoirSizeOne" |: forAllSampled(10000)(uniform(n)) {
      newSampler(1).andThenPresent(_.head).apply(0 until n)
    }
  }

  def reservoirSizeTwo[T](newSampler: Int => Aggregator[Int, T, Seq[Int]]): Prop = {
    val n = 10
    val tuples = for {
      i <- 0 until n
      j <- 0 until n
      if i != j
    } yield (i, j)

    "reservoirSizeTwo" |: forAllSampled(10000)(tuples.map(_ -> 1d).toMap) {
      newSampler(2).andThenPresent(xs => (xs(0), xs(1))).apply(0 until n)
    }
  }

  def sampleSpecificItem[T](newSampler: Int => Aggregator[Int, T, Seq[Int]]): Prop = {
    val sizeAndIndex: Gen[(Int, Int)] = for {
      k <- Gen.choose(1, 10)
      i <- Gen.choose(0, k - 1)
    } yield (k, i)

    val n = 100

    "sampleAnyItem" |: forAllSampled(10000, sizeAndIndex)(_ => uniform(n)) { case (k, i) =>
      newSampler(k).andThenPresent(_(i)).apply(0 until n)
    }
  }

  def sampleTwoItems[T](newSampler: Int => Aggregator[Int, T, Seq[Int]]): Prop = {
    val sizeAndIndexes: Gen[(Int, Int, Int)] = for {
      k <- Gen.choose(1, 10)
      i <- Gen.choose(0, k - 1)
      j <- Gen.choose(0, k - 1)
      if i != j
    } yield (k, i, j)

    val n = 20

    "sampleTwoItems" |: forAllSampled(10000, sizeAndIndexes)(_ =>
      (for {
        i <- 0 until n
        j <- 0 until n
        if i != j
      } yield (i, j)).map(_ -> 1d).toMap
    ) { case (k, i, j) =>
      newSampler(k).andThenPresent(xs => (xs(i), xs(j))).apply(0 until n)
    }
  }

  def appendPiecewise[T](newSampler: Int => MonoidAggregator[Int, T, Seq[Int]]): Prop =
    "sampleOne" |: forAllSampled(10000, Gen.choose(1, 20))(k => uniform(k + 1)) { k =>
      val agg = newSampler(k)
      val res = agg.appendAll(agg.monoid.zero, 0 until k)
      agg.present(agg.appendAll(res, List(k))).head
    }

  def aggregateHierarchy[T](newSampler: Int => MonoidAggregator[Int, T, Seq[Int]]): Prop =
    "aggregateHierarchy" |: forAllSampled(10000, Gen.choose(1, 20))(k => uniform(6 * k)) { k =>
      val agg = newSampler(k)
      val r1 = agg.reduce((0 until 2 * k).map(agg.prepare))
      val r2 = agg.reduce((2 * k until 3 * k).map(agg.prepare))
      val r3 = agg.reduce((3 * k until 4 * k).map(agg.prepare))
      val r4 = agg.reduce((4 * k until 6 * k).map(agg.prepare))
      val s1 = agg.monoid.plus(r1, r2)
      val s2 = agg.monoid.plus(r3, r4)
      val res = agg.monoid.plus(s1, s2)
      agg.present(res).head
    }

  def imbalancedSplit[T](newSampler: Int => MonoidAggregator[Int, T, Seq[Int]]): Prop =
    "imbalancedSplit" |: forAllSampled(10000, Gen.choose(10, 30))(uniform) { k =>
      val agg = newSampler(2)
      val r1 = agg.appendAll(agg.monoid.zero, Seq(0, 1))
      val r2 = agg.appendAll(agg.monoid.zero, 2 until k)
      val res = agg.monoid.plus(r1, r2)
      agg.present(res).head
    }

  def aggregateNonFullReservoirs[T](newSampler: Int => MonoidAggregator[Int, T, Seq[Int]]): Prop =
    "aggregateNonFullReservoirs" |: forAllSampled(10000, Gen.choose(1, 10))(k => uniform(6 * k)) { k =>
      val agg = newSampler(2 * k).andThenPresent(_.head)
      val r1 = agg.appendAll(agg.monoid.zero, 0 until 2 * k)
      val r2 = agg.appendAll(agg.monoid.zero, 2 * k until 3 * k)
      val r3 = agg.appendAll(agg.monoid.zero, 3 * k until 4 * k)
      val r4 = agg.appendAll(agg.monoid.zero, 4 * k until 6 * k)
      val s1 = agg.monoid.plus(r1, r2)
      val s2 = agg.monoid.plus(r3, r4)
      val res = agg.monoid.plus(s1, s2)
      agg.present(res)
    }

  def randomSamplingDistributions[T](newSampler: Int => MonoidAggregator[Int, T, Seq[Int]]): Prop =
    sampleOneUniformly(newSampler) &&
      reservoirSizeOne(newSampler) &&
      reservoirSizeTwo(newSampler) &&
      sampleSpecificItem(newSampler) &&
      sampleTwoItems(newSampler) &&
      appendPiecewise(newSampler) &&
      aggregateHierarchy(newSampler) &&
      imbalancedSplit(newSampler) &&
      aggregateNonFullReservoirs(newSampler)
}
