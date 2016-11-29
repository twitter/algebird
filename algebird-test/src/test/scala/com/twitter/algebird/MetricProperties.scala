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

import org.scalacheck.Arbitrary
import org.scalacheck.Prop._

class BaseMetricProperties extends CheckProperties with MetricProperties {
  property("double metric") {
    metricLaws[Double]
  }

  property("int metric") {
    metricLaws[Int]
  }

  property("float metric") {
    metricLaws[Float]
  }

  property("long metric") {
    metricLaws[Long]
  }

  property("short metric") {
    metricLaws[Short]
  }

  implicit val iterMetric = Metric.L1Iterable[Double]

  // TODO: we won't need this when we have an Equatable trait
  def listEqFn(a: List[Double], b: List[Double]) = {
    val maxSize = scala.math.max(a.size, b.size)
    val diffA = maxSize - a.size
    val diffB = maxSize - b.size
    val newA = if (diffA > 0) a ++ Iterator.fill(diffA)(0.0) else a
    val newB = if (diffB > 0) b ++ Iterator.fill(diffB)(0.0) else b
    newA == newB
  }

  property("double iterable metric") {
    implicit val eq: Equiv[List[Double]] = Equiv.fromFunction(listEqFn)
    metricLaws[List[Double]]
  }

  implicit val mapMetric = Metric.L1Map[Int, Double]

  // TODO: we won't need this when we have an Equatable trait
  def mapEqFn(a: Map[Int, Double], b: Map[Int, Double]) = {
    (a.keySet ++ b.keySet).forall { key =>
      (a.get(key), b.get(key)) match {
        case (Some(aVal), Some(bVal)) => aVal == bVal
        case (Some(aVal), None) => aVal == 0.0
        case (None, Some(bVal)) => bVal == 0.0
        case _ => true
      }
    }
  }

  property("int double map metric") {
    implicit val eq: Equiv[Map[Int, Double]] = Equiv.fromFunction(mapEqFn)
    metricLaws[Map[Int, Double]]
  }
}

trait MetricProperties {
  def isNonNegative[T: Metric: Arbitrary] = forAll { (a: T, b: T) =>
    val m = Metric(a, b)
    beGreaterThan(m, 0.0) || beCloseTo(m, 0.0)
  }
  def isEqualIffZero[T: Metric: Arbitrary: Equiv] =
    forAll { (a: T, b: T) =>
      if (Equiv[T].equiv(a, b)) beCloseTo(Metric(a, b), 0.0)
      else !beCloseTo(Metric(a, b), 0.0)
    }
  def isSymmetric[T: Metric: Arbitrary] = forAll { (a: T, b: T) =>
    beCloseTo(Metric(a, b), Metric(b, a))
  }
  def satisfiesTriangleInequality[T: Metric: Arbitrary] = forAll { (a: T, b: T, c: T) =>
    val m1 = Metric(a, b) + Metric(b, c)
    val m2 = Metric(a, c)
    beGreaterThan(m1, m2) || beCloseTo(m1, m2)
  }

  def metricLaws[T: Metric: Arbitrary: Equiv] =
    isNonNegative[T] && isEqualIffZero[T] && isSymmetric[T] && satisfiesTriangleInequality[T]

  // TODO: these are copied elsewhere in the tests. Move them to a common place
  def beCloseTo(a: Double, b: Double, eps: Double = 1e-10) = a == b || (math.abs(a - b) / math.abs(a)) < eps || (a.isInfinite && b.isInfinite)
  def beGreaterThan(a: Double, b: Double, eps: Double = 1e-10) = a > b - eps || (a.isInfinite && b.isInfinite)
}
