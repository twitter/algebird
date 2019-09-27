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

import com.twitter.algebird.BaseProperties._

class MetricLaws extends CheckProperties {
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
  def mapEqFn(a: Map[Int, Double], b: Map[Int, Double]) =
    (a.keySet ++ b.keySet).forall { key =>
      (a.get(key), b.get(key)) match {
        case (Some(aVal), Some(bVal)) => aVal == bVal
        case (Some(aVal), None)       => aVal == 0.0
        case (None, Some(bVal))       => bVal == 0.0
        case _                        => true
      }
    }

  property("int double map metric") {
    implicit val eq: Equiv[Map[Int, Double]] = Equiv.fromFunction(mapEqFn)
    metricLaws[Map[Int, Double]]
  }
}
