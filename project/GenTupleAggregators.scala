package algebird

import sbt._

object GenTupleAggregators {
  def gen(dir: File) = {
    val place = dir / "com" / "twitter" / "algebird" / "GeneratedTupleAggregators.scala"
    IO.write(place,
"""package com.twitter.algebird

object GeneratedTupleAggregator extends GeneratedTupleAggregator

trait GeneratedTupleAggregator {
""" + genAggregators(22) + "\n" + "}")

    Seq(place)
  }

  def genAggregators(max: Int): String = {
    (2 to max).map(i => {
      val nums = (1 to i)
      val vs = nums.map("aggs._%d.B".format(_)).mkString(", ")
      val ts = nums.map("T" + _).mkString(", ")
      val aggs = nums.map(x => "Aggregator[A, T%s]".format(x)).mkString(", ")
      val prepares = nums.map(x => "aggs._%s.prepare(v)".format(x)).mkString(", ")
      val reduces = nums.map(x => "aggs._%s.reduce(v1._%s, v2._%s)".format(x, x, x)).mkString(", ")
      val present = nums.map(x => "aggs._%s.present(v._%s)".format(x, x)).mkString(", ")
      val tupleVs = "Tuple%d[%s]".format(i, vs)
      val tupleTs = "Tuple%d[%s]".format(i, ts)

      """
implicit def from%d[A, %s](aggs: Tuple%d[%s]): Aggregator[A, %s] = {
  new Aggregator[A, %s] {
    type B = %s
    def prepare(v: A) = (%s)
    def reduce(v1: %s, v2: %s) = (%s)
    def present(v: %s) = (%s)
  }
}""".format(i, ts, i, aggs, tupleTs,
            tupleTs,
            tupleVs,
            prepares,
            tupleVs, tupleVs, reduces,
            tupleVs, present)
    }).mkString("\n")
  }
}
