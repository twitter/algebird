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
      val semigroups = nums.map(x => "aggs._%s.semigroup".format(x)).mkString(", ")
      val semigroup = "new Tuple%dSemigroup()(%s)".format(i, semigroups)
      val present = nums.map(x => "aggs._%s.present(v._%s)".format(x, x)).mkString(", ")
      val tupleVs = "Tuple%d[%s]".format(i, vs)
      val tupleTs = "Tuple%d[%s]".format(i, ts)

      """
implicit def from%d[A, %s](aggs: Tuple%d[%s]): Aggregator[A, %s] = {
  new Aggregator[A, %s] {
    type B = %s
    def prepare(v: A) = (%s)
    val semigroup = %s
    def present(v: %s) = (%s)
  }
}""".format(i, ts, i, aggs, tupleTs,
            tupleTs,
            tupleVs,
            prepares,
            semigroup,
            tupleVs, present)
    }).mkString("\n")
  }
}
