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
      val bs = nums.map("B" + _).mkString(", ")
      val cs = nums.map("C" + _).mkString(", ")
      val aggs = nums.map(x => "Aggregator[A, B%s, C%s]".format(x, x)).mkString(", ")
      val prepares = nums.map(x => "aggs._%s.prepare(a)".format(x)).mkString(", ")
      val semigroups = nums.map(x => "aggs._%s.semigroup".format(x)).mkString(", ")
      val semigroup = "new Tuple%dSemigroup()(%s)".format(i, semigroups)
      val present = nums.map(x => "aggs._%s.present(b._%s)".format(x, x)).mkString(", ")
      val tupleBs = "Tuple%d[%s]".format(i, bs)
      val tupleCs = "Tuple%d[%s]".format(i, cs)

      """
implicit def from%d[A, %s, %s](aggs: Tuple%d[%s]): Aggregator[A, %s, %s] = {
  new Aggregator[A, %s, %s] {
    def prepare(a: A) = (%s)
    val semigroup = %s
    def present(b: %s) = (%s)
  }
}""".format(i, bs, cs, i, aggs, tupleBs, tupleCs,
            tupleBs, tupleCs,
            prepares,
            semigroup,
            tupleBs, present)
    }).mkString("\n")
  }
}
