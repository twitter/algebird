package algebird

import sbt._

object GenTupleAggregators {
  def gen(dir: File) = {
    val tupleAggPlace = dir / "com" / "twitter" / "algebird" / "GeneratedTupleAggregators.scala"
    IO.write(tupleAggPlace,
"""package com.twitter.algebird

object GeneratedTupleAggregator extends GeneratedTupleAggregator

trait GeneratedTupleAggregator {
""" + genMethods(22, "implicit def", None) + "\n" + "}")

    val multiAggPlace = dir / "com" / "twitter" / "algebird" / "MultiAggregator.scala"
    IO.write(multiAggPlace,
"""package com.twitter.algebird

object MultiAggregator {
""" + genMethods(22, "def", Some("apply")) + "\n" + "}")

    Seq(tupleAggPlace, multiAggPlace)
  }

  def genMethods(max: Int, defStr: String, name: Option[String]): String = {
    (2 to max).map(i => {
      val methodName = name.getOrElse("from%d".format(i))
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
%s %s[A, %s, %s](aggs: Tuple%d[%s]): Aggregator[A, %s, %s] = {
  new Aggregator[A, %s, %s] {
    def prepare(a: A) = (%s)
    val semigroup = %s
    def present(b: %s) = (%s)
  }
}""".format(defStr, methodName, bs, cs, i, aggs, tupleBs, tupleCs,
            tupleBs, tupleCs,
            prepares,
            semigroup,
            tupleBs, present)
    }).mkString("\n")
  }
}
