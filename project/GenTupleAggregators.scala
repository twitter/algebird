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
""" +
      genMethods(22, "def", Some("apply")) + "\n" +
      genMethods(22, "def", Some("apply"), true) + "\n" + "}")

    val mapAggPlace = dir / "com" / "twitter" / "algebird" / "MapAggregator.scala"
    IO.write(mapAggPlace,
"""package com.twitter.algebird

object MapAggregator {
""" +
      genMapMethods(22) + "\n" +
      genMapMethods(22, true) + "\n" + "}")

    Seq(tupleAggPlace, multiAggPlace, mapAggPlace)
  }

  def genMethods(max: Int, defStr: String, name: Option[String], isHasAdditionOperatorAndZero: Boolean = false): String = {
    (2 to max).map(i => {
      val methodName = name.getOrElse("from%d".format(i))
      val aggType = if (isHasAdditionOperatorAndZero) "HasAdditionOperatorAndZero" else ""
      val nums = (1 to i)
      val bs = nums.map("B" + _).mkString(", ")
      val cs = nums.map("C" + _).mkString(", ")
      val aggs = nums.map(x => "%sAggregator[A, B%s, C%s]".format(aggType, x, x)).mkString(", ")
      val prepares = nums.map(x => "aggs._%s.prepare(a)".format(x)).mkString(", ")
      val semiType = if (isHasAdditionOperatorAndZero) "monoid" else "semigroup"
      val semigroups = nums.map(x => "aggs._%s.%s".format(x, semiType)).mkString(", ")
      val semigroup = "new Tuple%d%s()(%s)".format(i, semiType.capitalize, semigroups)
      val present = nums.map(x => "aggs._%s.present(b._%s)".format(x, x)).mkString(", ")
      val tupleBs = "Tuple%d[%s]".format(i, bs)
      val tupleCs = "Tuple%d[%s]".format(i, cs)

      """
%s %s[A, %s, %s](aggs: Tuple%d[%s]): %sAggregator[A, %s, %s] = {
  new %sAggregator[A, %s, %s] {
    def prepare(a: A) = (%s)
    val %s = %s
    def present(b: %s) = (%s)
  }
}""".format(defStr, methodName, bs, cs, i, aggs, aggType, tupleBs, tupleCs,
            aggType, tupleBs, tupleCs,
            prepares,
            semiType, semigroup,
            tupleBs, present)
    }).mkString("\n")
  }

  def genMapMethods(max: Int, isHasAdditionOperatorAndZero: Boolean = false): String = {
    (2 to max).map(i => {
      val aggType = if (isHasAdditionOperatorAndZero) "HasAdditionOperatorAndZero" else ""
      val nums = (1 to i)
      val bs = nums.map("B" + _).mkString(", ")
      val aggs = nums.map(x => "agg%s: Tuple2[K, %sAggregator[A, B%s, C]]".format(x, aggType, x)).mkString(", ")
      val prepares = nums.map(x => "agg%s._2.prepare(a)".format(x)).mkString(", ")
      val semiType = if (isHasAdditionOperatorAndZero) "monoid" else "semigroup"
      val semigroups = nums.map(x => "agg%s._2.%s".format(x, semiType)).mkString(", ")
      val semigroup = "new Tuple%d%s()(%s)".format(i, semiType.capitalize, semigroups)
      val present = nums.map(x => "agg%s._1 -> agg%s._2.present(b._%s)".format(x, x, x)).mkString(", ")
      val tupleBs = "Tuple%d[%s]".format(i, bs)

      """
def apply[K, A, %s, C](%s): %sAggregator[A, %s, Map[K, C]] = {
  new %sAggregator[A, %s, Map[K, C]] {
    def prepare(a: A) = (%s)
    val %s = %s
    def present(b: %s) = Map(%s)
  }
}""".format(bs, aggs, aggType, tupleBs,
            aggType, tupleBs,
            prepares,
            semiType, semigroup,
            tupleBs, present)
    }).mkString("\n")
  }
}
