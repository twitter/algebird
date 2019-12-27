package algebird

import sbt._

object GenTupleAggregators {
  def gen(dir: File) = {
    val tupleAggPlace = dir / "com" / "twitter" / "algebird" / "GeneratedTupleAggregators.scala"
    IO.write(
      tupleAggPlace,
      """package com.twitter.algebird

object GeneratedTupleAggregator extends GeneratedTupleAggregator

trait GeneratedTupleAggregator {
""" + genMethods(22, "implicit def", None) + "\n" + "}"
    )

    val multiAggPlace = dir / "com" / "twitter" / "algebird" / "MultiAggregator.scala"
    IO.write(
      multiAggPlace,
      """package com.twitter.algebird

object MultiAggregator {
""" +
        genMethods(22, "def", Some("apply")) + "\n" +
        genMethods(22, "def", Some("apply"), true) + "\n" + "}"
    )

    val mapAggPlace = dir / "com" / "twitter" / "algebird" / "MapAggregator.scala"
    IO.write(
      mapAggPlace,
      s"""
      |package com.twitter.algebird
      |
      |trait MapAggregator[A, B, K, C] extends Aggregator[A, B, Map[K, C]] {
      |  def keys: Set[K]
      |}
      |
      |trait MapMonoidAggregator[A, B, K, C] extends MonoidAggregator[A, B, Map[K, C]] {
      |  def keys: Set[K]
      |}
      |
      |object MapAggregator {
      |  ${genMapMethods(22)}
      |  ${genMapMethods(22, isMonoid = true)}
      |}
      """.stripMargin
    )

    Seq(tupleAggPlace, multiAggPlace, mapAggPlace)
  }

  def genMethods(max: Int, defStr: String, name: Option[String], isMonoid: Boolean = false): String =
    (2 to max)
      .map { i =>
        val methodName = name.getOrElse("from%d".format(i))
        val aggType = if (isMonoid) "Monoid" else ""
        val nums = (1 to i)
        val bs = nums.map("B" + _).mkString(", ")
        val cs = nums.map("C" + _).mkString(", ")
        val aggs = nums.map(x => "%sAggregator[A, B%s, C%s]".format(aggType, x, x)).mkString(", ")
        val prepares = nums.map(x => "aggs._%s.prepare(a)".format(x)).mkString(", ")
        val semiType = if (isMonoid) "monoid" else "semigroup"
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
}""".format(
          defStr,
          methodName,
          bs,
          cs,
          i,
          aggs,
          aggType,
          tupleBs,
          tupleCs,
          aggType,
          tupleBs,
          tupleCs,
          prepares,
          semiType,
          semigroup,
          tupleBs,
          present
        )
      }
      .mkString("\n")

  def genMapMethods(max: Int, isMonoid: Boolean = false): String = {
    val inputAggregatorType = if (isMonoid) "MonoidAggregator" else "Aggregator"
    val mapAggregatorType = if (isMonoid) "MapMonoidAggregator" else "MapAggregator"

    val semigroupType = if (isMonoid) "monoid" else "semigroup"

    // there's no Semigroup[Tuple1[T]], so just use T as intermediary type instead of Tuple1[T]
    // TODO: keys for 1 item
    val aggregatorForOneItem = s"""
       |def apply[K, A, B, C](agg: (K, ${inputAggregatorType}[A, B, C])): ${mapAggregatorType}[A, B, K, C] = {
       |  new ${mapAggregatorType}[A, B, K, C] {
       |    def prepare(a: A) = agg._2.prepare(a)
       |    val ${semigroupType} = agg._2.${semigroupType}
       |    def present(b: B) = Map(agg._1 -> agg._2.present(b))
       |    def keys = Set(agg._1)
       |  }
       |}
    """.stripMargin

    (2 to max)
      .map { aggrCount =>
        val aggrNums = 1 to aggrCount

        val inputAggs = aggrNums.map(i => s"agg$i: (K, ${inputAggregatorType}[A, B$i, C])").mkString(", ")

        val bs = aggrNums.map("B" + _).mkString(", ")
        val tupleBs = s"Tuple${aggrCount}[$bs]"

        s"""
      |def apply[K, A, $bs, C]($inputAggs): ${mapAggregatorType}[A, $tupleBs, K, C] = {
      |  new ${mapAggregatorType}[A, $tupleBs, K, C] {
      |    def prepare(a: A) = (
      |      ${aggrNums.map(i => s"agg${i}._2.prepare(a)").mkString(", ")}
      |    )
      |    // a field for semigroup/monoid that combines all input aggregators
      |    val $semigroupType = new Tuple${aggrCount}${semigroupType.capitalize}()(
      |      ${aggrNums.map(i => s"agg${i}._2.$semigroupType").mkString(", ")}
      |    )
      |    def present(b: $tupleBs) = Map(
      |      ${aggrNums.map(i => s"agg${i}._1 -> agg${i}._2.present(b._${i})").mkString(", ")}
      |    )
      |    def keys: Set[K] = Set(
      |      ${aggrNums.map(i => s"agg${i}._1").mkString(", ")}
      |    )
      |  }
      |}""".stripMargin
      }
      .mkString("\n") + aggregatorForOneItem
  }
}
