package com.twitter.algebird

import scala.collection.mutable.{Map => MMap}
import org.specs2.mutable._



class BenchMarkingTest extends Specification{
  def time[A](name: String, op: =>A): A = {
    val begin = System.currentTimeMillis()
    var res = op
    for (a <- 1 to 40) {
      op
    }
    val end = System.currentTimeMillis()
    println(name + ":" + (end - begin) + "ms")
    res
  }

  "An ScMapMonoid" should {
    "be able to add to itself"  in {
      val monoid = new ScMapMonoid[Int, Int]()

      val map1 = MMap(Range(0, 1000).zipWithIndex: _*)
      val res1 = time("ScMapMonoid mutable add itself", monoid.plus(map1, map1))
      res1 must_== MMap(Range(1, 1000).zip(Range(2, 2000, 2)): _*)
    }

    "be able to add to another"  in {
      val monoid = new ScMapMonoid[Int, Int]()
      val map1 = MMap(Range(0, 1000).zipWithIndex: _*)
      val map2 = MMap(Range(0, 1000).zipWithIndex: _*)
      val res1 = time("ScMapMonoid mutable add another", monoid.plus(map1, map2)) // time: 104.189ms
      res1 must_== MMap(Range(1, 1000).zip(Range(2, 2000, 2)): _*)
    }

    "be OK non mutable" in {
      val monoid = new ScMapMonoid[Int, Int]()
      val map1 = Map(Range(0, 1000).zipWithIndex: _*)
      val map2 = Map(Range(0, 1000).zipWithIndex: _*)
      val res1 = time("ScMapMonoid immutable add another", monoid.plus(map1, map2)) // time: 24.598ms
      res1 must_== Map(Range(1, 1000).zip(Range(2, 2000, 2)): _*)
    }

    "be able to add to itself sumOption"  in {
      val monoid = new ScMapMonoid[Int, Int]()

      val map1 = MMap(Range(0, 1000).zipWithIndex: _*)
      val res1 = time("ScMapMonoid mutable sumOption itself", monoid.sumOption(List(map1, map1))).get
      res1 must_== MMap(Range(1, 1000).zip(Range(2, 2000, 2)): _*)
    }

    "be able to add to another sumOption"  in {
      val monoid = new ScMapMonoid[Int, Int]()
      val map1 = MMap(Range(0, 1000).zipWithIndex: _*)
      val map2 = MMap(Range(0, 1000).zipWithIndex: _*)
      val res1 = time("ScMapMonoid mutable sumOption another", monoid.sumOption(List(map1, map2))).get // time: 104.189ms
      res1 must_== MMap(Range(1, 1000).zip(Range(2, 2000, 2)): _*)
    }

    "be OK non mutable sumOption" in {
      val monoid = new ScMapMonoid[Int, Int]()
      val map1 = Map(Range(0, 1000).zipWithIndex: _*)
      val map2 = Map(Range(0, 1000).zipWithIndex: _*)
      val res1 = time("ScMapMonoid immutable sumOption another", monoid.sumOption(List(map1, map2))).get // time: 24.598ms
      res1 must_== Map(Range(1, 1000).zip(Range(2, 2000, 2)): _*)
    }
  }
  "A MapMonoid" should {
    "be OK add" in {
      val monoid = new MapMonoid[Int, Int]()
      val map1 = Map(Range(0, 1000).zipWithIndex: _*)
      val map2 = Map(Range(0, 1000).zipWithIndex: _*)
      val res1 = time("MapMonoid immutable add another", monoid.plus(map1, map2)) // time: 24.598ms
      res1 must_== Map(Range(1, 1000).zip(Range(2, 2000, 2)): _*)
    }

    "be OK sumOption" in {
      val monoid = new MapMonoid[Int, Int]()
      val map1 = Map(Range(0, 1000).zipWithIndex: _*)
      val map2 = Map(Range(0, 1000).zipWithIndex: _*)
      val res1 = time("MapMonoid immutable sumOption another", monoid.sumOption(List(map1, map2))).get // time: 24.598ms
      res1 must_== Map(Range(1, 1000).zip(Range(2, 2000, 2)): _*)
    }
  }
  "BenchMark" should {
    "be OK" in {
      val map1 = Map(Range(0, 1000).zipWithIndex: _*)
      val map2 = Map(Range(0, 1000).zipWithIndex: _*)
      val res1 = time("Benchmark", map1.values.map(value => (value, map1(value) + map2(value))).toMap) // time: 28.177ms
      res1 must_== Map(Range(0, 1000).zip(Range(0, 2000, 2)): _*)
    }
  }
}
