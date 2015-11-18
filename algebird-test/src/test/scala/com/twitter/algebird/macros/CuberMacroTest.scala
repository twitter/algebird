package com.twitter.algebird.macros

import com.twitter.algebird._
import com.twitter.algebird.macros.ArbitraryCaseClassMacro.arbitrary

import org.scalacheck.Arbitrary

import org.scalatest.{ PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks

class CuberRollerProperties extends PropSpec with PropertyChecks with Matchers {
  import BaseProperties._

  implicit val arbitraryFoo: Arbitrary[Foo] = arbitrary[Foo]
  implicit val arbitraryBar: Arbitrary[Bar] = arbitrary[Bar]
  implicit val arbitraryBaz: Arbitrary[Baz] = arbitrary[Baz]
  implicit def arbitraryCaseClassWithTypeParams[T: Arbitrary]: Arbitrary[CaseClassWithTypeParams[T]] =
    arbitrary[CaseClassWithTypeParams[T]]

  case class Foo(a: Int, b: Option[String], c: Long)
  case class Bar(a: Boolean, foo: Foo)
  case class Baz(a: Int)
  case class CaseClassWithTypeParams[T](a: T, b: Option[String], c: Long)

  property("Cuber works for Foo"){
    forAll { f: Foo =>
      assert(Cuber.cuber(f) == List(
        (None, None, None),
        (Some(f.a), None, None),
        (None, Some(f.b), None),
        (Some(f.a), Some(f.b), None),
        (None, None, Some(f.c)),
        (Some(f.a), None, Some(f.c)),
        (None, Some(f.b), Some(f.c)),
        (Some(f.a), Some(f.b), Some(f.c))))
    }
  }

  property("Cuber works for Bar") {
    forAll { b: Bar =>
      assert(Cuber.cuber(b) == List(
        (None, None),
        (Some(b.a), None),
        (None, Some(b.foo)),
        (Some(b.a), Some(b.foo))))
    }
  }

  property("Cuber works for Baz") {
    forAll { b: Baz =>
      assert(Cuber.cuber(b) == List(
        Tuple1(None),
        Tuple1(Some(b.a))))
    }
  }

  property("Cuber works for CaseClassWithTypeParams"){
    forAll { f: CaseClassWithTypeParams[String] =>
      assert(Cuber.cuber(f) == List(
        (None, None, None),
        (Some(f.a), None, None),
        (None, Some(f.b), None),
        (Some(f.a), Some(f.b), None),
        (None, None, Some(f.c)),
        (Some(f.a), None, Some(f.c)),
        (None, Some(f.b), Some(f.c)),
        (Some(f.a), Some(f.b), Some(f.c))))
    }
  }

  property("Cuber works for (Int, Option[String])") {
    forAll { b: (Int, Option[String]) =>
      assert(Cuber.cuber(b) == List(
        (None, None),
        (Some(b._1), None),
        (None, Some(b._2)),
        (Some(b._1), Some(b._2))))
    }
  }

  property("Roller works for Foo") {
    forAll { f: Foo =>
      assert(Roller.roller(f) == List(
        (None, None, None),
        (Some(f.a), None, None),
        (Some(f.a), Some(f.b), None),
        (Some(f.a), Some(f.b), Some(f.c))))
    }
  }

  property("Roller works for Bar") {
    forAll { b: Bar =>
      assert(Roller.roller(b) == List(
        (None, None),
        (Some(b.a), None),
        (Some(b.a), Some(b.foo))))
    }
  }

  property("Roller works for Baz") {
    forAll { b: Baz =>
      assert(Roller.roller(b) == List(
        Tuple1(None),
        Tuple1(Some(b.a))))
    }
  }

  property("Roller works for CaseClassWithTypeParams"){
    forAll { f: CaseClassWithTypeParams[String] =>
      assert(Roller.roller(f) == List(
        (None, None, None),
        (Some(f.a), None, None),
        (Some(f.a), Some(f.b), None),
        (Some(f.a), Some(f.b), Some(f.c))))
    }
  }

  property("Roller works for (Int, Option[String])") {
    forAll { b: (Int, Option[String]) =>
      assert(Roller.roller(b) == List(
        (None, None),
        (Some(b._1), None),
        (Some(b._1), Some(b._2))))
    }
  }

  property("MapAlgebra.cube works for (Int, Option[String])") {
    import com.twitter.algebird.macros.Cuber._

    val list = List(
      Foo(1, Some("hi"), 1),
      Foo(1, Some("hi"), 2),
      Foo(2, Some("hi"), 3),
      Foo(1, None, 4),
      Foo(2, None, 5),
      Foo(2, None, 6))
    val expected = Map(
      (Some(1), Some(Some("hi"))) -> List(1, 2),
      (Some(1), Some(None)) -> List(4),
      (Some(2), Some(Some("hi"))) -> List(3),
      (Some(2), Some(None)) -> List(5, 6),
      (None, Some(Some("hi"))) -> List(1, 2, 3),
      (None, Some(None)) -> List(4, 5, 6),
      (None, None) -> List(1, 2, 3, 4, 5, 6),
      (Some(1), None) -> List(1, 2, 4),
      (Some(2), None) -> List(3, 5, 6))

    val pairs = list.map { f => ((f.a, f.b), f.c) }
    assert(MapAlgebra.cube(pairs) == expected)
  }

  property("MapAlgebra.cubeSum works for (Int, Option[String])") {
    import com.twitter.algebird.macros.Cuber._

    val list = List(
      Foo(1, Some("hi"), 1),
      Foo(1, Some("hi"), 2),
      Foo(2, Some("hi"), 3),
      Foo(1, None, 4),
      Foo(2, None, 5),
      Foo(2, None, 6))
    val expected = Map(
      (Some(1), Some(Some("hi"))) -> List(1, 2).sum,
      (Some(1), Some(None)) -> List(4).sum,
      (Some(2), Some(Some("hi"))) -> List(3).sum,
      (Some(2), Some(None)) -> List(5, 6).sum,
      (None, Some(Some("hi"))) -> List(1, 2, 3).sum,
      (None, Some(None)) -> List(4, 5, 6).sum,
      (None, None) -> List(1, 2, 3, 4, 5, 6).sum,
      (Some(1), None) -> List(1, 2, 4).sum,
      (Some(2), None) -> List(3, 5, 6).sum)

    val pairs = list.map { f => ((f.a, f.b), f.c) }
    assert(MapAlgebra.cubeSum(pairs) == expected)
  }

  property("MapAlgebra.cubeAggregate works for (Int, Option[String])") {
    import com.twitter.algebird.macros.Cuber._

    val list = List(
      Foo(1, Some("hi"), 1),
      Foo(1, Some("hi"), 2),
      Foo(2, Some("hi"), 3),
      Foo(1, None, 4),
      Foo(2, None, 5),
      Foo(2, None, 6))
    val expected = Map(
      (Some(1), Some(Some("hi"))) -> List(1, 2).mkString("").toLong,
      (Some(1), Some(None)) -> List(4).mkString("").toLong,
      (Some(2), Some(Some("hi"))) -> List(3).mkString("").toLong,
      (Some(2), Some(None)) -> List(5, 6).mkString("").toLong,
      (None, Some(Some("hi"))) -> List(1, 2, 3).mkString("").toLong,
      (None, Some(None)) -> List(4, 5, 6).mkString("").toLong,
      (None, None) -> List(1, 2, 3, 4, 5, 6).mkString("").toLong,
      (Some(1), None) -> List(1, 2, 4).mkString("").toLong,
      (Some(2), None) -> List(3, 5, 6).mkString("").toLong)

    val aggregator = new Aggregator[Foo, String, Long] {
      def prepare(foo: Foo) = foo.c.toString
      def semigroup = implicitly[Semigroup[String]]
      def present(s: String) = s.toLong
    }

    def foo2Key(f: Foo) = (f.a, f.b)
    assert(MapAlgebra.cubeAggregate(list, aggregator)(foo2Key) == expected)
  }

  property("MapAlgebra.rollup works for (Int, Option[String])") {
    import com.twitter.algebird.macros.Roller._

    val list = List(
      Foo(1, Some("hi"), 1),
      Foo(1, Some("hi"), 2),
      Foo(2, Some("hi"), 3),
      Foo(1, None, 4),
      Foo(2, None, 5),
      Foo(2, None, 6))
    val expected = Map(
      (Some(1), Some(Some("hi"))) -> List(1, 2),
      (Some(1), Some(None)) -> List(4),
      (Some(2), Some(Some("hi"))) -> List(3),
      (Some(2), Some(None)) -> List(5, 6),
      (Some(1), None) -> List(1, 2, 4),
      (Some(2), None) -> List(3, 5, 6),
      (None, None) -> List(1, 2, 3, 4, 5, 6))

    val pairs = list.map { f => ((f.a, f.b), f.c) }
    assert(MapAlgebra.rollup(pairs) == expected)
  }

  property("MapAlgebra.rollupSum works for (Int, Option[String])") {
    import com.twitter.algebird.macros.Roller._

    val list = List(
      Foo(1, Some("hi"), 1),
      Foo(1, Some("hi"), 2),
      Foo(2, Some("hi"), 3),
      Foo(1, None, 4),
      Foo(2, None, 5),
      Foo(2, None, 6))
    val expected = Map(
      (Some(1), Some(Some("hi"))) -> List(1, 2).sum,
      (Some(1), Some(None)) -> List(4).sum,
      (Some(2), Some(Some("hi"))) -> List(3).sum,
      (Some(2), Some(None)) -> List(5, 6).sum,
      (Some(1), None) -> List(1, 2, 4).sum,
      (Some(2), None) -> List(3, 5, 6).sum,
      (None, None) -> List(1, 2, 3, 4, 5, 6).sum)

    val pairs = list.map { f => ((f.a, f.b), f.c) }
    assert(MapAlgebra.rollupSum(pairs) == expected)
  }

  property("MapAlgebra.rollupAggregate works for (Int, Option[String])") {
    import com.twitter.algebird.macros.Roller._

    val list = List(
      Foo(1, Some("hi"), 1),
      Foo(1, Some("hi"), 2),
      Foo(2, Some("hi"), 3),
      Foo(1, None, 4),
      Foo(2, None, 5),
      Foo(2, None, 6))
    val expected = Map(
      (Some(1), Some(Some("hi"))) -> List(1, 2).mkString("").toLong,
      (Some(1), Some(None)) -> List(4).mkString("").toLong,
      (Some(2), Some(Some("hi"))) -> List(3).mkString("").toLong,
      (Some(2), Some(None)) -> List(5, 6).mkString("").toLong,
      (Some(1), None) -> List(1, 2, 4).mkString("").toLong,
      (Some(2), None) -> List(3, 5, 6).mkString("").toLong,
      (None, None) -> List(1, 2, 3, 4, 5, 6).mkString("").toLong)

    val aggregator = new Aggregator[Foo, String, Long] {
      def prepare(foo: Foo) = foo.c.toString
      def semigroup = implicitly[Semigroup[String]]
      def present(s: String) = s.toLong
    }

    def foo2Key(f: Foo) = (f.a, f.b)
    assert(MapAlgebra.rollupAggregate(list, aggregator)(foo2Key) == expected)
  }
}
