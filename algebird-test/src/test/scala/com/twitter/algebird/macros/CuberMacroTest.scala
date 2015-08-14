package com.twitter.algebird.macros

import com.twitter.algebird._
import com.twitter.algebird.macros._
import com.twitter.algebird.macros.ArbitraryCaseClassMacro.arbitrary

import org.scalacheck.{ Properties, Arbitrary }
import org.scalacheck.Prop.forAll

object CuberRollerProperties extends Properties("Cuber and roller macro") {
  import BaseProperties._

  implicit val arbitraryFoo: Arbitrary[Foo] = arbitrary[Foo]
  implicit val arbitraryBar: Arbitrary[Bar] = arbitrary[Bar]
  implicit val arbitraryBaz: Arbitrary[Baz] = arbitrary[Baz]

  case class Foo(a: Int, b: Option[String], c: Long)
  case class Bar(a: Boolean, foo: Foo)
  case class Baz(a: Int)

  property("Cuber works for Foo") = forAll { f: Foo =>
    Cuber.cuber(f).toSet == Set(
      (None, None, None),
      (Some(f.a), None, None),
      (None, Some(f.b), None),
      (None, None, Some(f.c)),
      (Some(f.a), Some(f.b), None),
      (Some(f.a), None, Some(f.c)),
      (None, Some(f.b), Some(f.c)),
      (Some(f.a), Some(f.b), Some(f.c)))
  }

  property("Cuber works for Bar") = forAll { b: Bar =>
    Cuber.cuber(b).toSet == Set(
      (None, None),
      (Some(b.a), None),
      (None, Some(b.foo)),
      (Some(b.a), Some(b.foo)))
  }

  property("Cuber works for Baz") = forAll { b: Baz =>
    Cuber.cuber(b).toSet == Set(
      Tuple1(None),
      Tuple1(Some(b.a)))
  }

  property("Roller works for Foo") = forAll { f: Foo =>
    Roller.roller(f).toSet == Set(
      (None, None, None),
      (Some(f.a), None, None),
      (Some(f.a), Some(f.b), None),
      (Some(f.a), Some(f.b), Some(f.c)))
  }

  property("Roller works for Bar") = forAll { b: Bar =>
    Roller.roller(b).toSet == Set(
      (None, None),
      (Some(b.a), None),
      (Some(b.a), Some(b.foo)))
  }

  property("Roller works for Baz") = forAll { b: Baz =>
    Roller.roller(b).toSet == Set(
      Tuple1(None),
      Tuple1(Some(b.a)))
  }
}
