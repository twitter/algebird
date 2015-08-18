package com.twitter.algebird.macros

import com.twitter.algebird._
import com.twitter.algebird.macros._
import com.twitter.algebird.macros.ArbitraryCaseClassMacro.arbitrary

import org.scalacheck.Arbitrary

import org.scalatest.{ PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks

class CuberRollerProperties extends PropSpec with PropertyChecks with Matchers {
  import BaseProperties._

  implicit val arbitraryFoo: Arbitrary[Foo] = arbitrary[Foo]
  implicit val arbitraryBar: Arbitrary[Bar] = arbitrary[Bar]
  implicit val arbitraryBaz: Arbitrary[Baz] = arbitrary[Baz]

  case class Foo(a: Int, b: Option[String], c: Long)
  case class Bar(a: Boolean, foo: Foo)
  case class Baz(a: Int)

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
}
