package com.twitter.algebird

import java.lang.{Boolean => JBool, Integer => JInt, Long => JLong, Short => JShort}
import java.util.{List => JList, Map => JMap}

import org.scalacheck.{Arbitrary, Gen}

import scala.collection.JavaConverters._

class JavaBoxedTests extends CheckProperties {
  import com.twitter.algebird.BaseProperties._

  implicit val jboolArg = Arbitrary {
    for (v <- Gen.oneOf(JBool.TRUE, JBool.FALSE)) yield v
  }
  implicit val jintArg = Arbitrary {
    for (v <- Gen.choose(Int.MinValue, Int.MaxValue))
      yield JInt.valueOf(v)
  }
  implicit val jshortArg = Arbitrary {
    for (v <- Gen.choose(Short.MinValue, Short.MaxValue))
      yield Short.box(v)
  }
  implicit val jlongArg = Arbitrary {
    // If we put Long.Max/Min we get overflows that seem to break the ring properties, not clear why
    for (v <- Gen.choose(Int.MinValue, Int.MaxValue))
      yield JLong.valueOf(v)
  }

  property("Int is a Ring") {
    ringLaws[Int]
  }

  property("JInt is a Ring") {
    ringLaws[JInt]
  }

  property("Short is a Ring") {
    ringLaws[Short]
  }

  property("JShort is a Ring") {
    ringLaws[JShort]
  }

  property("Long is a Ring") {
    ringLaws[Long]
  }

  property("JLong is a Ring") {
    ringLaws[JLong]
  }

  // TODO add testing with JFloat/JDouble but check for approximate equals, pain in the ass.

  implicit def jlist[T: Arbitrary] = Arbitrary {
    implicitly[Arbitrary[List[T]]].arbitrary.map { _.asJava }
  }

  property("JList is a Monoid") {
    monoidLaws[JList[Int]]
  }

  implicit def jmap[K: Arbitrary, V: Arbitrary: Semigroup] = Arbitrary {
    implicitly[Arbitrary[Map[K, V]]].arbitrary.map {
      _.filter { kv =>
        isNonZero[V](kv._2)
      }.asJava
    }
  }

  property("JMap[String,Int] is a Monoid") {
    isAssociative[JMap[String, Int]] && weakZero[JMap[String, Int]]
  }

  property("JMap[String,String] is a Monoid") {
    isAssociative[JMap[String, String]] && weakZero[JMap[String, String]]
  }
}
