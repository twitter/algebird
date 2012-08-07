package com.twitter.algebird

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Gen.choose
import org.scalacheck.Gen.oneOf

import java.lang.{Integer => JInt, Long => JLong, Float => JFloat, Double => JDouble, Boolean => JBool}
import java.util.{List => JList, Map => JMap}

import scala.collection.JavaConverters._

object JavaBoxedTests extends Properties("JavaBoxed") with BaseProperties {

  implicit val jboolArg = Arbitrary { for( v <- oneOf(JBool.TRUE, JBool.FALSE) ) yield v }
  implicit val jintArg = Arbitrary {
    for( v <- choose(Int.MinValue,Int.MaxValue) )
      yield JInt.valueOf(v)
  }
  implicit val jlongArg = Arbitrary {
    // If we put Long.Max/Min we get overflows that seem to break the ring properties, not clear why
    for( v <- choose(Int.MinValue, Int.MaxValue) )
      yield JLong.valueOf(v)
  }

  property("Boolean is a Field") = fieldLaws[Boolean]
  property("JBoolean is a Field") = fieldLaws[JBool]
  property("Int is a Ring") = ringLaws[Int]
  property("JInt is a Ring") = ringLaws[JInt]
  property("Long is a Ring") = ringLaws[Long]
  property("JLong is a Ring") = ringLaws[JLong]
  // TODO add testing with JFloat/JDouble but check for approximate equals, pain in the ass.

  implicit def jlist[T : Arbitrary] = Arbitrary {
    implicitly[Arbitrary[List[T]]].arbitrary.map { _.asJava }
  }
  property("JList is a Monoid") = monoidLaws[JList[Int]]

  implicit def jmap[K : Arbitrary, V : Arbitrary : Monoid] = Arbitrary {
    val mon = implicitly[Monoid[V]]
    implicitly[Arbitrary[Map[K,V]]].arbitrary.map { _.filter { kv => mon.isNonZero(kv._2) }.asJava }
  }
  property("JMap[String,Int] is a Monoid") = isAssociative[JMap[String,Int]] && weakZero[JMap[String,Int]]
  property("JMap[String,String] is a Monoid") = isAssociative[JMap[String,String]] && weakZero[JMap[String,String]]
}
