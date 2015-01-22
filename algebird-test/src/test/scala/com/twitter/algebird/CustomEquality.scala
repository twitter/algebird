package com.twitter.algebird

import org.scalactic.Equality

trait CustomEquality {

  import scala.reflect.ClassTag

  /**
   * A custom [[Equality]] definition for [[Set]] that understands how to compare [[Array]] elements in the set.
   *
   * To make use of this custom matcher for [[Set]] you must `import CustomEquality._` and
   * then use `set1 should equal(set2)` instead of `set1 should be(set2)`.
   *
   * Note: Once ScalaTest 3.x is released this custom [[Equality]] definition will no longer be needed.
   * See https://github.com/scalatest/scalatest/issues/491 for details.
   */
  implicit def customEq[K: ClassTag]: Equality[Set[K]] = new Equality[Set[K]] {

    override def areEqual(left: Set[K], right: Any): Boolean = {
      val equal = right match {
        case r: Set[_] if implicitly[ClassTag[K]].runtimeClass.isArray =>
          val leftSeqs = left.map { _.asInstanceOf[Array[_]].toSeq }
          val rightSeqs = r.map { _.asInstanceOf[Array[_]].toSeq }
          leftSeqs == rightSeqs
        case _ => left == right
      }
      equal
    }

  }
}

// Make the custom equality definitions easy to import with: `import CustomEquality._`
object CustomEquality extends CustomEquality