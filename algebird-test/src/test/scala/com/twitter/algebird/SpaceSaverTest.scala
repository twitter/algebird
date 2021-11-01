package com.twitter.algebird

import org.scalacheck.Prop._
import org.scalacheck.{Arbitrary, Gen}

import scala.util.Try
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SpaceSaverLaws extends CheckProperties {

  import com.twitter.algebird.BaseProperties._
  import com.twitter.algebird.scalacheck.arbitrary._

  // limit sizes to 100 to avoid large data structures in tests
  property("SpaceSaver is a Semigroup") {
    forAll(Gen.choose(2, 100)) { capacity =>
      forAll(Gen.choose(1, 100)) { range =>
        // need a non-uniform distro
        implicit val ssGenOne: Arbitrary[SSOne[Int]] = Arbitrary {
          for (key <- Gen.frequency((1 to range).map(x => (x * x, x: Gen[Int])): _*))
            yield SpaceSaver(capacity, key).asInstanceOf[SSOne[Int]]
        }

        implicit def ssGen(implicit sg: Semigroup[SpaceSaver[Int]]): Arbitrary[SpaceSaver[Int]] = Arbitrary {
          Gen.oneOf(
            Arbitrary.arbitrary[SSOne[Int]],
            Gen
              .nonEmptyContainerOf[List, SSOne[Int]](Arbitrary.arbitrary[SSOne[Int]])
              .map(_.reduce(sg.plus))
          )
        }

        implicit def equiv[T]: Equiv[SpaceSaver[T]] =
          Equiv.fromFunction((left, right) => (left.consistentWith(right)) && (right.consistentWith(left)))

        commutativeSemigroupLaws[SpaceSaver[Int]]
      }
    }
  }

  property("SpaceSaver can serialize/deserialize itself") {
    forAll { ss: SpaceSaver[String] =>
      val ssAsBytes = SpaceSaver.toBytes(ss, SpaceSaverTest.stringToArrayByte)
      SpaceSaver
        .fromBytes(ssAsBytes, SpaceSaverTest.arrayByteToString)
        .get == ss
    }
  }

  property("SpaceSaver.fromBytes yield a failure on bad Array[Byte]") {
    forAll { a: Array[Byte] =>
      try {
        val fromBytes =
          SpaceSaver.fromBytes(a, SpaceSaverTest.arrayByteToString)
        // We check that `fromBytes` doesn't yield exceptions
        fromBytes.isFailure || fromBytes.isSuccess
      } catch {
        case _: OutOfMemoryError =>
          true // this happens if random data has a giant number in it
      }
    }
  }
}

class SpaceSaverTest extends AnyWordSpec with Matchers {

  "SpaceSaver" should {
    "produce a top 20 with exact bounds" in {
      val gen = Gen.frequency((1 to 100).map(x => (x * x, x: Gen[Int])): _*)
      val items = (1 to 1000).map(_ => gen.sample.get)
      val exactCounts = items.groupBy(identity).mapValues(_.size)

      // simulate a distributed system with 10 mappers and 1 reducer
      val sg = new SpaceSaverSemigroup[Int]
      items
        .grouped(10)
        .map {
          _.iterator
            .map(SpaceSaver(40, _))
            .reduce(sg.plus)
        }
        .reduce(sg.plus)
        .topK(20)
        .foreach { case (item, approx, _) =>
          assert(approx ~ exactCounts(item))
        }
    }

    "be consistent in its constructors" in {
      assert(SpaceSaver(10, "ha") ++ SpaceSaver(10, "ha") ++ SpaceSaver(10, "ha") === SpaceSaver(10, "ha", 3))
    }

    "Serialize should not change in time" in {
      val ssOne = SpaceSaver(40, "string that is not empty")
      val ssOneBytes =
        SpaceSaver.toBytes(ssOne, SpaceSaverTest.stringToArrayByte)

      val ssMany = SpaceSaver(40, "another string that is not empty", 100)
      val ssManyBytes =
        SpaceSaver.toBytes(ssMany, SpaceSaverTest.stringToArrayByte)

      ssOneBytes shouldEqual Array(1, 0, 0, 0, 40, 0, 0, 0, 24, 115, 116, 114, 105, 110, 103, 32, 116, 104,
        97, 116, 32, 105, 115, 32, 110, 111, 116, 32, 101, 109, 112, 116, 121)
      ssManyBytes shouldEqual Array(2, 0, 0, 0, 40, 0, 0, 0, 1, 0, 0, 0, 32, 97, 110, 111, 116, 104, 101, 114,
        32, 115, 116, 114, 105, 110, 103, 32, 116, 104, 97, 116, 32, 105, 115, 32, 110, 111, 116, 32, 101,
        109, 112, 116, 121, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0)
    }
  }
}

object SpaceSaverTest {

  def stringToArrayByte(s: String): Array[Byte] = s.getBytes("UTF-8")

  def arrayByteToString(a: Array[Byte]): Try[String] =
    Try(new String(a, "UTF-8"))

}
