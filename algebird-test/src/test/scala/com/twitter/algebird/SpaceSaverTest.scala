package com.twitter.algebird

import org.scalacheck.Prop._
import org.scalacheck.{ Arbitrary, Gen }
import org.scalatest._

class SpaceSaverLaws extends CheckProperties {
  import com.twitter.algebird.BaseProperties._

  // limit sizes to 100 to avoid large data structures in tests
  property("SpaceSaver is a Semigroup") {
    forAll(Gen.choose(2, 100)){ capacity =>
      forAll(Gen.choose(1, 100)){ range =>

        // need a non-uniform distro
        implicit val ssGenOne: Arbitrary[SSOne[Int]] = Arbitrary {
          for (key <- Gen.frequency((1 to range).map{ x => (x * x, x: Gen[Int]) }: _*)) yield SpaceSaver(capacity, key).asInstanceOf[SSOne[Int]]
        }
        implicit def ssGen(implicit sg: Semigroup[SpaceSaver[Int]]): Arbitrary[SpaceSaver[Int]] = Arbitrary {
          Gen.oneOf(
            Arbitrary.arbitrary[SSOne[Int]],
            Gen.nonEmptyContainerOf[List, SSOne[Int]](Arbitrary.arbitrary[SSOne[Int]]).map(_.reduce(sg.plus)))
        }

        commutativeSemigroupLawsEq[SpaceSaver[Int]] { (left, right) => (left consistentWith right) && (right consistentWith left) }
      }
    }
  }
}

class SpaceSaverTest extends WordSpec with Matchers {
  "SpaceSaver" should {
    "produce a top 20 with exact bounds" in {
      val gen = Gen.frequency((1 to 100).map{ x => (x * x, x: Gen[Int]) }: _*)
      val items = (1 to 1000).map{ x => gen.sample.get }
      val exactCounts = items.groupBy(identity).mapValues(_.size)

      // simulate a distributed system with 10 mappers and 1 reducer
      val sg = new SpaceSaverSemigroup[Int]
      items.grouped(10).map{
        _
          .iterator
          .map(SpaceSaver(40, _))
          .reduce(sg.plus)
      }
        .reduce(sg.plus)
        .topK(20)
        .foreach {
          case (item, approx, guarantee) =>
            // println("item " + item + " : " + approx.min + " <= " + exactCounts(item) + " <= " + approx.max)
            assert(approx ~ exactCounts(item))
        }
    }

    "be consistent in its constructors" in {
      assert(SpaceSaver(10, "ha") ++ SpaceSaver(10, "ha") ++ SpaceSaver(10, "ha") === SpaceSaver(10, "ha", 3))
    }
  }
}
