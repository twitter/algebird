package com.twitter.algebird

import org.scalacheck._
import org.scalacheck.Prop._
import org.scalatest.{Matchers, WordSpec}
import org.scalatestplus.scalacheck.Checkers

import Arbitrary.arbitrary

object SetDiffTest {
  implicit def arbSetDiff[T: Arbitrary]: Arbitrary[SetDiff[T]] =
    Arbitrary(arbitrary[(Set[T], Set[T])].map {
      case (a, b) => SetDiff.of(a, b)
    })
}

class SetDiffTest extends WordSpec with Matchers with Checkers {
  import SetDiffTest._

  "SetDiff" should {
    "be a monoid" in {
      check(BaseProperties.monoidLaws[SetDiff[Int]])
    }
    "be idempotent" in {
      check { (d: SetDiff[Int]) =>
        d.merge(d) == d
      }
    }

    /**
     * This is the core law, along with associativity that allows
     * us to reason about set SetDiffs.
     */
    "SetDiffs are the same as updating the set" in {
      check { (init: Set[Int], items: List[Either[Int, Int]]) =>
        val updated = items.foldLeft(init) {
          case (s, Left(i))  => s - i
          case (s, Right(i)) => s + i
        }
        val diff = Monoid.sum(items.map {
          case Left(i)  => SetDiff.remove(i)
          case Right(i) => SetDiff.add(i)
        })
        updated == diff(init)
      }
    }
    "+ is the same as SetDiff.add" in {
      check { (d: SetDiff[Int], inc: Int) =>
        d + inc == (d.merge(SetDiff.add(inc)))
      }
    }
    "- is the same as SetDiff.remove" in {
      check { (d: SetDiff[Int], dec: Int) =>
        d - dec == (d.merge(SetDiff.remove(dec)))
      }
    }
    "++ is the same as SetDiff.addAll" in {
      check { (d: SetDiff[Int], inc: Set[Int]) =>
        d ++ inc == (d.merge(SetDiff.addAll(inc)))
      }
    }
    "-- is the same as SetDiff.removeAll" in {
      check { (d: SetDiff[Int], dec: Set[Int]) =>
        d -- dec == (d.merge(SetDiff.removeAll(dec)))
      }
    }
    "+ then - is the same as -" in {
      check { (i: Int) =>
        (SetDiff.add(i).merge(SetDiff.remove(i))) == SetDiff.remove(i)
      }
    }
    "- then + is the same as +" in {
      check { (i: Int) =>
        (SetDiff.remove(i).merge(SetDiff.add(i))) == SetDiff.add(i)
      }
    }

    "apply diffs between sets" in {
      check { (oldSet: Set[String], newSet: Set[String]) =>
        SetDiff.of(oldSet, newSet)(oldSet) == newSet
      }
    }

    "create proper diffs" in {
      check { (oldSet: Set[String], newSet: Set[String]) =>
        val diff = SetDiff.of(oldSet, newSet)
        (diff.add &~ newSet).isEmpty && (diff.remove & newSet).isEmpty
      }
    }

    "never intersect the add and remove sets by construction" in {
      check { (ops: List[Either[Int, Int]]) =>
        val built = ops.foldLeft(SetDiff.empty[Int]) {
          case (diff, Left(remove)) => diff - remove
          case (diff, Right(add))   => diff + add
        }
        (built.add & built.remove).isEmpty
      }
    }

    "apply distributes over merge" in {
      check { (init: Set[Int], a: SetDiff[Int], b: SetDiff[Int]) =>
        a.merge(b)(init) == b(a(init))
      }
    }

    "strict application fails if the diff tries to remove extra items" in {
      check { (set: Set[Int], a: SetDiff[Int]) =>
        a.strictApply(set) match {
          case None =>
            (a.remove.diff(set).nonEmpty || (a.add & set).nonEmpty) &&
              (a.invert(a(set)) != set) // invert only succeeds when strictApply does
          /*
           * And if it DOES succeed it inverts!
           * Note that this law is not true:
           * d.invert(d(init)) == init
           */
          case Some(newSet) =>
            (a(set) == newSet) && (a.invert(newSet) == set)
        }
      }
    }
    "if diff.invert(diff(a)) == a implies diff.strictApply(a).isDefined" in {
      check { (a: Set[Int], diff: SetDiff[Int]) =>
        (diff.invert(diff(a)) != a) || diff.strictApply(a).isDefined
      }
    }

    "Valid diffs are invertible" in {
      check { (a: Set[Int], b: Set[Int]) =>
        val diff = SetDiff.of(a, b)
        // we know that diff(a) == b from a law above
        a == diff.invert(b)
      }
    }
  }
}
