package com.twitter.algebird

import org.scalacheck._
import org.scalacheck.Prop._
import org.scalatest.{ WordSpec, Matchers }
import org.scalatest.prop.Checkers

import Arbitrary.arbitrary

object DiffTest {
  implicit def arbDiff[T: Arbitrary]: Arbitrary[Diff[T]] =
    Arbitrary(arbitrary[(Set[T], Set[T])].map { case (a, b) => Diff.of(a, b) })
}

class DiffTest extends WordSpec with Matchers with Checkers {
  import DiffTest._

  "Diff" should {
    "be a monoid" in {
      check(BaseProperties.monoidLaws[Diff[Int]])
    }
    "be idempotent" in {
      check { (d: Diff[Int]) =>
        d.merge(d) == d
      }
    }

    /**
     * This is the core law, along with associativity that allows
     * us to reason about set Diffs.
     */
    "Diffs are the same as updating the set" in {
      check { (init: Set[Int], items: List[Either[Int, Int]]) =>
        val updated = items.foldLeft(init) {
          case (s, Left(i)) => s - i
          case (s, Right(i)) => s + i
        }
        val diff = Monoid.sum(items.map {
          case Left(i) => Diff.remove(i)
          case Right(i) => Diff.add(i)
        })
        updated == diff(init)
      }
    }
    "+ is the same as Diff.add" in {
      check { (d: Diff[Int], inc: Int) =>
        d + inc == (d merge (Diff.add(inc)))
      }
    }
    "- is the same as Diff.remove" in {
      check { (d: Diff[Int], dec: Int) =>
        d - dec == (d merge (Diff.remove(dec)))
      }
    }
    "++ is the same as Diff.addAll" in {
      check { (d: Diff[Int], inc: Set[Int]) =>
        d ++ inc == (d merge (Diff.addAll(inc)))
      }
    }
    "-- is the same as Diff.removeAll" in {
      check { (d: Diff[Int], dec: Set[Int]) =>
        d -- dec == (d merge (Diff.removeAll(dec)))
      }
    }
    "+ then - is the same as -" in {
      check { (i: Int) =>
        (Diff.add(i) merge Diff.remove(i)) == Diff.remove(i)
      }
    }
    "- then + is the same as +" in {
      check { (i: Int) =>
        (Diff.remove(i) merge Diff.add(i)) == Diff.add(i)
      }
    }

    "apply diffs between sets" in {
      check { (oldSet: Set[String], newSet: Set[String]) =>
        Diff.of(oldSet, newSet)(oldSet) == newSet
      }
    }

    "create proper diffs" in {
      check { (oldSet: Set[String], newSet: Set[String]) =>
        val diff = Diff.of(oldSet, newSet)
        (diff.add &~ newSet).isEmpty && (diff.remove & newSet).isEmpty
      }
    }

    "never intersect the add and remove sets by construction" in {
      check { (ops: List[Either[Int, Int]]) =>
        val built = ops.foldLeft(Diff.empty[Int]) {
          case (diff, Left(remove)) => diff - remove
          case (diff, Right(add)) => diff + add
        }
        (built.add & built.remove).isEmpty
      }
    }

    "apply distributes over merge" in {
      check { (init: Set[Int], a: Diff[Int], b: Diff[Int]) =>
        (a merge b)(init) == b(a(init))
      }
    }

    "strict application fails if the diff tries to remove extra items" in {
      check { (set: Set[Int], a: Diff[Int]) =>
        a.strictApply(set) match {
          case None =>
            ((a.remove diff set).nonEmpty || (a.add & set).nonEmpty) &&
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
      check { (a: Set[Int], diff: Diff[Int]) =>
        (diff.invert(diff(a)) != a) || diff.strictApply(a).isDefined
      }
    }

    "Valid diffs are invertible" in {
      check { (a: Set[Int], b: Set[Int]) =>
        val diff = Diff.of(a, b)
        // we know that diff(a) == b from a law above
        a == diff.invert(b)
      }
    }
  }
}
