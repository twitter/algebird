/*
Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package com.twitter.algebird

import org.scalacheck.Arbitrary
import org.scalacheck.Prop._

class AggregatorLaws extends CheckProperties {

  implicit def aggregator[A, B, C](implicit
      prepare: Arbitrary[A => B],
      sg: Semigroup[B],
      present: Arbitrary[B => C]
  ): Arbitrary[Aggregator[A, B, C]] = Arbitrary {
    for {
      pp <- prepare.arbitrary
      ps <- present.arbitrary
    } yield new Aggregator[A, B, C] {
      def prepare(a: A) = pp(a)
      def semigroup = sg
      def present(b: B) = ps(b)
    }
  }

  property("composing before Aggregator is correct") {
    forAll { (in: List[Int], compose: (Int => Int), ag: Aggregator[Int, Int, Int]) =>
      val composed = ag.composePrepare(compose)
      in.isEmpty || composed(in) == ag(in.map(compose))
    }
  }

  property("andThen after Aggregator is correct") {
    forAll { (in: List[Int], andt: (Int => Int), ag: Aggregator[Int, Int, Int]) =>
      val ag1 = ag.andThenPresent(andt)
      in.isEmpty || ag1(in) == andt(ag(in))
    }
  }

  property("composing two Aggregators is correct") {
    forAll { (in: List[Int], ag1: Aggregator[Int, String, Int], ag2: Aggregator[Int, Int, String]) =>
      val c = GeneratedTupleAggregator.from2((ag1, ag2))
      in.isEmpty || c(in) == ((ag1(in), ag2(in)))
    }
  }

  property("Applicative composing two Aggregators is correct") {
    forAll { (in: List[Int], ag1: Aggregator[Int, Set[Int], Int], ag2: Aggregator[Int, Unit, String]) =>
      type AggInt[T] = Aggregator[Int, _, T]
      val c = Applicative.join[AggInt, Int, String](ag1, ag2)
      in.isEmpty || c(in) == ((ag1(in), ag2(in)))
    }
  }

  property("Aggregator.zip composing two Aggregators is correct") {
    forAll {
      (
          in: List[(Int, String)],
          ag1: Aggregator[Int, Int, Int],
          ag2: Aggregator[String, Set[String], Double]
      ) =>
        val c = ag1.zip(ag2)
        val (as, bs) = in.unzip
        in.isEmpty || c(in) == ((ag1(as), ag2(bs)))
    }
  }

  property("Aggregator.lift works for empty sequences") {
    forAll { (in: List[Int], ag: Aggregator[Int, Int, Int]) =>
      val liftedAg = ag.lift
      in.isEmpty && liftedAg(in) == None || liftedAg(in) == Some(ag(in))
    }
  }

  def checkNumericSum[T: Arbitrary](implicit num: Numeric[T]) =
    forAll { in: List[T] =>
      val aggregator = Aggregator.numericSum[T]
      aggregator(in) == in.map(num.toDouble).sum
    }
  property("Aggregator.numericSum is correct for Ints")(checkNumericSum[Int])
  property("Aggregator.numericSum is correct for Longs") {
    checkNumericSum[Long]
  }
  property("Aggregator.numericSum is correct for Doubles") {
    checkNumericSum[Double]
  }
  property("Aggregator.numericSum is correct for Floats") {
    checkNumericSum[Float]
  }

  implicit def monoidAggregator[A, B, C](implicit
      prepare: Arbitrary[A => B],
      m: Monoid[B],
      present: Arbitrary[B => C]
  ): Arbitrary[MonoidAggregator[A, B, C]] =
    Arbitrary {
      for {
        pp <- prepare.arbitrary
        ps <- present.arbitrary
      } yield new MonoidAggregator[A, B, C] {
        def prepare(a: A) = pp(a)
        def monoid = m
        def present(b: B) = ps(b)
      }
    }

  property("Aggregator.count is like List.count") {
    forAll((in: List[Int], fn: Int => Boolean) => in.count(fn) == (Aggregator.count(fn)(in)))
  }
  property("Aggregator.exists is like List.exists") {
    forAll((in: List[Int], fn: Int => Boolean) => in.exists(fn) == (Aggregator.exists(fn)(in)))
  }
  property("Aggregator.forall is like List.forall") {
    forAll((in: List[Int], fn: Int => Boolean) => in.forall(fn) == (Aggregator.forall(fn)(in)))
  }
  property("Aggregator.head is like List.head") {
    forAll((in: List[Int]) => in.headOption == (Aggregator.head.applyOption(in)))
  }
  property("Aggregator.last is like List.last") {
    forAll((in: List[Int]) => in.lastOption == (Aggregator.last.applyOption(in)))
  }
  property("Aggregator.maxBy is like List.maxBy") {
    forAll { (head: Int, in: List[Int], fn: Int => Int) =>
      val nonempty = head :: in
      nonempty.maxBy(fn) == (Aggregator.maxBy(fn).apply(nonempty))
    }
  }
  property("Aggregator.minBy is like List.minBy") {
    forAll { (head: Int, in: List[Int], fn: Int => Int) =>
      val nonempty = head :: in
      nonempty.minBy(fn) == (Aggregator.minBy(fn).apply(nonempty))
    }
  }
  property("Aggregator.sortedTake same as List.sorted.take") {
    forAll { (in: List[Int], t0: Int) =>
      val t = math.max(t0, 1)
      val l = in.sorted.take(t)
      val a = Aggregator.sortedTake[Int](t).apply(in)
      l == a
    }
  }
  property("Aggregator.sortByTake same as List.sortBy(fn).take") {
    forAll { (in: List[Int], t0: Int, fn: Int => Int) =>
      val t = math.max(t0, 1)
      val l = in.sortBy(fn).take(t)
      val a = Aggregator.sortByTake(t)(fn).apply(in)
      // since we considered two things equivalent under fn,
      // we have to use that here:
      val ord = Ordering.Iterable(Ordering.by(fn))
      ord.equiv(l, a)
    }
  }
  property("Aggregator.sortByReverseTake same as List.sortBy(fn).reverse.take") {
    forAll { (in: List[Int], t0: Int, fn: Int => Int) =>
      val t = math.max(t0, 1)
      val l = in.sortBy(fn).reverse.take(t)
      val a = Aggregator.sortByReverseTake(t)(fn).apply(in)
      // since we considered two things equivalent under fn,
      // we have to use that here:
      val ord = Ordering.Iterable(Ordering.by(fn))
      ord.equiv(l, a)
    }
  }
  property("Aggregator.immutableSortedTake same as List.sorted.take") {
    forAll { (in: List[Int], t0: Int) =>
      val t = math.max(t0, 1)
      val l = in.sorted.take(t)
      val a = Aggregator.immutableSortedTake[Int](t).apply(in)
      l == a
    }
  }
  property("Aggregator.immutableSortedReverseTake same as List.sorted.reverse.take") {
    forAll { (in: List[Int], t0: Int) =>
      val t = math.max(t0, 1)
      val l = in.sorted.reverse.take(t)
      val a = Aggregator.immutableSortedReverseTake[Int](t).apply(in)
      l == a
    }
  }
  property("Aggregator.toList is identity on lists") {
    forAll((in: List[Int]) => in == Aggregator.toList(in))
  }

  property("MonoidAggregator.sumBefore is correct") {
    forAll { (in: List[List[Int]], ag: MonoidAggregator[Int, Int, Int]) =>
      val liftedAg = ag.sumBefore
      liftedAg(in) == ag(in.flatten)
    }
  }

  property("Aggregator.applyCumulatively is correct") {
    forAll { (in: List[Int], ag: Aggregator[Int, Int, Int]) =>
      val cumulative: List[Int] = ag.applyCumulatively(in)
      cumulative.size == in.size &&
      cumulative.zipWithIndex.forall {
        case (sum, i) =>
          sum == ag.apply(in.take(i + 1))
      }
    }
  }
  property("MonoidAggregator.either is correct") {
    forAll {
      (in: List[(Int, Int)], agl: MonoidAggregator[Int, Int, Int], agr: MonoidAggregator[Int, Int, Int]) =>
        agl.zip(agr).apply(in) ==
          agl
            .either(agr)
            .apply(in.flatMap { case (l, r) => List(Left(l), Right(r)) })
    }
  }

  property("MonoidAggregator.filter is correct") {
    forAll { (in: List[Int], ag: MonoidAggregator[Int, Int, Int], fn: Int => Boolean) =>
      ag.filterBefore(fn).apply(in) == ag.apply(in.filter(fn))
    }
  }

  property("MonoidAggregator.collectBefore is like filter + compose") {
    forAll { (in: List[Int], ag: MonoidAggregator[Int, Int, Int], fn: Int => Option[Int]) =>
      val cp = ag.collectBefore[Int] { case x if fn(x).isDefined => fn(x).get }
      val fp =
        ag.composePrepare[Int](fn(_).get).filterBefore[Int](fn(_).isDefined)
      cp(in) == fp(in)
    }
  }
}
