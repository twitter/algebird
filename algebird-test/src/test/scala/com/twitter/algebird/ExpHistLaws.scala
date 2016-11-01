package com.twitter.algebird

import org.scalatest.{ PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks
import org.scalatest.prop.Checkers.check
import org.scalacheck.{ Gen, Arbitrary }
import Arbitrary.arbitrary

case class PosNum[T: Numeric](value: T)

object PosNum {
  implicit def arb[T: Numeric: Gen.Choose]: Arbitrary[PosNum[T]] =
    Arbitrary(for { p <- Gen.posNum[T] } yield PosNum(p))
}

case class NonEmptyVector[T](items: Vector[T]) {
  def sorted(implicit ev: Ordering[T]): Vector[T] = items.sorted
}

object NonEmptyVector {
  implicit def arb[T: Ordering: Arbitrary]: Arbitrary[NonEmptyVector[T]] =
    Arbitrary(for {
      l <- Gen.nonEmptyListOf(arbitrary[T])
    } yield NonEmptyVector[T](l.toVector))
}

class ExpHistLaws extends PropSpec with PropertyChecks with Matchers {
  import ExpHist.{ Bucket, Config }

  implicit val arb: Arbitrary[Bucket] =
    Arbitrary(for {
      count <- Gen.posNum[Long]
      timestamp <- Gen.posNum[Long]
    } yield Bucket(count - 1L, timestamp))

  implicit val conf: Arbitrary[Config] =
    Arbitrary(for {
      k <- Gen.posNum[Short]
      windowSize <- Gen.posNum[Long]
    } yield Config(1 / k.toDouble, windowSize))

  implicit val expHist: Arbitrary[ExpHist] =
    Arbitrary(for {
      buckets <- arbitrary[Vector[Bucket]]
      conf <- arbitrary[ExpHist.Config]
    } yield ExpHist.empty(conf).addAll(buckets))

  // The example in the paper is actually busted, based on his
  // algorithm. He says to assume that k/2 == 2, but then he promotes
  // as if k/2 == 1, ie k == 2.
  property("example from paper") {
    // Returns the vector of bucket sizes from largest to smallest.
    def windows(e: ExpHist): Vector[Long] = e.buckets.reverse.map(_.size)

    val e = ExpHist.empty(ExpHist.Config(0.5, 100))
    val plus76 = e.add(76, 0)

    val inc = plus76.inc(0)
    val incinc = inc.add(2, 0)

    assert(windows(plus76) == Vector(32, 16, 8, 8, 4, 4, 2, 1, 1))
    assert(windows(inc) == Vector(32, 16, 8, 8, 4, 4, 2, 2, 1))
    assert(windows(incinc) == Vector(32, 16, 16, 8, 4, 2, 1))
  }

  property("adding i results in upperBoundSum == i") {
    forAll { (conf: ExpHist.Config, bucket: Bucket) =>
      assert(ExpHist.empty(conf)
        .add(bucket.size, bucket.timestamp)
        .upperBoundSum == bucket.size)
    }
  }

  property("empty.add and from are identical") {
    forAll { (conf: ExpHist.Config, bucket: Bucket) =>
      val byAdd = ExpHist.empty(conf).add(bucket.size, bucket.timestamp)
      val byFrom = ExpHist.from(bucket.size, bucket.timestamp, conf)
      assert(byAdd == byFrom)
    }
  }

  property("step should be idempotent") {
    forAll { (expHist: ExpHist, bucket: Bucket) =>
      val ts = bucket.timestamp
      val stepped = expHist.step(ts)
      assert(stepped == stepped.step(ts))

      val added = expHist.add(bucket.size, ts)
      val addThenStep = added.step(ts)
      val stepThenAdd = stepped.add(bucket.size, ts)

      assert(added == addThenStep)
      assert(added == stepThenAdd)
      assert(addThenStep == stepThenAdd)
    }
  }

  property("stepping is the same as adding 0 at the same ts") {
    forAll { (expHist: ExpHist, ts: PosNum[Long]) =>
      assert(expHist.step(ts.value) == expHist.add(0, ts.value))
    }
  }

  // property("add matches addAll") {
  //   forAll { (unsorted: NonEmptyVector[Bucket], conf: ExpHist.Config) =>
  //     val e = ExpHist.empty(conf)
  //     val items = unsorted.sorted
  //     val withAdd = items.foldLeft(e) {
  //       case (e, Bucket(count, timestamp)) =>
  //         e.add(count, timestamp)
  //     }

  //     assert(withAdd == e.addAll(items))
  //   }
  // }

  // This is the meat!
  def testBounds(eh: ExpHist, actualSum: Long) {
    val upperBound = eh.upperBoundSum
    val lowerBound = eh.lowerBoundSum
    assert(lowerBound <= actualSum)
    assert(upperBound >= actualSum)

    val maxOutsideWindow = eh.oldestBucketSize - 1
    val minInsideWindow = 1 + eh.total - eh.oldestBucketSize
    val absoluteError = maxOutsideWindow / 2.0
    val relativeError = absoluteError / minInsideWindow

    // local relative error calc:
    assert(relativeError <= eh.conf.epsilon)

    // Instance's relative error calc:
    assert(eh.relativeError <= eh.conf.epsilon)

    // Error can never be above 0.5.
    assert(eh.relativeError <= 0.5)
  }

  def actualBucketSum(buckets: Vector[Bucket], exclusiveCutoff: Long): Long =
    buckets.collect {
      case Bucket(count, ts) if ts > exclusiveCutoff => count
    }.sum

  property("bounded error of the query with addAll") {
    forAll { (v: NonEmptyVector[Bucket], conf: Config) =>
      val buckets = v.items
      val recentTs = buckets.maxBy(_.timestamp).timestamp
      val cutoff = conf.expiration(recentTs)

      testBounds(
        ExpHist.empty(conf).addAll(buckets),
        actualBucketSum(buckets, cutoff))
    }
  }

  property("bounded error of the query with add") {
    forAll { (items: NonEmptyVector[Bucket], conf: Config) =>
      val buckets = items.sorted
      val recentTs = buckets.last.timestamp
      val cutoff = conf.expiration(recentTs)

      val full = buckets.foldLeft(ExpHist.empty(conf)) {
        case (e, Bucket(c, t)) => e.add(c, t)
      }

      testBounds(full, actualBucketSum(buckets, cutoff))
    }
  }

  property("add and inc should generate the same results") {
    forAll { (bucket: Bucket, conf: ExpHist.Config) =>
      val e = ExpHist.empty(conf)

      val incs = (0L until bucket.size).foldLeft(e) {
        case (acc, _) => acc.inc(bucket.timestamp)
      }

      val adds = e.add(bucket.size, bucket.timestamp)

      assert(incs.total == adds.total)
      assert(incs.lowerBoundSum == adds.lowerBoundSum)
      assert(incs.upperBoundSum == adds.upperBoundSum)
    }
  }
}

class CanonicalLaws extends PropSpec with PropertyChecks with Matchers {
  property("l-canonical representation round-trips") {
    forAll { (i: PosNum[Long], l: PosNum[Short]) =>
      assert(Canonical.toLong(Canonical.fromLong(i.value, l.value)) == i.value)
    }
  }

  property("Sum of canonical rep is # of buckets required.") {
    forAll { (i: PosNum[Long], k: PosNum[Short]) =>
      val rep = Canonical.fromLong(i.value, k.value)
      val numBuckets = Canonical.toBuckets(rep).size
      assert(numBuckets == rep.sum)

      // longToBuckets works.
      assert(Canonical.longToBuckets(i.value, k.value).size == numBuckets)
    }
  }

  property("all i except last have either k/2, k/2 + 1 buckets") {
    forAll { (i: PosNum[Long], k: PosNum[Short]) =>
      val lower = k.value
      val upper = lower + 1
      assert(
        Canonical.fromLong(i.value, k.value).init.forall { numBuckets =>
          lower <= numBuckets && numBuckets <= upper
        })
    }
  }
}
