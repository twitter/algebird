package com.twitter.algebird

import org.scalacheck.{Arbitrary, Gen, Prop}
import scala.concurrent.duration.{DAYS, Duration, HOURS, MINUTES}

import Prop.{forAllNoShrink => forAll}

class DecayingCMSProperties extends CheckProperties {

  // uncomment to stress test (scalatest default is 10)
  // override val generatorDrivenConfig =
  //   PropertyCheckConfiguration(minSuccessful = 1000)

  val eps: Double = 1e-5

  def close(a: Double, b: Double): Boolean =
    if (a == b) {
      true
    } else {
      val (aa, ab) = (Math.abs(a), Math.abs(b))
      if (aa < eps && ab < eps) true
      else if (aa < ab) (b / a) < 1.0 + eps
      else (a / b) < 1.0 + eps
    }

  def fuzzyEq[K](module: DecayingCMS[K])(cms0: module.CMS, cms1: module.CMS): Boolean = {
    val t = cms0.timeInHL.max(cms1.timeInHL)
    val (x0, x1) =
      if (t == Double.NegativeInfinity) (cms0, cms1)
      else (cms0.rescaleTo(t), cms1.rescaleTo(t))

    (0 until module.depth).forall { d =>
      (0 until module.width).forall(w => close(x0.cells(d)(w), x1.cells(d)(w)))
    }
  }

  def genModule[K: CMSHasher]: Gen[DecayingCMS[K]] =
    for {
      seed <- Gen.choose(1L, Long.MaxValue)
      hl <- genDuration
      d <- Gen.choose(1, 5)
      w <- Gen.choose(1, 12)
    } yield DecayingCMS[K](seed, hl, d, w)

  def genBigModule[K: CMSHasher]: Gen[DecayingCMS[K]] =
    for {
      seed <- Gen.choose(1L, Long.MaxValue)
      hl <- genDuration
      d <- Gen.const(6)
      w <- Gen.choose(100, 200)
    } yield DecayingCMS[K](seed, hl, d, w)

  implicit def arbitraryModule[K: CMSHasher]: Arbitrary[DecayingCMS[K]] =
    Arbitrary(genModule)

  def genCms[K](
      module: DecayingCMS[K],
      genk: Gen[K],
      gent: Gen[Long],
      genv: Gen[Double]
  ): Gen[module.CMS] = {

    val genEmpty = Gen.const(module.monoid.zero)
    val genItem = Gen.zip(gent, genk, genv)

    def genSeq(cms0: module.CMS): Gen[module.CMS] =
      Gen.listOf(genItem).map { items =>
        items.foldLeft(cms0) { case (cms, (t, k, n)) =>
          cms.add(t, k, n)
        }
      }

    val terminalGens: Gen[module.CMS] = genEmpty

    def gen(depth: Int): Gen[module.CMS] =
      if (depth <= 0) terminalGens
      else {
        val recur = Gen.lzy(gen(depth - 1))
        Gen.frequency(
          1 -> genEmpty,
          7 -> recur.flatMap(genSeq(_)),
          2 -> Gen.zip(recur, recur).map { case (g0, g1) => module.monoid.plus(g0, g1) },
          1 -> Gen.listOf(recur).map(module.monoid.sum(_))
        )
      }

    gen(2)
  }

  val genDuration: Gen[Duration] =
    Gen.oneOf(Duration(1, HOURS), Duration(1, DAYS), Duration(10, MINUTES), Duration(7, DAYS))

  implicit val arbitraryDuration: Arbitrary[Duration] =
    Arbitrary(genDuration)

  val stdKey: Gen[String] =
    Gen.listOfN(2, Gen.choose('a', 'm')).map(_.mkString)

  val stdVal: Gen[Double] =
    Gen.choose(0.0, 100.0)

  val stdItem: Gen[(String, Double)] =
    Gen.zip(stdKey, stdVal)

  val stdItems: Gen[List[(String, Double)]] =
    Gen.listOf(stdItem)

  def genTimestamp[K](module: DecayingCMS[K]): Gen[Long] =
    Gen.choose(0L, module.halfLifeSecs.toLong * 10L)

  def genDoubleAt[K](module: DecayingCMS[K]): Gen[module.DoubleAt] =
    Gen.zip(genTimestamp(module), stdVal).map { case (t, x) => module.DoubleAt(x, t) }

  def genItem(module: DecayingCMS[String]): Gen[(Long, String, Double)] =
    Gen.zip(genTimestamp(module), stdKey, stdVal)

  def genItems(module: DecayingCMS[String]): Gen[List[(Long, String, Double)]] =
    Gen.listOf(genItem(module))

  def genValues(module: DecayingCMS[String]): Gen[List[(Long, Double)]] =
    Gen.listOf(Gen.zip(genTimestamp(module), stdVal))

  property("basic") {
    val gk = Gen.identifier
    val gt = Gen.choose(-30610206238000L, 32503698000000L)
    val gn = Gen.choose(0, 0xffff).map(_.toDouble)
    val gm = genModule[String]
    forAll(gk, gt, gn, gm) { (key: String, t: Long, n: Double, module: DecayingCMS[String]) =>
      val cms0 = module.monoid.zero
      val cms = cms0.add(t, key, n)
      val got = cms.get(key).at(t)
      Prop(got == n) :| s"$got == $n"
    }
  }

  property("sum(xs) = xs.foldLeft(zero)(plus)") {
    forAll { (module: DecayingCMS[String]) =>
      val g = genCms(module, stdKey, genTimestamp(module), stdVal)
      forAll(Gen.listOf(g)) { xs =>
        val left = module.monoid.sum(xs)
        val right = xs.foldLeft(module.monoid.zero)(module.monoid.plus)
        Prop(fuzzyEq(module)(left, right)) :| s"$left was not equal to $right"
      }
    }
  }

  property("all rows should sum to the same value") {
    forAll { (module: DecayingCMS[String]) =>
      val g = genCms(module, stdKey, genTimestamp(module), stdVal)
      forAll(g) { cms =>
        val sum0 = cms.cells(0).sum
        cms.cells.foldLeft(Prop(true)) { (res, row) =>
          val sum = row.sum
          res && (Prop(close(sum, sum0)) :| s"close($sum, $sum0)")
        }
      }
    }
  }

  property("round-tripping timestamps works") {
    forAll { (module: DecayingCMS[String]) =>
      val n = module.halfLifeSecs * 1000.0
      forAll(Gen.choose(-n, n)) { x =>
        val t = module.toTimestamp(x)
        val y = module.fromTimestamp(t)
        Prop(close(x, y)) :| s"close($x, $y)"
      }
    }
  }

  property("total works reliably") {
    forAll { (module: DecayingCMS[String]) =>
      val g = genCms(module, stdKey, genTimestamp(module), stdVal)
      forAll(g, genItems(module)) { (cms0, items) =>
        val time = cms0.timeInHL
        val cms1 = items.foldLeft(cms0) { case (c, (_, k, v)) =>
          c.scaledAdd(time, k, v)
        }
        val got = cms1.total.value
        val expected = cms0.total.value + items.map(_._3).sum
        Prop(close(got, expected)) :| s"close($got, $expected) with cms1=$cms1"
      }
    }
  }

  def timeCommutativeBinop(op: String)(f: (Double, Double) => Double): Unit =
    property(s"DoubleAt operations are time-commutative with $op") {
      forAll { (module: DecayingCMS[String]) =>
        val g = genDoubleAt(module)
        forAll(g, g, genTimestamp(module)) { (x, y, t) =>
          val lhs = x.map2(y)(f).at(t)
          val rhs = f(x.at(t), y.at(t))
          Prop(close(lhs, rhs)) :| s"close($lhs, $rhs)"
        }
      }
    }

  def timeCommutativeUnop(op: String)(f: Double => Double): Unit =
    property(s"DoubleAt operations are time-commutative with $op") {
      forAll { (module: DecayingCMS[String]) =>
        val g = genDoubleAt(module)
        forAll(g, genTimestamp(module)) { (x, t) =>
          val lhs = x.map(f).at(t)
          val rhs = f(x.at(t))
          Prop(close(lhs, rhs)) :| s"close($lhs, $rhs)"
        }
      }
    }

  // this idea here is that for a given operation (e.g. +) we want:
  //
  //   (x + y).at(t) = x(t) + y(t)
  //
  timeCommutativeBinop("+")(_ + _)
  timeCommutativeBinop("-")(_ - _)
  timeCommutativeBinop("min")(_.min(_))
  timeCommutativeBinop("max")(_.max(_))

  timeCommutativeUnop("abs")(_.abs)
  timeCommutativeUnop("unary -")(-_)
  timeCommutativeUnop("*2")(_ * 2.0)

  property("division is scale-independent") {
    forAll { (module: DecayingCMS[String]) =>
      val g = genDoubleAt(module)
      forAll(g, g, genTimestamp(module)) { (x, y, t) =>
        if (y.at(t) != 0) {
          val lhs = (y * (x / y)).at(t)
          val rhs = x.at(t)
          Prop(close(lhs, rhs)) :| s"close($lhs, $rhs)"
        } else {
          Prop(true)
        }
      }
    }
  }

  property("timeToZero") {
    forAll { (module: DecayingCMS[String]) =>
      val g = genDoubleAt(module)
      forAll(g) { x =>
        val t = x.timeToZero
        if (java.lang.Double.isFinite(t)) {
          Prop(x.at(module.toTimestamp(t - 100.0)) != 0.0) &&
          Prop(x.at(module.toTimestamp(t + 100.0)) == 0.0)
        } else {
          Prop(true)
        }
      }
    }
  }

  property("timeToUnit") {
    forAll { (module: DecayingCMS[String]) =>
      val g = genDoubleAt(module)
      forAll(g) { x =>
        val timeInHL = x.timeToUnit
        if (java.lang.Double.isFinite(timeInHL)) {
          val time = module.toTimestamp(timeInHL)
          val x0 = Math.abs(x.at(time - 100L))
          val x1 = Math.abs(x.at(time))
          val x2 = Math.abs(x.at(time + 100L))
          val p0 = Prop(x0 > 1.0) :| s"$x0 > 1.0"
          val p1 = Prop(close(x1, 1.0)) :| s"$x1 == 1.0"
          val p2 = Prop(x2 < 1.0) :| s"$x2 < 1.0"
          p0 && p1 && p2
        } else {
          Prop(true)
        }
      }
    }
  }

  property("((x compare y) = n) = ((x.at(t) compare y.at(t)) = n)") {
    forAll { (module: DecayingCMS[String]) =>
      val g = genDoubleAt(module)
      forAll(g, g, genTimestamp(module)) { (x, y, t) =>
        val got = x.compare(y)
        val expected = x.at(t).compare(y.at(t))
        Prop(got == expected)
      }
    }
  }

  property("range works reliably") {
    forAll { (module: DecayingCMS[String]) =>
      val g = genCms(module, stdKey, genTimestamp(module), stdVal)
      forAll(g, genItems(module)) { (cms0, items) =>
        val cms1 = cms0.bulkAdd(items)
        val (minAt, maxAt) = cms1.range
        val (xmin, xmax) = (minAt.value, maxAt.value)
        items.iterator.map(_._2).foldLeft(Prop(true)) { (res, k) =>
          val x = cms1.get(k).value
          res &&
          (Prop(xmin <= x) :| s"$xmin <= $x was false for key $k") &&
          (Prop(x <= xmax) :| s"$x <= $xmax was false for key $k")
        }
      }
    }
  }

  property("innerProductRoot(x, 0) = 0") {
    forAll { (module: DecayingCMS[String]) =>
      val g = genCms(module, stdKey, genTimestamp(module), stdVal)
      forAll(g) { cms =>
        val got = cms.innerProductRoot(module.empty)
        Prop(got.value == 0.0) :| s"got $got expected 0"
      }
    }
  }

  property("innerProductRoot(x, x) = x for singleton x") {
    forAll { (module: DecayingCMS[String]) =>
      forAll(genItem(module)) { case (t, k, v) =>
        val cms0 = module.empty.add(t, k, v)
        val got = cms0.l2Norm.at(t)
        val expected = v
        Prop(close(got, expected)) :| s"got $got, expected $expected"
      }
    }
  }

  property("innerProductRoot triangle inequality") {
    // the triangle inequality is only approximately true. for a
    // saturated CMS, it will stop being true. since normally we
    // generate very small CMS structures, we choose to use a bigger
    // module just for this test.
    forAll(genBigModule[String]) { module =>
      val g = genCms(module, stdKey, genTimestamp(module), stdVal)
      forAll(g, g) { (x, y) =>
        // abs(x + y) <= abs(x) + abs(y)
        val lhs = ((x + y).l2Norm).timeToUnit
        val rhs = (x.l2Norm + y.l2Norm).timeToUnit
        Prop(lhs <= rhs || close(lhs, rhs))
      }
    }
  }

  property("(a + b) + c ~= a + (b + c) and a + b ~= b + a and a + zero = a") {
    forAll { (module: DecayingCMS[String]) =>
      val g = genCms(module, stdKey, genTimestamp(module), stdVal)
      forAll(g, g, g) { (a, b, c) =>
        import module.monoid.plus
        val p0 = {
          val left = plus(plus(a, b), c)
          val right = plus(a, plus(b, c))
          Prop(fuzzyEq(module)(left, right)) :| s"associate: $left was not equal to $right"
        }

        val p1 = {
          val left = plus(a, b)
          val right = plus(b, a)
          Prop(fuzzyEq(module)(left, right)) :| s"commute: $left was not equal to $right"
        }

        val p2 = {
          val left = plus(a, module.monoid.zero)
          Prop(fuzzyEq(module)(left, a)) :| s"a + zero = a: $left was not equal to $a"
        }

        p0 && p1 && p2
      }
    }
  }

  property("fixed key ~ decayedvalue") {

    def makeModule(halfLife: Duration): DecayingCMS[String] = {
      val seed = "FIXED".hashCode.toLong
      DecayingCMS[String](seed, halfLife, depth = 2, width = 3)
    }

    val genTestCase: Gen[(DecayingCMS[String], String, List[(Long, Double)])] =
      for {
        halfLife <- genDuration
        module = makeModule(halfLife)
        key <- stdKey
        values <- genValues(module)
      } yield (module, key, values)

    def law(testCase: (DecayingCMS[String], String, List[(Long, Double)])): Prop = {

      val (module, key, inputs0) = testCase
      val halfLife = module.halfLife
      val inputs = inputs0.sorted
      val halfLifeSecs = halfLife.toSeconds.toDouble

      if (inputs.nonEmpty) {

        val tlast = inputs.last._1

        val dvm = new DecayedValueMonoid(0.0)
        val dv = dvm.sum(inputs.map { case (t, n) =>
          DecayedValue.build(n, (t.toDouble / 1000.0), halfLifeSecs)
        })
        val expected = dvm.valueAsOf(dv, halfLifeSecs, (tlast.toDouble / 1000.0))

        val cms0 = module.monoid.zero
        val cmss = inputs.map { case (t, n) => cms0.add(t, key, n) }
        val cms = module.monoid.sum(cmss)

        val got = cms.get(key).at(tlast)

        Prop(close(got, expected)) :| s"$got is not close to $expected"
      } else {
        Prop(true)
      }
    }

    forAll(genTestCase)(law _)

    val regressions =
      List(
        (
          "",
          List((-7634593159529L, 1.0), (-10628114330964L, 0.0)),
          Duration(1, HOURS)
        ),
        (
          "",
          List((29617281175711L, 65534.0), (29614132054038L, 255.0)),
          Duration(7, DAYS)
        )
      )

    regressions.foldLeft(Prop(true)) { case (res, (k, items, hl)) =>
      res && law((makeModule(hl), k, items))
    }
  }
}
