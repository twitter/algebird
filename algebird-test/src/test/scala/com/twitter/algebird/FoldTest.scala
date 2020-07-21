package com.twitter.algebird

import org.scalatest.wordspec.AnyWordSpec

class FoldTest extends AnyWordSpec {

  sealed trait Case[I, O] {
    def expected: O
    def runCase(fold: Fold[I, O]): O
  }
  case class Zero[I, O](expected: O) extends Case[I, O] {
    override def runCase(fold: Fold[I, O]): O = fold.overEmpty
  }
  case class One[I, O](in: I, expected: O) extends Case[I, O] {
    override def runCase(fold: Fold[I, O]): O = fold.overSingleton(in)
  }
  case class Many[I, O](in: Traversable[I], expected: O) extends Case[I, O] {
    override def runCase(fold: Fold[I, O]): O = fold.overTraversable(in)
  }

  def run[I, O](fold: Fold[I, O], cases: Case[I, O]*): Unit =
    cases.foreach(c => assert(c.runCase(fold) === c.expected))

  "Fold" should {

    "foldLeft" in {
      run[String, String](
        Fold.foldLeft("")((a, b) => a ++ b),
        Zero(""),
        One("1", "1"),
        Many(Seq("1", "2", "3"), "123")
      )
    }

    "seq" in {
      run[Int, Seq[Int]](
        Fold.seq,
        Zero(Seq.empty),
        One(1, Seq(1)),
        Many(Seq(1, 2, 3), Seq(1, 2, 3)),
        Many(Seq(2, 1, 3), Seq(2, 1, 3))
      )
    }

    "const" in {
      run[Int, String](Fold.const("42"), Zero("42"), One(1, "42"), Many(Seq(1, 2, 3), "42"))
    }

    "first" in {
      run[String, Option[String]](
        Fold.first,
        Zero(None),
        One("1", Some("1")),
        Many(Seq("1", "2", "3"), Some("1"))
      )
    }

    "last" in {
      run[String, Option[String]](
        Fold.last,
        Zero(None),
        One("1", Some("1")),
        Many(Seq("1", "2", "3"), Some("3"))
      )
    }

    "max" in {
      run[Int, Option[Int]](
        Fold.max,
        Zero(None),
        One(1, Some(1)),
        Many(Seq(1, 2, 3), Some(3)),
        Many(Seq(1, 3, 2), Some(3))
      )
    }

    "min" in {
      run[Int, Option[Int]](
        Fold.min,
        Zero(None),
        One(1, Some(1)),
        Many(Seq(1, 2, 3), Some(1)),
        Many(Seq(2, 1, 3), Some(1))
      )
    }

    "sum" in {
      run[Int, Int](Fold.sum, Zero(0), One(1, 1), Many(Seq(1, 2, 3), 6), Many(Seq(2, 1, 3), 6))
    }

    "size" in {
      run[String, Long](Fold.size, Zero(0), One("1", 1), Many(Seq("1", "2", "3"), 3))
    }

    "average" in {
      run[Int, Double](
        Fold.sum[Int].joinWith(Fold.size)((s, c) => s.toDouble / c),
        One(1, 1.0),
        Many(Seq(1, 2, 3), 2.0),
        Many(Seq(2, 1, 3), 2.0)
      )
    }

    "sequence" in {
      run[Int, Seq[Long]](
        Fold.sequence(
          Seq(
            Fold.count(_ < 0),
            Fold.count {
              _ >= 0
            }
          )
        ),
        Zero(Seq(0, 0)),
        One(1, Seq(0, 1)),
        Many(Seq(-2, -1, 0, 1, 2), Seq(2, 3))
      )
    }

  }

}
