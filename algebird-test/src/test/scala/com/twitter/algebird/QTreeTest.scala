package com.twitter.algebird

import org.specs._

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Gen.choose

import java.util.Arrays

object QTreeLaws extends Properties("QTree") {
  import BaseProperties._

  implicit val qtSemigroup = new QTreeSemigroup[Long](6)
  implicit val qtGen = Arbitrary { for(
      v <- choose(0L,10000L)
    ) yield (QTree(v))
  }

  property("QTree is associative") = isAssociative[QTree[Long]]
}

class QTreeTest extends Specification {
  def randomList(n : Long) = {
    (1L to n).map{i => math.random}
  }

  def buildQTree(k : Int, list : Seq[Double]) = {
    val qtSemigroup = new QTreeSemigroup[Double](6)
    list.map{QTree(_)}.reduce{qtSemigroup.plus(_,_)}
  }

  def trueQuantile(list : Seq[Double], q : Double) = {
    val rank = math.floor(q * list.size).toInt
    val sorted = list.toList.sorted
    sorted(rank)
  }

  def trueRangeSum(list : Seq[Double], from : Double, to : Double) = {
    list.filter{_ >= from}.filter{_ < to}.sum
  }

  for(k <- (1 to 6))
    ("QTree with sizeHint 2^" + k) should {
       "always contain the true quantile within its bounds" in {
          val list = randomList(10000)
          val qt = buildQTree(k, list)
          val quantile = math.random
          val (lower, upper) = qt.quantileBounds(quantile)
          val truth = trueQuantile(list, quantile)
          lower must be_>=(lower)
          truth must be_<=(upper)
       }
       "always contain the true range sum within its bounds" in {
          val list = randomList(10000)
          val qt = buildQTree(k, list)
          val from = math.random
          val to = math.random
          val (lower, upper) = qt.rangeSumBounds(from, to)
          val truth = trueRangeSum(list, from, to)
          truth must be_>=(lower)
          truth must be_<=(upper)
       }
       "have < 256 nodes" in {
         val list = randomList(100000)
         val qt = buildQTree(k, list)
         qt.size must be_<=(256)
       }
     }
}
