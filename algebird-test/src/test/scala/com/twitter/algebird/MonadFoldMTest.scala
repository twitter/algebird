package com.twitter.algebird

import org.specs2.mutable._

class MonadFoldMTest extends Specification {

  def binSmalls(x: Int, y: Int): Option[Int] = if (y > 9) None else Some(x + y)
  "A monad foldM" should {
    "fold correctly" in {

      // nice easy example from Learn You a Haskell

      val first = Monad.foldM(0, List(2, 8, 3, 1))(binSmalls)
      first must be_==(Some(14))
      def binSmalls2(x: Int, y: String): Option[Int] = if (y == "11") None else Some(x + y.toInt)

      val second = Monad.foldM(0, List("2", "11", "3", "1"))(binSmalls2)
      second must be_==(None)
    }
    "handle an empty list" in {
      val third = Monad.foldM(0, List.empty)(binSmalls)
      third must be_==(Some(0))
    }
  }
}