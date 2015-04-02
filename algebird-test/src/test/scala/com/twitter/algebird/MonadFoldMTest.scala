package com.twitter.algebird

import org.scalatest._

class ChainableCallbackCollectorBuilderFoldMTest extends WordSpec with Matchers {

  def binSmalls(x: Int, y: Int): Option[Int] = if (y > 9) None else Some(x + y)
  "A monad foldM" should {
    "fold correctly" in {

      // nice easy example from Learn You a Haskell

      val first = ChainableCallbackCollectorBuilder.foldM(0, List(2, 8, 3, 1))(binSmalls)
      assert(first == Some(14))
      def binSmalls2(x: Int, y: String): Option[Int] = if (y == "11") None else Some(x + y.toInt)

      val second = ChainableCallbackCollectorBuilder.foldM(0, List("2", "11", "3", "1"))(binSmalls2)
      assert(second == None)
    }
    "handle an empty list" in {
      val third = ChainableCallbackCollectorBuilder.foldM(0, List.empty)(binSmalls)
      assert(third == Some(0))
    }
  }
}