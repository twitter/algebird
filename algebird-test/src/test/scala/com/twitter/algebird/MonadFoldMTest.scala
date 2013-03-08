package com.twitter.algebird

import org.specs._
import Monad.option

class MonadFoldMTest extends Specification {
	noDetailedDiffs()

	"A monad foldM" should {
		"fold correctly" in {
			
 			// nice easy example from Learn You a Haskell
 			def binSmalls(x: Int, y: Int) : Option[Int] = if(y > 9) None else Some(x+y)
 			val first = option.foldM(binSmalls)(0)(List(2,8,3,1))
 			first must be_==(Some(14))
 			val second = option.foldM(binSmalls)(0)(List(2,11,3,1))
 			second must be_==(None)
		}
	}
}