package com.twitter.algebird

import org.scalatestplus.scalacheck.Checkers
import org.scalatest.propspec.AnyPropSpec

/**
 * @author Mansur Ashraf.
 */
trait CheckProperties extends AnyPropSpec with Checkers {

  def property(testName: String, testTags: org.scalatest.Tag*)(testFun: org.scalacheck.Prop): Unit =
    super.property(testName, testTags: _*)(check(testFun))
}
