package com.twitter.algebird

import org.scalatest.PropSpec
import org.scalatestplus.scalacheck.Checkers

/**
 * @author Mansur Ashraf.
 */
trait CheckProperties extends PropSpec with Checkers {

  def property(testName: String, testTags: org.scalatest.Tag*)(testFun: org.scalacheck.Prop): Unit =
    super.property(testName, testTags: _*) { check { testFun } }
}
