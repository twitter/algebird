package com.twitter.algebird.macros

import scala.language.experimental.{ macros => sMacros }
import scala.reflect.macros.Context
import scala.reflect.runtime.universe._

import com.twitter.algebird._

object RingMacro {
  def caseClassRing[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[Ring[T]] = {
    import c.universe._

    macros.ensureCaseClass(c)

    val params = macros.getParams(c)
    val companion = macros.getCompanionObject(c)

    val timesList = params.map(param => q"implicitly[_root_.com.twitter.algebird.Ring[$param]].times(l.$param, r.$param)")
    val oneList = params.map(param => q"implicitly[_root_.com.twitter.algebird.Ring[$param]].one")

    val res = q"""
    new _root_.com.twitter.algebird.Ring[$T] {
      ${SemigroupMacro.plus(c)}
      ${SemigroupMacro.sumOption(c)}
      ${MonoidMacro.zero(c)}
      ${GroupMacro.negate(c)}
      def times(l: $T, r: $T): $T = $companion.apply(..$timesList)
      def one: $T = $companion.apply(..$oneList)
    }
    """
    c.Expr[Ring[T]](res)
  }

}

