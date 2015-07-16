package com.twitter.algebird.macros

import scala.language.experimental.{ macros => sMacros }
import scala.reflect.macros.Context
import scala.reflect.runtime.universe._

import com.twitter.algebird._

object MonoidMacro {
  def caseClassMonoid[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[Monoid[T]] = {
    import c.universe._

    macros.ensureCaseClass(c)

    val res = q"""
    new _root_.com.twitter.algebird.Monoid[$T] {
      ${SemigroupMacro.plus(c)}
      ${SemigroupMacro.sumOption(c)}
      ${zero(c)}
    }
    """
    c.Expr[Monoid[T]](res)
  }

  def zero[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Tree = {
    import c.universe._

    val companion = macros.getCompanionObject(c)
    val zerosList = macros.getParams(c).map(param => q"implicitly[_root_.com.twitter.algebird.Monoid[$param]].zero")

    q"def zero: $T = $companion.apply(..$zerosList)"
  }

}
