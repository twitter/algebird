package com.twitter.algebird.macros

import scala.language.experimental.{ macros => sMacros }
import scala.reflect.macros.Context
import scala.reflect.runtime.universe._

import com.twitter.algebird._

object GroupMacro {
  def caseClassGroup[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[Group[T]] = {
    import c.universe._

    macros.ensureCaseClass(c)

    val res = q"""
    new _root_.com.twitter.algebird.Group[$T] {
      ${SemigroupMacro.plus(c)}
      ${SemigroupMacro.sumOption(c)}
      ${MonoidMacro.zero(c)}
      ${negate(c)}
      ${minus(c)}
    }
    """
    c.Expr[Group[T]](res)
  }

  def negate[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Tree = {
    import c.universe._

    val companion = macros.getCompanionObject(c)
    val negateList = getParams(c).map(param => q"implicitly[_root_.com.twitter.algebird.Group[$param]].negate(x.$param)")

    q"override def negate(x: $T): $T = $companion.apply(..$negateList)"
  }

  def minus[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Tree = {
    import c.universe._

    val companion = getCompanionObject(c)
    val minusList = getParams(c).map(param => q"implicitly[_root_.com.twitter.algebird.Group[$param]].minus(l.$param, r.$param)")

    q"override def minus(l: $T, r: $T): $T = $companion.apply(..$minusList)"
  }

}
