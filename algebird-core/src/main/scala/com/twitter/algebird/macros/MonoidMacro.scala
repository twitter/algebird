package com.twitter.algebird.macros

import scala.language.experimental.{ macros => sMacros }
import scala.reflect.macros.Context
import scala.reflect.runtime.universe._

import com.twitter.algebird._

object MonoidMacro {
  def caseClassMonoid[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[Monoid[T]] = {
    import c.universe._

    ensureCaseClass(c)

    val implicitMonoids = getParams(c).map {
      param => q"implicitly[_root_.com.twitter.algebird.Monoid[${param.returnType}]]"
    }

    val res = q"""
    new _root_.com.twitter.algebird.Monoid[$T] {
      ${SemigroupMacro.plus(c)(implicitMonoids)}
      ${SemigroupMacro.sumOption(c)(implicitMonoids)}
      ${zero(c)(implicitMonoids)}
    }
    """
    c.Expr[Monoid[T]](res)
  }

  def zero[T](c: Context)(implicitInstances: List[c.Tree])(implicit T: c.WeakTypeTag[T]): c.Tree = {
    import c.universe._

    val companion = getCompanionObject(c)
    val zerosList = implicitInstances.map(instance => q"$instance.zero")

    q"def zero: $T = $companion.apply(..$zerosList)"
  }

}
