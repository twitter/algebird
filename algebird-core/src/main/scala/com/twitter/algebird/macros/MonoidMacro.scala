package com.twitter.algebird.macros

import com.twitter.algebird._
import com.twitter.algebird.macros.MacroCompat._

object MonoidMacro {
  def caseClassMonoid[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[Monoid[T]] = {
    import c.universe._

    ensureCaseClass(c)

    val implicitMonoids = getParams(c).map { param =>
      q"implicitly[_root_.com.twitter.algebird.Monoid[${param.typeSignatureIn(T.tpe)}]]"
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
