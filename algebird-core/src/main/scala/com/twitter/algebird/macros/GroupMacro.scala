package com.twitter.algebird.macros

import scala.language.experimental.{macros => sMacros}
import scala.reflect.macros.Context
import scala.reflect.runtime.universe._

import com.twitter.algebird._

object GroupMacro {
  def caseClassGroup[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[Group[T]] = {
    import c.universe._

    ensureCaseClass(c)

    val implicitGroups = getParams(c).map { param =>
      q"implicitly[_root_.com.twitter.algebird.Group[${param.typeSignatureIn(T.tpe)}]]"
    }

    val res = q"""
    new _root_.com.twitter.algebird.Group[$T] {
      ${SemigroupMacro.plus(c)(implicitGroups)}
      ${SemigroupMacro.sumOption(c)(implicitGroups)}
      ${MonoidMacro.zero(c)(implicitGroups)}
      ${negate(c)(implicitGroups)}
      ${minus(c)(implicitGroups)}
    }
    """
    c.Expr[Group[T]](res)
  }

  def negate[T](c: Context)(implicitInstances: List[c.Tree])(implicit T: c.WeakTypeTag[T]): c.Tree = {
    import c.universe._

    val companion = getCompanionObject(c)
    val negateList = getParams(c).zip(implicitInstances).map {
      case (param, instance) => q"$instance.negate(x.$param)"
    }

    q"override def negate(x: $T): $T = $companion.apply(..$negateList)"
  }

  def minus[T](c: Context)(implicitInstances: List[c.Tree])(implicit T: c.WeakTypeTag[T]): c.Tree = {
    import c.universe._

    val companion = getCompanionObject(c)
    val minusList = getParams(c).zip(implicitInstances).map {
      case (param, instance) => q"$instance.minus(l.$param, r.$param)"
    }

    q"override def minus(l: $T, r: $T): $T = $companion.apply(..$minusList)"
  }

}
