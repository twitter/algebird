package com.twitter.algebird.macros

import scala.language.experimental.macros
import scala.reflect.macros.Context
import scala.reflect.runtime.universe._

import com.twitter.algebird._

object SemigroupMacro {
  def caseClassSemigroup[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[Semigroup[T]] = {
    import c.universe._

    val tpe = weakTypeOf[T]

    val isCaseClass = tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass
    if (!isCaseClass)
      c.abort(c.enclosingPosition, s"$T is not a clase class")

    val params = tpe.declarations.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }.toList

    val plusList = params.map(param => q"implicitly[_root_.com.twitter.algebird.Semigroup[$param]].plus(l.$param, r.$param)")

    val companion = tpe.typeSymbol.companionSymbol

    val res = q"""
    new _root_.com.twitter.algebird.Semigroup[$T] {
      def plus(l: $T, r: $T): $T = $companion.apply(..$plusList)
    }
    """
    c.Expr[Semigroup[T]](res)
  }

}
