package com.twitter.algebird.macros

import scala.language.experimental.macros
import scala.reflect.macros.Context
import scala.reflect.runtime.universe._

import com.twitter.algebird._

object RingMacro {
  def caseClassRing[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[Ring[T]] = {
    import c.universe._

    val tpe = weakTypeOf[T]

    val isCaseClass = tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass
    if (!isCaseClass)
      c.abort(c.enclosingPosition, s"$T is not a clase class")

    val params = tpe.declarations.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }.toList

    val plusList = params.map(param => q"implicitly[_root_.com.twitter.algebird.Ring[$param]].plus(l.$param, r.$param)")
    val zeroList = params.map(param => q"implicitly[_root_.com.twitter.algebird.Ring[$param]].zero")
    val negateList = params.map(param => q"implicitly[_root_.com.twitter.algebird.Ring[$param]].negate(x.$param)")
    val timesList = params.map(param => q"implicitly[_root_.com.twitter.algebird.Ring[$param]].times(l.$param, r.$param)")
    val oneList = params.map(param => q"implicitly[_root_.com.twitter.algebird.Ring[$param]].one")

    val companion = tpe.typeSymbol.companionSymbol

    val res = q"""
    new _root_.com.twitter.algebird.Ring[$T] {
      def plus(l: $T, r: $T): $T = $companion.apply(..$plusList)
      def zero: $T = $companion.apply(..$zeroList)
      override def negate(x: $T): $T = $companion.apply(..$negateList)
      def times(l: $T, r: $T): $T = $companion.apply(..$timesList)
      def one: $T = $companion.apply(..$oneList)
    }
    """
    c.Expr[Ring[T]](res)
  }

}
