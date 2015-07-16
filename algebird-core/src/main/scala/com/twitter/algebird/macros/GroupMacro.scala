package com.twitter.algebird.macros

import scala.language.experimental.macros
import scala.reflect.macros.Context
import scala.reflect.runtime.universe._

import com.twitter.algebird._

object GroupMacro {
  def caseClassGroup[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[Group[T]] = {
    import c.universe._

    val tpe = weakTypeOf[T]

    val isCaseClass = tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass
    if (!isCaseClass)
      c.abort(c.enclosingPosition, s"$T is not a clase class")

    val params = tpe.declarations.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }.toList

    val plusList = params.map(param => q"implicitly[_root_.com.twitter.algebird.Group[$param]].plus(l.$param, r.$param)")
    val zeroList = params.map(param => q"implicitly[_root_.com.twitter.algebird.Group[$param]].zero")
    val negateList = params.map(param => q"implicitly[_root_.com.twitter.algebird.Group[$param]].negate(x.$param)")
    val minusList = params.map(param => q"implicitly[_root_.com.twitter.algebird.Group[$param]].minus(l.$param, r.$param)")

    val companion = tpe.typeSymbol.companionSymbol

    _root_.scala.Predef.println("hi")
    val res = q"""
    _root_.scala.Predef.println("ho")
    new _root_.com.twitter.algebird.Group[$T] {
      def plus(l: $T, r: $T): $T = $companion.apply(..$plusList)
      def zero: $T = $companion.apply(..$zeroList)
      override def negate(x: $T): $T = $companion.apply(..$negateList)
      override def minus(l: $T, r: $T): $T = $companion.apply(..$minusList)
    }
    """
    c.Expr[Group[T]](res)
  }

}
