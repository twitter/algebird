package com.twitter.algebird.macros

import scala.language.experimental.macros
import scala.reflect.macros.Context
import scala.reflect.runtime.universe._

import com.twitter.algebird._

object FieldMacro {
  def caseClassField[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[Field[T]] = {
    import c.universe._

    val tpe = weakTypeOf[T]

    val isCaseClass = tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass
    if (!isCaseClass)
      c.abort(c.enclosingPosition, s"$T is not a clase class")

    val params = tpe.declarations.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }.toList

    val plusList = params.map(param => q"implicitly[_root_.com.twitter.algebird.Field[$param]].plus(l.$param, r.$param)")
    val zeroList = params.map(param => q"implicitly[_root_.com.twitter.algebird.Field[$param]].zero")
    val negateList = params.map(param => q"implicitly[_root_.com.twitter.algebird.Field[$param]].negate(x.$param)")
    val timesList = params.map(param => q"implicitly[_root_.com.twitter.algebird.Field[$param]].times(l.$param, r.$param)")
    val oneList = params.map(param => q"implicitly[_root_.com.twitter.algebird.Field[$param]].one")
    val assertNotZeroList = params.map(param => q"{ implicitly[_root_.com.twitter.algebird.Field[$param]].assertNotZero(r.$param); () }")
    val inverseList = params.map(param => q"implicitly[_root_.com.twitter.algebird.Field[$param]].div(implicitly[_root_.com.twitter.algebird.Field[$param]].one, r.$param)")
    val divList = params.map(param => q"implicitly[_root_.com.twitter.algebird.Field[$param]].times(l.$param, implicitly[_root_.com.twitter.algebird.Field[$param]].inverse(r.$param))")

    val companion = tpe.typeSymbol.companionSymbol

    val res = q"""
    new _root_.com.twitter.algebird.Field[$T] {
      def plus(l: $T, r: $T): $T = $companion.apply(..$plusList)
      def zero: $T = $companion.apply(..$zeroList)
      override def negate(x: $T): $T = $companion.apply(..$negateList)
      def times(l: $T, r: $T): $T = $companion.apply(..$timesList)
      def one: $T = $companion.apply(..$oneList)
      override def inverse(r: $T): $T = {
        _root_.scala.collection.immutable.List(..$assertNotZeroList)
        $companion.apply(..$inverseList)
      }
      override def div(l: $T, r: $T): $T = {
        _root_.scala.collection.immutable.List(..$assertNotZeroList)
        $companion.apply(..$divList)
      }
    }
    """
    c.Expr[Field[T]](res)
  }

}
