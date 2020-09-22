package com.twitter.algebird.macros

import scala.language.experimental.macros

import com.twitter.algebird._
import com.twitter.algebird.macros.MacroCompat._

import org.scalacheck.{Arbitrary, Gen}

object ArbitraryCaseClassMacro {
  def caseClassGen[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[Gen[T]] = {
    import c.universe._

    val tpe = weakTypeOf[T]

    val isCaseClass = tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass
    if (!isCaseClass)
      c.abort(c.enclosingPosition, s"$T is not a case class")

    val params = getParams(c)
    val types = getParamTypes(c)

    val getsList = params.zip(types).map { case (param, t) =>
      fq"${param.name} <- _root_.org.scalacheck.Arbitrary.arbitrary[$t]"
    }

    val paramsList =
      params.map(param => q"${param.name.asInstanceOf[TermName]}")
    val companion = companionSymbol(c)(tpe.typeSymbol)

    val res = q"""
    for ( ..$getsList ) yield $companion.apply(..$paramsList)
    """

    c.Expr[Gen[T]](res)
  }

  def caseClassArbitrary[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[Arbitrary[T]] = {
    import c.universe._

    val gen = caseClassGen(c)(T)
    val res = q"_root_.org.scalacheck.Arbitrary($gen)"

    c.Expr[Arbitrary[T]](res)
  }

  def gen[T]: Gen[T] = macro caseClassGen[T]
  def arbitrary[T]: Arbitrary[T] = macro caseClassArbitrary[T]
}
