package com.twitter.algebird.macros

import scala.reflect.macros.{Context => WhiteBoxContext}

private[algebird] object MacroCompat {

  type Context = WhiteBoxContext

  def normalize(c: Context)(tpe: c.universe.Type) = tpe.normalize

  def declarations(c: Context)(tpe: c.universe.Type) = tpe.declarations

  def companionSymbol[T](c: Context)(typeSymbol: c.universe.Symbol) = typeSymbol.companionSymbol

  def typeName(c: Context)(s: String): c.universe.TypeName = c.universe.newTypeName(s)

  def termName(c: Context)(s: String): c.universe.TermName = c.universe.newTermName(s)
}
