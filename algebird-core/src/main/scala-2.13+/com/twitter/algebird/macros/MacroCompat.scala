package com.twitter.algebird.macros

import scala.reflect.macros.whitebox

private[algebird] object MacroCompat {

  type Context = whitebox.Context

  def normalize(c: Context)(tpe: c.universe.Type) = tpe.etaExpand

  def declarations(c: Context)(tpe: c.universe.Type) = tpe.decls

  def companionSymbol[T](c: Context)(typeSymbol: c.universe.Symbol) = typeSymbol.companion

  def typeName(c: Context)(s: String): c.universe.TypeName = c.universe.TypeName(s)

  def termName(c: Context)(s: String): c.universe.TermName = c.universe.TermName(s)

}
