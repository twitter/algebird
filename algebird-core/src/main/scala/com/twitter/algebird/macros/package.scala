package com.twitter.algebird

import scala.language.experimental.{ macros => sMacros }
import scala.reflect.macros.Context
import scala.reflect.runtime.universe._

package object macros {
  private[macros] def ensureCaseClass[T](c: Context)(implicit T: c.WeakTypeTag[T]): Unit = {
    import c.universe._

    val tpe = weakTypeOf[T]

    val isCaseClass = tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass
    if (!isCaseClass)
      c.abort(c.enclosingPosition, s"$T is not a clase class")
  }

  private[macros] def getParams[T](c: Context)(implicit T: c.WeakTypeTag[T]): List[c.universe.MethodSymbol] = {
    import c.universe._

    val tpe = weakTypeOf[T]
    tpe.declarations.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }.toList
  }

  private[macros] def getCompanionObject[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.universe.Symbol = {
    import c.universe._
    weakTypeOf[T].typeSymbol.companionSymbol
  }
}
