package com.twitter.algebird

import com.twitter.algebird.macros.MacroCompat._

package object macros {
  private[macros] def ensureCaseClass[T](c: Context)(implicit T: c.WeakTypeTag[T]): Unit = {
    import c.universe._

    val tpe = weakTypeOf[T]

    val isCaseClass = tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass
    if (!isCaseClass)
      c.abort(c.enclosingPosition, s"${tpe.typeSymbol} is not a case class")
  }

  private[macros] def getParams[T](
      c: Context
  )(implicit T: c.WeakTypeTag[T]): List[c.universe.MethodSymbol] = {
    import c.universe._

    val tpe = weakTypeOf[T]
    declarations(c)(tpe).collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }.toList
  }

  private[macros] def getCompanionObject[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.universe.Symbol = {
    import c.universe._
    companionSymbol(c)(weakTypeOf[T].typeSymbol)
  }

  private[macros] def getParamTypes[T](c: Context)(implicit T: c.WeakTypeTag[T]): List[c.universe.Type] = {
    import c.universe._

    @annotation.tailrec
    def normalized(tpe: c.universe.Type): c.universe.Type = {
      val norm = normalize(c)(tpe)
      if (!(norm =:= tpe))
        normalized(norm)
      else
        tpe
    }

    val tpe = weakTypeOf[T]
    declarations(c)(tpe).collect {
      case m: MethodSymbol if m.isCaseAccessor =>
        normalized(m.returnType.asSeenFrom(tpe, tpe.typeSymbol.asClass))
    }.toList
  }

}
