package com.twitter.algebird.macros
import scala.quoted.Quotes
import scala.quoted.Expr

trait MacroHelper:
  inline def genTupleNTyped(using q: Quotes)(
      caseclassFields: List[q.reflect.Symbol]
  ): q.reflect.Applied =
    import q.reflect.*
    val optTpes = caseclassFields
      .map(sym =>
        Applied(
          TypeIdent(TypeRepr.of[Option].typeSymbol),
          List(sym.termRef.typeSymbol.tree)
        )
      )
      .toList
    val tupleClazz =
      Symbol.classSymbol(s"scala.Tuple${caseclassFields.length}")
    val tupleNIdent = Ident(tupleClazz.termRef)
    val tupleN = TypeIdent(tupleNIdent.symbol)
    Applied(tupleN, optTpes)
  def genSomes(using q: Quotes)(
      caseclassFields: List[q.reflect.Symbol]
  ): List[q.reflect.Symbol] =
    import q.reflect.*
    caseclassFields.map(sym =>
      sym.termRef.widen.asType match
        case '[t] =>
          Symbol.newVal(
            Symbol.spliceOwner,
            sym.name,
            TypeRepr.of[Option[t]],
            Flags.EmptyFlags,
            Symbol.noSymbol
          )
    )
  def somesStmt[T](using q: Quotes)(
      somes: List[q.reflect.Symbol],
      caseclassFields: List[q.reflect.Symbol],
      caseclass: Expr[T]
  ): List[q.reflect.ValDef] =
    import q.reflect.*
    somes
      .zip(caseclassFields)
      .map((someSym, field) =>
        ValDef(
          someSym,
          Some {
            field.termRef.widen.asType match
              case '[t] =>
                val value = Select(caseclass.asTerm, field).asExprOf[t]
                '{ Some(${ value }) }.asTerm
          }
        )
      )
