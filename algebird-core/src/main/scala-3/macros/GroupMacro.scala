package com.twitter.algebird.macros

import com.twitter.algebird.*

import scala.compiletime.summonInline
import scala.deriving.Mirror
import scala.quoted.Expr
import scala.quoted.Quotes
import scala.quoted.Type
import scala.runtime.Tuples
object GroupMacro:
  
  def summonAll[T: Type](using q: Quotes): List[Expr[Group[?]]] =
      Type.of[T] match
         case '[tpe *: tpes]   =>
            Expr.summon[Group[tpe]] match
              case None => derivedGroup[tpe] :: summonAll[tpes]
              case Some(inst) => inst  :: summonAll[tpes]
         case '[EmptyTuple]     => Nil

  def derivedGroup[T: Type](using q: Quotes): Expr[Group[T]] =
    import q.reflect.*
    val ev: Expr[Mirror.Of[T]] = Expr.summon[Mirror.Of[T]] match
      case None =>
        val tname = TypeRepr.of[T].typeSymbol.name
        report.errorAndAbort(s"unable to find or derive Group instance for ${tname}")
      case Some(expr) => expr

    

    ev match
      case '{ $m: Mirror.ProductOf[T] { type MirroredElemTypes = elementTypes } } =>
        
        val insts = summonAll[elementTypes]
        val instsAsExpr = Expr.ofTupleFromSeq(insts)

        '{ 
            new Group[T]:
              override def zero: T =
                val t = MonoidMacro.applyZeros(${instsAsExpr}).asInstanceOf[elementTypes]
                ${m}.fromTuple(t)
              override def plus(x:T,y:T):T = 
                val args = Tuple.fromProduct(x.asInstanceOf[Product]).zip(Tuple.fromProduct(y.asInstanceOf[Product])).zip(${instsAsExpr}).asInstanceOf[Tuple]
                val t = SemigroupMacro.applyPlus(args).asInstanceOf[elementTypes]
                ${m}.fromTuple(t)
              override def minus(x:T,y:T):T = 
                val args = Tuple.fromProduct(x.asInstanceOf[Product]).zip(Tuple.fromProduct(y.asInstanceOf[Product])).zip(${instsAsExpr}).asInstanceOf[Tuple]
                val t = applyMinus(args).asInstanceOf[elementTypes]
                ${m}.fromTuple(t)
              override def negate(x: T): T = 
                val args = Tuple.fromProduct(x.asInstanceOf[Product]).zip(${instsAsExpr}).asInstanceOf[Tuple]
                val t = applyNegates(args).asInstanceOf[elementTypes]
                ${m}.fromTuple(t)
        
        }

      case '{ $m: Mirror.SumOf[T] { type MirroredElemTypes = elementTypes } } =>
        ???
  private[macros] def applyNegates(a:Tuple):Tuple =
    a match
        case EmptyTuple => EmptyTuple
        case ((x,inst):((?,Group[?]))) *: ts =>
            inst.asInstanceOf[Group[Any]].negate(x) *: applyNegates(ts)
        case _ => ???
  private[macros] def applyMinus(a:Tuple):Tuple =
    a match
        case EmptyTuple => EmptyTuple
        case (((x,y),inst):((?,?),Group[?])) *: ts =>
            inst.asInstanceOf[Group[Any]].minus(x,y) *: applyMinus(ts)
        case _ => ???



