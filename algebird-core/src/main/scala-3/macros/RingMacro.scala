package com.twitter.algebird.macros

import com.twitter.algebird.*

import scala.compiletime.summonInline
import scala.deriving.Mirror
import scala.quoted.Expr
import scala.quoted.Quotes
import scala.quoted.Type
import scala.runtime.Tuples
object RingMacro:
  
  def summonAll[T: Type](using q: Quotes): List[Expr[Ring[?]]] =
      Type.of[T] match
         case '[tpe *: tpes]   =>
            Expr.summon[Ring[tpe]] match
              case None => derivedRing[tpe] :: summonAll[tpes]
              case Some(inst) => inst  :: summonAll[tpes]
         case '[EmptyTuple]     => Nil

  def derivedRing[T: Type](using q: Quotes): Expr[Ring[T]] =
    import q.reflect.*
    val ev: Expr[Mirror.Of[T]] = Expr.summon[Mirror.Of[T]] match
      case None =>
        val tname = TypeRepr.of[T].typeSymbol.name
        report.errorAndAbort(s"unable to find or derive Ring instance for ${tname}")
      case Some(expr) => expr

    

    ev match
      case '{ $m: Mirror.ProductOf[T] { type MirroredElemTypes = elementTypes } } =>
        
        val insts = summonAll[elementTypes]
        val instsAsExpr = Expr.ofTupleFromSeq(insts)

        '{ 
            new Ring[T]:
              override def zero: T =
                val t = MonoidMacro.applyZeros(${instsAsExpr}).asInstanceOf[elementTypes]
                ${m}.fromTuple(t)
              override def plus(x:T,y:T):T = 
                val args = Tuple.fromProduct(x.asInstanceOf[Product]).zip(Tuple.fromProduct(y.asInstanceOf[Product])).zip(${instsAsExpr}).asInstanceOf[Tuple]
                val t = SemigroupMacro.applyPlus(args).asInstanceOf[elementTypes]
                ${m}.fromTuple(t)
              override def minus(x:T,y:T):T = 
                val args = Tuple.fromProduct(x.asInstanceOf[Product]).zip(Tuple.fromProduct(y.asInstanceOf[Product])).zip(${instsAsExpr}).asInstanceOf[Tuple]
                val t = GroupMacro.applyMinus(args).asInstanceOf[elementTypes]
                ${m}.fromTuple(t)
              override def negate(x: T): T = 
                val args = Tuple.fromProduct(x.asInstanceOf[Product]).zip(${instsAsExpr}).asInstanceOf[Tuple]
                val t = GroupMacro.applyNegates(args).asInstanceOf[elementTypes]
                ${m}.fromTuple(t)
              override def one: T =
                val t = applyOnes(${instsAsExpr}).asInstanceOf[elementTypes]
                ${m}.fromTuple(t)
              override def times(x: T, y: T): T =
                val args = Tuple.fromProduct(x.asInstanceOf[Product]).zip(Tuple.fromProduct(y.asInstanceOf[Product])).zip(${instsAsExpr}).asInstanceOf[Tuple]
                val t = applyTimes(args).asInstanceOf[elementTypes]
                ${m}.fromTuple(t)
        
        }

      case '{ $m: Mirror.SumOf[T] { type MirroredElemTypes = elementTypes } } =>
        ???
  private[macros] def applyOnes(a:Tuple):Tuple =
    a match
        case EmptyTuple => EmptyTuple
        case inst *: ts =>
            inst.asInstanceOf[Ring[Any]].one *: applyOnes(ts)
  private[macros] def applyTimes(a:Tuple):Tuple =
      a match
        case EmptyTuple => EmptyTuple
        case (((x,y),inst):((?,?),Ring[?])) *: ts =>
            inst.asInstanceOf[Ring[Any]].times(x,y) *: applyTimes(ts)
        case _ => ???



