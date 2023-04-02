
package com.twitter.algebird.macros

import com.twitter.algebird.*

import scala.compiletime.summonInline
import scala.deriving.Mirror
import scala.quoted.Expr
import scala.quoted.Quotes
import scala.quoted.Type
import scala.runtime.Tuples
object SemigroupMacro:
  
  def summonAll[T: Type](using q: Quotes): List[Expr[Semigroup[?]]] =
      Type.of[T] match
         case '[tpe *: tpes]   =>
            Expr.summon[Semigroup[tpe]] match
              case None => derivedSemigroup[tpe] :: summonAll[tpes]
              case Some(inst) => inst  :: summonAll[tpes]
         case '[EmptyTuple]     => Nil

  def derivedSemigroup[T: Type](using q: Quotes): Expr[Semigroup[T]] =
    import q.reflect.*
    val ev: Expr[Mirror.Of[T]] = Expr.summon[Mirror.Of[T]] match
      case None =>
        val tname = TypeRepr.of[T].typeSymbol.name
        report.errorAndAbort(s"unable to find or derive Semigroup instance for ${tname}")
      case Some(expr) => expr

    

    ev match
      case '{ $m: Mirror.ProductOf[T] { type MirroredElemTypes = elementTypes } } =>
        
        val insts = summonAll[elementTypes]
        val instsAsExpr = Expr.ofTupleFromSeq(insts)

        '{
            new Semigroup[T]:
              override def plus(x:T,y:T):T =
                val args = Tuple.fromProduct(x.asInstanceOf[Product]).zip(Tuple.fromProduct(y.asInstanceOf[Product])).zip(${instsAsExpr}).asInstanceOf[Tuple]
                val t = applyPlus(args).asInstanceOf[elementTypes]
                ${m}.fromTuple(t)
        }

      case '{ $m: Mirror.SumOf[T] { type MirroredElemTypes = elementTypes } } =>
        ???
                    
  private[macros] def applyPlus(a:Tuple):Tuple =
    a match
        case EmptyTuple => EmptyTuple
        case (((x,y),inst):((?,?),Semigroup[?])) *: ts =>
            inst.asInstanceOf[Semigroup[Any]].plus(x,y) *: applyPlus(ts)
        case _ => ???

